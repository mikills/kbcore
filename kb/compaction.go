// Compaction merges multiple shard snapshot files (DuckDB shard parts) into a
// single compacted shard and publishes a new shard manifest that replaces the
// original shards. The goals are:
//
//   - Reduce shard count and size variance (consolidate fragmented shards).
//   - Reclaim storage from tombstoned/deleted rows.
//   - Rebuild the HNSW vector index for the combined data.
//
// System fit:
//
//   - Compaction acquires a write lease before performing any work, ensuring
//     only one compaction/ingest operation runs per KB at a time cluster-wide.
//   - Publishing the new manifest uses compare-and-set (UploadIfMatch) so
//     concurrent manifest changes are detected and rejected.
//   - Replaced shards are enqueued for garbage collection rather than deleted
//     immediately, allowing in-flight readers to finish gracefully.
//
// Selection algorithm:
//
//   - Shards are grouped into size tiers using log2(size / targetSize).
//   - The densest tier (most shards) is selected; up to maxCompactionCandidates
//     shards from that tier are merged per pass.
//   - Alternatively, shards with high tombstone ratios can trigger compaction
//     even if the tier density is low ("tombstone pressure").
//
// Failure modes:
//
//   - Network/blob errors abort compaction and return an error.
//   - Manifest CAS conflicts (ErrBlobVersionMismatch) abort compaction; the
//     caller can retry on a subsequent pass.
//   - Compaction is I/O and CPU intensive; temp files and HNSW index rebuilds
//     can be expensive for large shards.

package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// maxCompactionCandidates is the upper bound on shards merged in one pass.
// Limiting candidates bounds memory/CPU usage per compaction cycle.
const maxCompactionCandidates = 4

// CompactionPublishResult describes the outcome of a single compaction attempt.
//
// When Performed is false, no compaction was necessary (insufficient candidates
// or compaction debt). When Performed is true, ReplacedShards were merged into
// ReplacementShards and the manifest was updated from ManifestVersionOld to
// ManifestVersionNew.
type CompactionPublishResult struct {
	Performed          bool
	ReplacedShards     []SnapshotShardMetadata
	ReplacementShards  []SnapshotShardMetadata
	ManifestVersionOld string
	ManifestVersionNew string
}

// CompactShardsIfNeeded performs one compaction pass for a sharded KB.
//
// It acquires a write lease, evaluates compaction candidates, and if at least
// two candidates exist, merges them into a single replacement shard. The new
// manifest is published via compare-and-set; on CAS conflict the operation
// aborts with ErrBlobVersionMismatch.
//
// Side effects on success:
//   - Replacement shard uploaded to BlobStore.
//   - New manifest published (old manifest version superseded).
//   - Replaced shards enqueued for garbage collection.
//   - Metrics recorded via recordCompactionResult and recordShardCount.
//
// Returns Performed=false when there is no compaction debt or fewer than two
// candidates. Returns an error on lease acquisition failure, blob I/O errors,
// or manifest CAS conflict.
func (l *KB) CompactShardsIfNeeded(ctx context.Context, kbID string) (result *CompactionPublishResult, err error) {
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	startedAt := time.Now()
	defer func() {
		l.recordCompactionResult(kbID, time.Since(startedAt), result, err)
	}()

	leaseManager, lease, err := l.acquireWriteLease(ctx, kbID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = leaseManager.Release(context.Background(), lease)
	}()

	manifestVersion, err := l.currentShardManifestVersion(ctx, kbID)
	if err != nil {
		return nil, err
	}
	if manifestVersion == "" {
		return &CompactionPublishResult{Performed: false}, nil
	}

	manifest, err := l.downloadShardManifest(ctx, kbID)
	if err != nil {
		return nil, err
	}

	candidates, candidateReason := selectCompactionCandidatesWithReason(l.ShardingPolicy, manifest)
	slog.Default().InfoContext(ctx, "evaluated compaction candidates", "kb_id", kbID, "reason", candidateReason, "candidate_count", len(candidates), "shard_count", len(manifest.Shards))
	if len(candidates) < 2 {
		return &CompactionPublishResult{Performed: false, ManifestVersionOld: manifestVersion}, nil
	}

	replacement, err := l.buildAndUploadCompactionReplacement(ctx, kbID, candidates)
	if err != nil {
		return nil, err
	}

	nextManifest := buildCompactedManifest(kbID, manifest, candidates, replacement)
	nextInfo, err := l.uploadShardManifestIfMatch(ctx, kbID, nextManifest, manifestVersion)
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			slog.Default().WarnContext(ctx, "compaction manifest CAS conflict", "kb_id", kbID, "reason", "manifest_cas_conflict")
		}
		return nil, err
	}
	l.recordShardCount(kbID, len(nextManifest.Shards))
	l.enqueueReplacedShardsForGC(kbID, candidates, time.Now().UTC())
	slog.Default().InfoContext(ctx, "completed shard compaction", "kb_id", kbID, "reason", "publish_compaction", "replaced_count", len(candidates), "new_manifest_version", nextInfo.Version)

	return &CompactionPublishResult{
		Performed:          true,
		ReplacedShards:     append([]SnapshotShardMetadata(nil), candidates...),
		ReplacementShards:  []SnapshotShardMetadata{replacement},
		ManifestVersionOld: manifestVersion,
		ManifestVersionNew: nextInfo.Version,
	}, nil
}

// buildAndUploadCompactionReplacement downloads the candidate shards, merges
// their docs tables into a single DuckDB file, rebuilds the HNSW index, and
// uploads the result to BlobStore.
//
// The merge process:
//  1. Create a temporary DuckDB file.
//  2. For each candidate shard, ATTACH in read-only mode, INSERT INTO docs,
//     then DETACH.
//  3. CREATE INDEX ... USING HNSW (embedding) on the combined docs table.
//  4. Checkpoint and close the DB.
//  5. Compute SHA256 and file size, then upload via UploadIfMatch.
//
// Returns the metadata for the replacement shard on success.
func (l *KB) buildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []SnapshotShardMetadata) (SnapshotShardMetadata, error) {
	tmpDir, err := os.MkdirTemp("", "kbcore-compact-*")
	if err != nil {
		return SnapshotShardMetadata{}, err
	}
	defer os.RemoveAll(tmpDir)

	combinedPath := filepath.Join(tmpDir, "replacement.duckdb")
	db, err := l.openConfiguredDB(ctx, combinedPath)
	if err != nil {
		return SnapshotShardMetadata{}, err
	}
	defer db.Close()

	vectorRows := int64(0)
	graphAvailable := false
	for i, shard := range shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("in-%05d.duckdb", i))
		if err := l.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return SnapshotShardMetadata{}, err
		}

		alias := fmt.Sprintf("s%d", i)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", quoteSQLString(partPath), alias)); err != nil {
			return SnapshotShardMetadata{}, err
		}
		if i == 0 {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE docs AS SELECT * FROM %s.docs WHERE 1=0", alias)); err != nil {
				_, _ = db.ExecContext(ctx, fmt.Sprintf("DETACH %s", alias))
				return SnapshotShardMetadata{}, err
			}
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO docs SELECT * FROM %s.docs", alias)); err != nil {
			_, _ = db.ExecContext(ctx, fmt.Sprintf("DETACH %s", alias))
			return SnapshotShardMetadata{}, err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DETACH %s", alias)); err != nil {
			return SnapshotShardMetadata{}, err
		}

		vectorRows += shard.VectorRows
		graphAvailable = graphAvailable || shard.GraphAvailable
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return SnapshotShardMetadata{}, err
	}
	if err := checkpointAndCloseDB(ctx, db, "close compacted shard db"); err != nil {
		return SnapshotShardMetadata{}, err
	}

	sha, err := fileContentSHA256(combinedPath)
	if err != nil {
		return SnapshotShardMetadata{}, err
	}
	info, err := os.Stat(combinedPath)
	if err != nil {
		return SnapshotShardMetadata{}, err
	}

	now := time.Now().UTC()
	replacementID := fmt.Sprintf("compact-%d", now.UnixNano())
	replacementKey := fmt.Sprintf("%s.duckdb.compacted/%s/part-00000", kbID, replacementID)
	uploadInfo, err := l.BlobStore.UploadIfMatch(ctx, replacementKey, combinedPath, "")
	if err != nil {
		return SnapshotShardMetadata{}, err
	}

	return SnapshotShardMetadata{
		ShardID:        replacementID,
		Key:            replacementKey,
		Version:        uploadInfo.Version,
		SizeBytes:      info.Size(),
		VectorRows:     vectorRows,
		CreatedAt:      now,
		SealedAt:       now,
		TombstoneRatio: 0,
		GraphAvailable: graphAvailable,
		SHA256:         sha,
	}, nil
}

// buildCompactedManifest produces a new manifest with the replaced shards
// removed and the replacement shard inserted in their place.
//
// The replacement is inserted at the position of the first replaced shard to
// preserve approximate ordering. TotalSizeBytes is recalculated.
func buildCompactedManifest(kbID string, current *SnapshotShardManifest, replaced []SnapshotShardMetadata, replacement SnapshotShardMetadata) SnapshotShardManifest {
	replacedByID := make(map[string]struct{}, len(replaced))
	for _, shard := range replaced {
		replacedByID[shard.ShardID] = struct{}{}
	}

	nextShards := make([]SnapshotShardMetadata, 0, len(current.Shards)-len(replaced)+1)
	inserted := false
	for _, shard := range current.Shards {
		if _, drop := replacedByID[shard.ShardID]; drop {
			if !inserted {
				nextShards = append(nextShards, replacement)
				inserted = true
			}
			continue
		}
		nextShards = append(nextShards, shard)
	}
	if !inserted {
		nextShards = append(nextShards, replacement)
	}

	total := int64(0)
	for _, shard := range nextShards {
		total += shard.SizeBytes
	}

	return SnapshotShardManifest{
		SchemaVersion:  current.SchemaVersion,
		Layout:         current.Layout,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: total,
		Shards:         nextShards,
	}
}

// uploadShardManifestIfMatch writes a new manifest to BlobStore only if the
// current manifest version matches expectedVersion (compare-and-set).
//
// On version mismatch it returns ErrBlobVersionMismatch and records a CAS
// conflict metric. This prevents lost updates when concurrent writers attempt
// to publish manifests.
func (l *KB) uploadShardManifestIfMatch(ctx context.Context, kbID string, manifest SnapshotShardManifest, expectedVersion string) (*BlobObjectInfo, error) {
	tmpDir, err := os.MkdirTemp("", "kbcore-compaction-manifest-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	manifestPath := filepath.Join(tmpDir, "manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return nil, err
	}

	info, err := l.BlobStore.UploadIfMatch(ctx, shardManifestKey(kbID), manifestPath, expectedVersion)
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			l.recordManifestCASConflict(kbID)
		}
		return nil, err
	}
	return info, nil
}

// selectCompactionCandidates returns shards eligible for compaction based on
// the current sharding policy and manifest. It is a convenience wrapper around
// selectCompactionCandidatesWithReason that discards the reason string.
func (l *KB) selectCompactionCandidates(manifest *SnapshotShardManifest) []SnapshotShardMetadata {
	return selectCompactionCandidates(l.ShardingPolicy, manifest)
}

// selectCompactionCandidatesWithReason evaluates the manifest and returns a
// deterministic candidate set for compaction along with the selection reason.
//
// Selection logic:
//  1. If shouldCompact returns false, returns nil with reason "no_compaction_debt".
//  2. Group shards by size tier (log2(size / targetSize)).
//  3. Select the densest tier (most shards). If it has >= 2 shards, return up
//     to maxCompactionCandidates from that tier (reason "size_tier").
//  4. Otherwise, collect shards with TombstoneRatio >= threshold. If >= 2,
//     return up to maxCompactionCandidates (reason "tombstone_pressure").
//  5. If neither condition yields >= 2 candidates, return nil with reason
//     "insufficient_candidates".
//
// Candidates are sorted by tombstone ratio (desc), size (asc), shard ID (asc)
// for deterministic selection.
func selectCompactionCandidatesWithReason(policy ShardingPolicy, manifest *SnapshotShardManifest) ([]SnapshotShardMetadata, string) {
	policy = normalizeShardingPolicy(policy)
	if !shouldCompact(policy, manifest) {
		return nil, "no_compaction_debt"
	}

	tiers := make(map[int][]SnapshotShardMetadata)
	for _, shard := range manifest.Shards {
		if shard.SizeBytes <= 0 {
			continue
		}
		tier := shardSizeTier(shard.SizeBytes, policy.TargetShardBytes)
		tiers[tier] = append(tiers[tier], shard)
	}

	if tier, ok := densestTier(tiers); ok && len(tiers[tier]) >= 2 {
		return sortAndLimitCompactionCandidates(tiers[tier], maxCompactionCandidates), "size_tier"
	}

	pressure := make([]SnapshotShardMetadata, 0, len(manifest.Shards))
	for _, shard := range manifest.Shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			pressure = append(pressure, shard)
		}
	}
	if len(pressure) < 2 {
		return nil, "insufficient_candidates"
	}

	return sortAndLimitCompactionCandidates(pressure, maxCompactionCandidates), "tombstone_pressure"
}

// selectCompactionCandidates is a convenience function that calls
// selectCompactionCandidatesWithReason and discards the reason.
func selectCompactionCandidates(policy ShardingPolicy, manifest *SnapshotShardManifest) []SnapshotShardMetadata {
	candidates, _ := selectCompactionCandidatesWithReason(policy, manifest)
	return candidates
}

// shouldCompact returns true when compaction is enabled and the manifest has
// compaction debt: either enough shards to trigger size-tiered compaction or
// at least one shard with excessive tombstone ratio.
func shouldCompact(policy ShardingPolicy, manifest *SnapshotShardManifest) bool {
	if !policy.CompactionEnabled || manifest == nil || len(manifest.Shards) < 2 {
		return false
	}
	if len(manifest.Shards) >= policy.CompactionMinShardCount {
		return true
	}
	for _, shard := range manifest.Shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			return true
		}
	}
	return false
}

// shardSizeTier computes the size tier for a shard as round(log2(size / target)).
// Shards within the same tier are considered similar in size and good candidates
// for merging together.
func shardSizeTier(sizeBytes, targetBytes int64) int {
	if sizeBytes <= 0 || targetBytes <= 0 {
		return 0
	}
	ratio := float64(sizeBytes) / float64(targetBytes)
	return int(math.Round(math.Log2(ratio)))
}

// densestTier returns the tier with the most shards. When multiple tiers tie,
// it prefers the tier closest to 0 (target size), then the lower tier number.
func densestTier(tiers map[int][]SnapshotShardMetadata) (int, bool) {
	bestTier := 0
	bestCount := 0
	found := false
	for tier, shards := range tiers {
		count := len(shards)
		if !found || count > bestCount || (count == bestCount && absInt(tier) < absInt(bestTier)) || (count == bestCount && absInt(tier) == absInt(bestTier) && tier < bestTier) {
			bestTier = tier
			bestCount = count
			found = true
		}
	}
	return bestTier, found
}

// sortAndLimitCompactionCandidates sorts candidates by tombstone ratio (desc),
// size (asc), and shard ID (asc) for deterministic selection, then limits to
// the specified count.
func sortAndLimitCompactionCandidates(candidates []SnapshotShardMetadata, limit int) []SnapshotShardMetadata {
	sorted := append([]SnapshotShardMetadata(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		if left.TombstoneRatio != right.TombstoneRatio {
			return left.TombstoneRatio > right.TombstoneRatio
		}
		if left.SizeBytes != right.SizeBytes {
			return left.SizeBytes < right.SizeBytes
		}
		return left.ShardID < right.ShardID
	})

	if limit > 0 && len(sorted) > limit {
		sorted = sorted[:limit]
	}
	return sorted
}

// absInt returns the absolute value of an integer.
func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
