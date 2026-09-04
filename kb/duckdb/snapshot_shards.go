package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/centroid"
	"github.com/mikills/minnow/kb/duckdb/internal/compact"
	"github.com/mikills/minnow/kb/duckdb/internal/reconstruct"
	"github.com/mikills/minnow/kb/duckdb/internal/shardbuild"
	"github.com/mikills/minnow/kb/duckdb/internal/shardcache"
)

func (f *DuckDBArtifactFormat) BuildArtifacts(
	ctx context.Context,
	kbID, srcPath string,
	targetBytes int64,
) ([]kb.SnapshotShardMetadata, error) {
	// Once, before the loop. Checking per shard aborts a snapshot midway and
	// strands the shards already uploaded.
	if err := f.budget().AdmitBuild(); err != nil {
		return nil, err
	}
	if targetBytes <= 0 {
		targetBytes = kb.DefaultSnapshotShardSize
	}
	dbHash, err := kb.FileContentSHA256(ctx, srcPath)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("%s.duckdb.shards/%s", kbID, dbHash[:16])
	parts, _, err := f.uploadQueryableSnapshotShards(ctx, prefix, srcPath, targetBytes)
	if err != nil {
		return nil, err
	}
	return parts, nil
}

func (f *DuckDBArtifactFormat) uploadQueryableSnapshotShards(
	ctx context.Context,
	prefix, localDBPath string,
	partSize int64,
) ([]kb.SnapshotShardMetadata, int64, error) {
	hasTombstones, activeRows, err := f.inspectSnapshotSourceDB(ctx, localDBPath)
	if err != nil {
		return nil, 0, err
	}
	if activeRows <= 0 {
		// Every row was deleted. Refusing here means the delete that removes
		// the last document can never succeed.
		return nil, 0, nil
	}

	rowsPerShard, err := planShardRowCount(localDBPath, activeRows, partSize)
	if err != nil {
		return nil, 0, err
	}

	tmpDir, err := os.MkdirTemp("", "minnow-queryable-shards-*")
	if err != nil {
		return nil, 0, err
	}
	defer os.RemoveAll(tmpDir)

	estimatedShards := int((activeRows + int64(rowsPerShard) - 1) / int64(rowsPerShard))
	parts := make([]kb.SnapshotShardMetadata, 0, estimatedShards)
	totalSize := int64(0)
	processedRows := int64(0)
	lastDocID := ""
	for shardIndex := 0; processedRows < activeRows; shardIndex++ {
		shardPath := filepath.Join(tmpDir, fmt.Sprintf("shard-%05d.duckdb", shardIndex))
		part, vectorRows, nextDocID, err := f.buildAndUploadOneSnapshotShard(
			ctx,
			snapshotShardBuildInput{
				prefix:        prefix,
				localDBPath:   localDBPath,
				hasTombstones: hasTombstones,
				shardPath:     shardPath,
				shardIndex:    shardIndex,
				rowsPerShard:  rowsPerShard,
				lastDocID:     lastDocID,
			},
		)
		if err != nil {
			return nil, 0, err
		}
		if vectorRows <= 0 {
			return nil, 0, fmt.Errorf("failed to advance shard cursor while building snapshot shards")
		}
		lastDocID = nextDocID
		processedRows += vectorRows
		parts = append(parts, part)
		totalSize += part.SizeBytes
	}

	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("failed to build shard files")
	}
	return parts, totalSize, nil
}

// inspectSnapshotSourceDB opens the source DB to detect tombstones and count
// active docs, then closes it before returning. DuckDB v1.5+ does not allow
// a file to be open by multiple database handles simultaneously, and shard
// building ATTACHes localDBPath from a separate handle.
func (f *DuckDBArtifactFormat) inspectSnapshotSourceDB(ctx context.Context, localDBPath string) (bool, int64, error) {
	sourceDB, err := f.openConfiguredDB(ctx, localDBPath)
	if err != nil {
		return false, 0, err
	}
	defer sourceDB.Close()

	hasTombstones, err := tableExists(ctx, sourceDB, "doc_tombstones")
	if err != nil {
		return false, 0, err
	}
	activeRows, err := shardbuild.CountActiveDocs(ctx, sourceDB, hasTombstones)
	if err != nil {
		return false, 0, err
	}
	return hasTombstones, activeRows, nil
}

func planShardRowCount(localDBPath string, activeRows, partSize int64) (int, error) {
	sourceInfo, err := os.Stat(localDBPath)
	if err != nil {
		return 0, err
	}
	bytesPerRow := sourceInfo.Size() / activeRows
	if bytesPerRow <= 0 {
		bytesPerRow = 1
	}
	rowsPerShard := int(partSize / bytesPerRow)
	if rowsPerShard <= 0 {
		rowsPerShard = 1
	}
	return rowsPerShard, nil
}

type snapshotShardBuildInput struct {
	prefix        string
	localDBPath   string
	hasTombstones bool
	shardPath     string
	shardIndex    int
	rowsPerShard  int
	lastDocID     string
}

func (f *DuckDBArtifactFormat) buildAndUploadOneSnapshotShard(
	ctx context.Context,
	input snapshotShardBuildInput,
) (kb.SnapshotShardMetadata, int64, string, error) {
	vectorRows, graphAvailable, nextDocID, centroid, err := f.buildShardDBFromSourceRange(
		ctx,
		shardSourceRange{
			sourceDBPath:  input.localDBPath,
			hasTombstones: input.hasTombstones,
			shardPath:     input.shardPath,
			limit:         input.rowsPerShard,
			lastDocID:     input.lastDocID,
		},
	)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	if vectorRows <= 0 {
		return kb.SnapshotShardMetadata{}, 0, nextDocID, nil
	}

	sha, err := kb.FileContentSHA256(ctx, input.shardPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	info, err := os.Stat(input.shardPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	partKey := fmt.Sprintf("%s/shard-%05d.duckdb", input.prefix, input.shardIndex)
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, partKey, input.shardPath, "")
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}

	now := time.Now().UTC()
	mediaIDs, mediaErr := collectShardMediaIDs(ctx, input.shardPath, f.openConfiguredDB)
	if mediaErr != nil {
		return kb.SnapshotShardMetadata{}, 0, "", fmt.Errorf("collect shard media ids: %w", mediaErr)
	}
	return kb.SnapshotShardMetadata{
		ShardID:        fmt.Sprintf("shard-%05d", input.shardIndex),
		Key:            partKey,
		Version:        uploadInfo.Version,
		SizeBytes:      info.Size(),
		VectorRows:     vectorRows,
		CreatedAt:      now,
		SealedAt:       now,
		TombstoneRatio: 0,
		GraphAvailable: graphAvailable,
		Centroid:       centroid,
		SHA256:         sha,
		MediaIDs:       mediaIDs,
	}, vectorRows, nextDocID, nil
}

type shardSourceRange struct {
	sourceDBPath  string
	hasTombstones bool
	shardPath     string
	limit         int
	lastDocID     string
}

func (f *DuckDBArtifactFormat) buildShardDBFromSourceRange(
	ctx context.Context,
	source shardSourceRange,
) (int64, bool, string, []float32, error) {
	if err := shardbuild.ValidateLimit(source.limit); err != nil {
		return 0, false, "", nil, err
	}
	threads, releaseThreads := f.budget().AcquireBuildThreads(ctx, f.buildThreads())
	defer releaseThreads()
	openBuild := func(ctx context.Context, path string) (*sql.DB, error) {
		return f.openWithThreads(ctx, path, threads)
	}
	shardDB, err := shardbuild.OpenAttached(ctx, source.sourceDBPath, source.shardPath, openBuild)
	if err != nil {
		return 0, false, "", nil, err
	}
	defer shardDB.Close()
	defer shardbuild.DetachSource(ctx, shardDB)

	if err := shardbuild.CopyRowRange(ctx, shardDB, source.hasTombstones, source.limit, source.lastDocID); err != nil {
		return 0, false, "", nil, logDuckDBMemoryOnError(ctx, shardDB, "shard_copy_rows", err)
	}

	if err := buildShardIndexes(ctx, shardDB); err != nil {
		return 0, false, "", nil, err
	}

	if err := shardbuild.CopyGraphTables(ctx, shardDB, EnsureGraphTables); err != nil {
		return 0, false, "", nil, err
	}

	rowCount, empty, err := shardRowCountOrClose(ctx, shardDB)
	if err != nil || empty {
		return rowCount, false, source.lastDocID, nil, err
	}
	maxDocID, err := shardbuild.MaxDocID(ctx, shardDB)
	if err != nil {
		return 0, false, "", nil, err
	}
	graphAvailable, centroid, err := finalizeBuiltShardDB(ctx, shardDB)
	if err != nil {
		return 0, false, "", nil, err
	}
	return rowCount, graphAvailable, maxDocID, centroid, nil
}

func finalizeBuiltShardDB(ctx context.Context, db *sql.DB) (bool, []float32, error) {
	centroid, err := centroid.Compute(ctx, db)
	if err != nil {
		return false, nil, err
	}
	graphAvailable := hasGraphQueryData(ctx, db)
	// NOTE: SnapshotShardMetadata.MediaIDs is populated at manifest-publish
	// time by walking docs.media_refs. See collectShardMediaIDs.
	if err := CheckpointAndCloseDB(ctx, db, "close shard db"); err != nil {
		return false, nil, err
	}
	return graphAvailable, centroid, nil
}

func shardRowCountOrClose(ctx context.Context, db *sql.DB) (int64, bool, error) {
	rowCount, err := countCompactedDocs(ctx, db)
	if err != nil || rowCount > 0 {
		return rowCount, false, err
	}
	return 0, true, CheckpointAndCloseDB(ctx, db, "close empty shard db")
}

// collectShardMediaIDs scans docs.media_refs for a shard file and returns
// the unique media ids it references. Called when sealing a shard so the
// manifest can carry the ids for pending-media GC promotion.
func collectShardMediaIDs(
	ctx context.Context,
	shardPath string,
	openDB func(context.Context, string) (*sql.DB, error),
) ([]string, error) {
	db, err := openDB(ctx, shardPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT media_refs FROM docs WHERE media_refs IS NOT NULL AND media_refs != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		refs, err := decodeMediaRefs(raw)
		if err != nil {
			continue
		}
		for _, ref := range refs {
			if ref.MediaID == "" {
				continue
			}
			if _, ok := seen[ref.MediaID]; ok {
				continue
			}
			seen[ref.MediaID] = struct{}{}
			out = append(out, ref.MediaID)
		}
	}
	return out, rows.Err()
}

func (f *DuckDBArtifactFormat) downloadSnapshotFromShards(
	ctx context.Context,
	kbID, dest string,
) (*kb.SnapshotShardManifest, error) {
	if err := validateSnapshotDownload(kbID, dest); err != nil {
		return nil, err
	}

	manifest, err := f.downloadSourceManifest(ctx, kbID)
	if err != nil {
		return nil, err
	}

	// Group-bounded rebuild: partition shards by live bytes so each group's
	// merge fits within MaxShardBytes. Groups merge sequentially into the
	// single dest DB (single mutable DB design), one group resident at a
	// time, so ingest peak scales with one group, not the corpus.
	maxBytes := f.reconstructMaxBytes()
	groups := compact.PartitionForReconstruct(manifest.Shards, maxBytes)
	if len(groups) == 0 {
		return nil, fmt.Errorf("manifest has no shards")
	}

	tmpDir, err := os.MkdirTemp("", "minnow-shard-download-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	tmpDest, cleanupTmpDest, err := reconstruct.TempPath(dest)
	if err != nil {
		return nil, err
	}
	defer cleanupTmpDest()

	db, releaseThreads, err := f.openBuildDB(ctx, tmpDest)
	if err != nil {
		return nil, err
	}
	defer releaseThreads()
	defer db.Close()

	if err := f.mergeManifestShards(ctx, db, tmpDir, groups); err != nil {
		return nil, logDuckDBMemoryOnError(ctx, db, "merge_manifest_shards", err)
	}
	// A corpus-wide HNSW/FTS build over the merged dest would reintroduce
	// the unbounded peak this grouping removes: index memory scales with the
	// full corpus, not one group. The mutable DB never serves queries
	// (reads go via ManifestStore + openCachedShardConn per shard; only
	// writers open vectors.duckdb and re-split via the seal path, which
	// builds per-shard indexes), so a multi-group rebuild skips the
	// corpus-wide index and still converges: the next seal re-splits through
	// buildShardDBFromSourceRange + buildShardIndexes + collectShardMediaIDs.
	// A single group stays under MaxShardBytes (live bytes; file bytes may
	// exceed it by HNSW/FTS overhead), so building its indexes preserves the
	// existing small-corpus behavior that direct-open tests rely on.
	buildIndexes := len(groups) == 1
	if err := finalizeReconstructedSnapshot(ctx, db, buildIndexes); err != nil {
		return nil, logDuckDBMemoryOnError(ctx, db, "finalize_snapshot", err)
	}
	if err := os.Rename(tmpDest, dest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func validateSnapshotDownload(kbID string, dest string) error {
	if kbID == "" {
		return fmt.Errorf(errEmptyKBID)
	}
	if dest == "" {
		return fmt.Errorf("dest cannot be empty")
	}
	return nil
}

func (f *DuckDBArtifactFormat) downloadSourceManifest(
	ctx context.Context,
	kbID string,
) (kb.SnapshotShardManifest, error) {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		return kb.SnapshotShardManifest{}, err
	}
	manifest := doc.Manifest
	if err := f.validateManifestFormat(&manifest); err != nil {
		return kb.SnapshotShardManifest{}, err
	}
	f.deps.Metrics.RecordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return kb.SnapshotShardManifest{}, fmt.Errorf("manifest has no shards")
	}
	return manifest, nil
}

// reconstructMaxBytes bounds one reconstruct group. Normalized policy always
// carries a positive cap; the fallback keeps a zero policy from collapsing
// every corpus into one unbounded group.
func (f *DuckDBArtifactFormat) reconstructMaxBytes() int64 {
	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if policy.MaxShardBytes > 0 {
		return policy.MaxShardBytes
	}
	return kb.DefaultShardingPolicy().MaxShardBytes
}

func (f *DuckDBArtifactFormat) mergeManifestShards(
	ctx context.Context,
	db *sql.DB,
	tmpDir string,
	groups [][]kb.SnapshotShardMetadata,
) error {
	// Sequential per-group merge into the single dest DB. Each part is
	// downloaded, merged (ATTACH + INSERT + DETACH inside MergeShardIntoDB),
	// then removed before the next part, so at most one downloaded part file
	// plus one group's INSERT batch is resident at a time. The dest file
	// itself grows on disk across groups, but merge memory per step is
	// bounded by the group's live bytes (each group <= MaxShardBytes, except
	// an unsplittable oversize singleton which merges alone). That is a
	// live-bytes bound, not a bound on copied bytes: the merge copies all
	// docs rows unfiltered and never consults doc_tombstones, so a group
	// whose shards carry nonzero tombstone ratios can copy more than the
	// cap. That is latent today (sealed shards carry TombstoneRatio 0, set
	// at snapshot_shards.go build time and on compaction replacements, and
	// inherited from Fit's live-bytes sizing); merge behavior is
	// deliberately unchanged and preserves pre-change copy-all semantics.
	global := 0
	for _, group := range groups {
		for _, shard := range group {
			partPath := filepath.Join(tmpDir, fmt.Sprintf("part-%05d.duckdb", global))
			if err := f.downloadAndVerifyShardPart(ctx, shard, partPath); err != nil {
				return err
			}
			if err := reconstruct.MergeShardIntoDB(ctx, db, reconstruct.MergeOptions{Alias: fmt.Sprintf("s%d", global), PartPath: partPath, IsFirst: global == 0, EnsureGraphTables: EnsureGraphTables}); err != nil {
				return err
			}
			// DETACH has run inside MergeShardIntoDB, so the part file is
			// unreferenced; remove it before the next download so disk and
			// page-cache pressure also track one group, not the corpus. A
			// removal failure only logs: the deferred tmpDir RemoveAll is
			// the backstop that reclaims the file.
			if err := os.Remove(partPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				slog.Default().WarnContext(ctx, "failed to remove merged shard part file",
					"shard_id", shard.ShardID, "path", partPath, logKeyError, err)
			}
			global++
		}
	}
	return nil
}

func (f *DuckDBArtifactFormat) downloadAndVerifyShardPart(
	ctx context.Context,
	shard kb.SnapshotShardMetadata,
	partPath string,
) error {
	if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
		return err
	}
	return shardcache.VerifyDownloaded(ctx, shard, partPath)
}
func finalizeReconstructedSnapshot(ctx context.Context, db *sql.DB, buildIndexes bool) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}
	// buildIndexes is true only for a single group (small corpus). A
	// multi-group rebuild skips the corpus-wide HNSW/FTS build to keep peak
	// bounded by one group; the mutable DB is writer-only (queries serve from
	// per-shard conns) and the next seal rebuilds per-shard indexes.
	if !buildIndexes {
		return CheckpointAndCloseDB(ctx, db, "close reconstructed shard snapshot")
	}
	if err := createDocsVectorIndex(ctx, db); err != nil {
		return err
	}
	if err := createDocsFTSIndex(ctx, db); err != nil {
		return err
	}
	return CheckpointAndCloseDB(ctx, db, "close reconstructed shard snapshot")
}

// DownloadSnapshotFromShards downloads and reconstructs a sharded snapshot into a single DB file.
func (f *DuckDBArtifactFormat) DownloadSnapshotFromShards(
	ctx context.Context,
	kbID, dest string,
) (*kb.SnapshotShardManifest, error) {
	return f.downloadSnapshotFromShards(ctx, kbID, dest)
}

// cleanupPreShardSnapshotObjectsBestEffort deletes the monolithic snapshot
// objects from the pre-sharded layout that may still exist in long-running
// deployments. Failures only log. the upload path must succeed regardless.
func (f *DuckDBArtifactFormat) cleanupPreShardSnapshotObjectsBestEffort(ctx context.Context, kbID string) error {
	preShardSnapshotKeys := []string{
		kbID + ".duckdb",
		kbID + ".snapshot.json",
	}
	for _, key := range preShardSnapshotKeys {
		err := f.deps.BlobStore.Delete(ctx, key)
		if err == nil || errors.Is(err, kb.ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			continue
		}
		slog.Default().
			WarnContext(ctx, "failed to delete pre-shard snapshot object", logKeyKBID, kbID, "key", key, logKeyError, err)
	}
	return nil
}

func hasGraphQueryData(ctx context.Context, db *sql.DB) bool {
	if err := ensureGraphQueryReady(ctx, db); err != nil {
		return false
	}
	return true
}
