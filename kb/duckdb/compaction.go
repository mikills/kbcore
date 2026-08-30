package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/centroid"
	"github.com/mikills/minnow/kb/duckdb/internal/reconstruct"
)

func (f *DuckDBArtifactFormat) CompactIfNeeded(ctx context.Context, kbID string) (*kb.CompactionPublishResult, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf(errEmptyKBID)
	}

	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	// Compacting would move the manifest past the session's local rows.
	if kb.HasPendingSession(filepath.Join(f.deps.CacheDir, kbID)) {
		return &kb.CompactionPublishResult{Performed: false}, nil
	}

	releaseLease, err := f.acquireCompactionLease(ctx, kbID)
	if err != nil {
		return nil, err
	}
	defer releaseLease()

	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		return nil, manifestGetError(err)
	}

	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return nil, err
	}

	candidates, _ := selectCompactionCandidatesWithReason(f.deps.ShardingPolicy, manifest)
	if len(candidates) < 2 {
		return &kb.CompactionPublishResult{
			Performed:          false,
			ManifestVersionOld: doc.Version,
		}, nil
	}

	replacement, err := f.buildAndUploadCompactionReplacement(ctx, kbID, candidates)
	if err != nil {
		return nil, err
	}

	nextManifest := buildCompactedManifest(kbID, manifest, candidates, replacement)
	newVersion, err := f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, nextManifest, doc.Version)
	if err != nil {
		return nil, err
	}

	f.afterCompactionPublish(kbID, candidates, nextManifest)

	return &kb.CompactionPublishResult{
		Performed:          true,
		ReplacedShards:     append([]kb.SnapshotShardMetadata(nil), candidates...),
		ReplacementShards:  []kb.SnapshotShardMetadata{replacement},
		ManifestVersionOld: doc.Version,
		ManifestVersionNew: newVersion,
	}, nil
}

func (f *DuckDBArtifactFormat) acquireCompactionLease(ctx context.Context, kbID string) (func(), error) {
	if f.deps.AcquireWriteLease == nil {
		return func() {}, nil
	}
	leaseManager, lease, err := f.deps.AcquireWriteLease(ctx, kbID)
	if err != nil {
		return nil, err
	}
	release := func() {
		if err := leaseManager.Release(context.WithoutCancel(ctx), lease); err != nil {
			slog.Default().Warn("compaction lease release failed", logKeyKBID, kbID, logKeyError, err)
		}
	}
	return release, nil
}

func manifestGetError(err error) error {
	if errors.Is(err, kb.ErrManifestNotFound) {
		return kb.ErrKBUninitialized
	}
	return err
}

func (f *DuckDBArtifactFormat) afterCompactionPublish(
	kbID string,
	candidates []kb.SnapshotShardMetadata,
	nextManifest kb.SnapshotShardManifest,
) {
	if f.deps.EnqueueReplacedShardsForGC != nil {
		f.deps.EnqueueReplacedShardsForGC(kbID, candidates, time.Now().UTC())
	}
	f.deps.Metrics.RecordShardCount(kbID, len(nextManifest.Shards))
}

type builtCompactionShard struct {
	path           string
	vectorRows     int64
	sizeBytes      int64
	graphAvailable bool
	centroid       []float32
	sha            string
}

func (f *DuckDBArtifactFormat) buildAndUploadCompactionReplacement(
	ctx context.Context,
	kbID string,
	shards []kb.SnapshotShardMetadata,
) (kb.SnapshotShardMetadata, error) {
	tmpDir, err := os.MkdirTemp("", "minnow-compact-*")
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	defer os.RemoveAll(tmpDir)

	built, err := f.buildCompactionShard(ctx, tmpDir, shards)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	now := time.Now().UTC()
	replacementID := fmt.Sprintf("compact-%d", now.UnixNano())
	replacementKey := fmt.Sprintf("%s.duckdb.compacted/%s/part-00000", kbID, replacementID)
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, replacementKey, built.path, "")
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	return kb.SnapshotShardMetadata{
		ShardID:        replacementID,
		Key:            replacementKey,
		Version:        uploadInfo.Version,
		SizeBytes:      built.sizeBytes,
		VectorRows:     built.vectorRows,
		CreatedAt:      now,
		SealedAt:       now,
		TombstoneRatio: 0,
		GraphAvailable: built.graphAvailable,
		Centroid:       built.centroid,
		SHA256:         built.sha,
	}, nil
}

func (f *DuckDBArtifactFormat) buildCompactionShard(ctx context.Context, tmpDir string, shards []kb.SnapshotShardMetadata) (builtCompactionShard, error) {
	combinedPath := filepath.Join(tmpDir, "replacement.duckdb")
	db, releaseThreads, err := f.openBuildDB(ctx, combinedPath)
	if err != nil {
		return builtCompactionShard{}, err
	}
	defer releaseThreads()
	defer db.Close()

	if err := f.mergeCompactionShards(ctx, db, tmpDir, shards); err != nil {
		return builtCompactionShard{}, err
	}
	vectorRows, err := countCompactedDocs(ctx, db)
	if err != nil {
		return builtCompactionShard{}, err
	}
	if err := buildShardIndexes(ctx, db); err != nil {
		return builtCompactionShard{}, err
	}
	c, err := centroid.Compute(ctx, db)
	if err != nil {
		return builtCompactionShard{}, err
	}
	graphAvailable := hasGraphQueryData(ctx, db)
	if err := CheckpointAndCloseDB(ctx, db, "close compacted shard db"); err != nil {
		return builtCompactionShard{}, err
	}
	sha, info, err := compactedShardFileInfo(ctx, combinedPath)
	if err != nil {
		return builtCompactionShard{}, err
	}
	return builtCompactionShard{path: combinedPath, vectorRows: vectorRows, sizeBytes: info.Size(), graphAvailable: graphAvailable, centroid: c, sha: sha}, nil
}

func (f *DuckDBArtifactFormat) mergeCompactionShards(
	ctx context.Context,
	db *sql.DB,
	tmpDir string,
	shards []kb.SnapshotShardMetadata,
) error {
	for i, shard := range shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("in-%05d.duckdb", i))
		if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return err
		}
		if err := reconstruct.MergeShardIntoDB(ctx, db, reconstruct.MergeOptions{Alias: fmt.Sprintf("s%d", i), PartPath: partPath, IsFirst: i == 0, EnsureGraphTables: EnsureGraphTables}); err != nil {
			return err
		}
	}
	return nil
}

func countCompactedDocs(ctx context.Context, db *sql.DB) (int64, error) {
	var vectorRows int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&vectorRows); err != nil {
		return 0, err
	}
	return vectorRows, nil
}

// buildShardIndexes builds HNSW and FTS indexes only. Used by compaction,
// where graph tables are already merged by mergeCompactionShards.
func buildShardIndexes(ctx context.Context, db *sql.DB) error {
	if err := createDocsVectorIndex(ctx, db); err != nil {
		return logDuckDBMemoryOnError(ctx, db, "build_hnsw_index", err)
	}
	return logDuckDBMemoryOnError(ctx, db, "build_fts_index", createDocsFTSIndex(ctx, db))
}

func createDocsVectorIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`)
	return err
}

func createDocsFTSIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `PRAGMA create_fts_index('docs', 'id', 'content', stemmer='none', stopwords='none', ignore='', strip_accents=1, lower=1, overwrite=1)`)
	return err
}

func compactedShardFileInfo(ctx context.Context, path string) (string, os.FileInfo, error) {
	sha, err := kb.FileContentSHA256(ctx, path)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, err
	}
	return sha, info, nil
}

// BuildAndUploadCompactionReplacement merges the given shards into one replacement shard and uploads it.
func (f *DuckDBArtifactFormat) BuildAndUploadCompactionReplacement(
	ctx context.Context,
	kbID string,
	shards []kb.SnapshotShardMetadata,
) (kb.SnapshotShardMetadata, error) {
	return f.buildAndUploadCompactionReplacement(ctx, kbID, shards)
}
