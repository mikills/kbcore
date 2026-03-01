// Snapshot publishing and reconstruction for sharded knowledge bases.
//
// This file implements the two-direction transfer of a KB between local DuckDB
// files and BlobStore-backed shard objects:
//
//   - Upload: a mutable local DuckDB is split into fixed-size, independently
//     queryable shard files, each uploaded as an immutable blob, and a manifest
//     JSON is published to replace the previous snapshot.
//   - Download: the manifest is fetched, every shard is downloaded and verified,
//     and all shard tables are merged into a single reconstructed DuckDB file
//     with a fresh HNSW index.
//
// System fit:
//
//   - Upload acquires a write lease before any work begins, ensuring only one
//     writer operates on a KB at a time cluster-wide.
//   - The manifest is published via compare-and-set (UploadIfMatch with the
//     caller-supplied expectedManifestVersion). A concurrent writer that
//     published first will cause ErrBlobVersionMismatch; the caller retries.
//   - Shard files are named by content hash prefix, making them immutable and
//     safe to upload concurrently. A losing CAS writer's uploaded shards are
//     simply orphaned and collected by the shard GC pass.
//
// Sharding algorithm (upload):
//
//   - Active row count and file size are measured on the source DB to estimate
//     bytes-per-row. Rows are assigned to shards in ID order using LIMIT/OFFSET
//     so that shard boundaries are deterministic for a given snapshot.
//   - Each shard is a standalone DuckDB file with its own docs table, HNSW
//     index, and graph tables (doc_entities, entities, edges) copied from the
//     rows that fell into that shard's range.
//   - Tombstoned rows are excluded at shard build time, reclaiming storage
//     without an explicit compaction pass.
//   - Each shard's SHA256 and size are recorded in the manifest for integrity
//     verification on download.
//
// Reconstruction (download):
//
//   - Each shard is downloaded, its SHA256 and size checked against the
//     manifest, then ATTACHed read-only. Rows (docs, graph tables) are
//     INSERT-selected into a new combined DuckDB file.
//   - After all shards are merged, an HNSW index is built over the full
//     combined docs table and the file is checkpointed and atomically renamed
//     into place.
//
// Failure modes:
//
//   - Checksum or size mismatch on a downloaded shard aborts reconstruction.
//   - Zero active docs in the source DB aborts upload (nothing to shard).
//   - Manifest CAS conflict (ErrBlobVersionMismatch) aborts upload; the caller
//     owns the retry decision.
//   - Graph table copy is best-effort: absent graph tables in a shard are
//     skipped silently; errors during copy abort the operation.

package kb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultSnapshotShardSize   int64  = 16 * 1024 * 1024
	shardManifestLayoutDuckDBs string = "duckdb_shard_files"
)

// SnapshotShardMetadata describes one logical shard file in the manifest.
type SnapshotShardMetadata struct {
	ShardID        string    `json:"shard_id"`
	Key            string    `json:"key"`
	Version        string    `json:"version,omitempty"`
	SizeBytes      int64     `json:"size_bytes"`
	VectorRows     int64     `json:"vector_rows"`
	CreatedAt      time.Time `json:"created_at"`
	SealedAt       time.Time `json:"sealed_at,omitempty"`
	TombstoneRatio float64   `json:"tombstone_ratio"`
	GraphAvailable bool      `json:"graph_available"`
	Centroid       []float32 `json:"centroid,omitempty"`
	SHA256         string    `json:"sha256,omitempty"`
}

// SnapshotShardManifest describes a sharded snapshot using one manifest key.
type SnapshotShardManifest struct {
	SchemaVersion  int                     `json:"schema_version"`
	Layout         string                  `json:"layout,omitempty"`
	FormatKind     string                  `json:"format_kind,omitempty"`
	FormatVersion  int                     `json:"format_version,omitempty"`
	KBID           string                  `json:"kb_id"`
	CreatedAt      time.Time               `json:"created_at"`
	TotalSizeBytes int64                   `json:"total_size_bytes"`
	Shards         []SnapshotShardMetadata `json:"shards"`
}

func shardManifestKey(kbID string) string {
	return kbID + ".duckdb.manifest.json"
}

// UploadSnapshotShardedIfMatch uploads a DB snapshot as immutable shard objects
// and updates the manifest with optimistic version matching.
func (l *KB) UploadSnapshotShardedIfMatch(ctx context.Context, kbID, localDBPath, expectedManifestVersion string, partSize int64) (*BlobObjectInfo, error) {
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if localDBPath == "" {
		return nil, fmt.Errorf("localDBPath cannot be empty")
	}
	if partSize <= 0 {
		partSize = defaultSnapshotShardSize
	}

	leaseManager, lease, err := l.acquireWriteLease(ctx, kbID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = leaseManager.Release(context.Background(), lease)
	}()

	if l.ArtifactFormat == nil {
		return nil, ErrArtifactFormatNotConfigured
	}
	artifacts, err := l.ArtifactFormat.BuildArtifacts(ctx, kbID, localDBPath, partSize)
	if err != nil {
		return nil, err
	}

	totalSize := int64(0)
	for _, a := range artifacts {
		totalSize += a.SizeBytes
	}

	manifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         shardManifestLayoutDuckDBs,
		FormatKind:     l.ArtifactFormat.Kind(),
		FormatVersion:  l.ArtifactFormat.Version(),
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: totalSize,
		Shards:         artifacts,
	}

	newVersion, err := l.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			l.recordManifestCASConflict(kbID)
		}
		return nil, err
	}
	l.recordShardCount(kbID, len(artifacts))
	return &BlobObjectInfo{
		Key:     shardManifestKey(kbID),
		Version: newVersion,
	}, nil
}

// DownloadSnapshotFromShards downloads a sharded snapshot manifest and
// reconstructs a single DB file at dest.
func (l *KB) DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*SnapshotShardManifest, error) {
	if l.ArtifactFormat == nil {
		return nil, ErrArtifactFormatNotConfigured
	}
	return l.ArtifactFormat.DownloadSnapshotFromShards(ctx, kbID, dest)
}

// mergeShardIntoDB ATTACHes a shard file under alias, copies docs and graph
// tables into db, then DETACHes via defer. isFirst signals that the target
// tables must be created before inserting.
func mergeShardIntoDB(ctx context.Context, db *sql.DB, alias, partPath string, isFirst bool) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", quoteSQLString(partPath), alias)); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.Background(), fmt.Sprintf("DETACH %s", alias)) }()

	if isFirst {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE docs AS SELECT * FROM %s.docs WHERE 1=0", alias)); err != nil {
			return err
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO docs SELECT * FROM %s.docs", alias)); err != nil {
		return err
	}
	if ok, err := attachedTableExists(ctx, db, alias, "doc_entities"); err != nil {
		return err
	} else if ok {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO doc_entities SELECT * FROM %s.doc_entities", alias)); err != nil {
			return err
		}
	}
	if ok, err := attachedTableExists(ctx, db, alias, "entities"); err != nil {
		return err
	} else if ok {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT OR IGNORE INTO entities SELECT * FROM %s.entities", alias)); err != nil {
			return err
		}
	}
	if ok, err := attachedTableExists(ctx, db, alias, "edges"); err != nil {
		return err
	} else if ok {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO edges SELECT * FROM %s.edges", alias)); err != nil {
			return err
		}
	}
	return nil
}

func countActiveDocs(ctx context.Context, db *sql.DB, hasTombstones bool) (int64, error) {
	query := `SELECT COUNT(*) FROM docs d`
	if hasTombstones {
		query += ` WHERE NOT EXISTS (SELECT 1 FROM doc_tombstones t WHERE t.doc_id = d.id)`
	}
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func hasGraphQueryData(ctx context.Context, db *sql.DB) bool {
	if err := ensureGraphQueryReady(ctx, db); err != nil {
		return false
	}
	return true
}

func sourceGraphTablesReady(ctx context.Context, db *sql.DB) (bool, error) {
	for _, table := range []string{"entities", "edges", "doc_entities"} {
		ok, err := attachedTableExists(ctx, db, "src", table)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func attachedTableExists(ctx context.Context, db *sql.DB, alias, table string) (bool, error) {
	query := fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 1", alias, table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found") {
			return false, nil
		}
		return false, err
	}
	return true, rows.Close()
}

func quoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
