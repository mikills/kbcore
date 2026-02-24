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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

	dbHash, err := fileContentSHA256(localDBPath)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("%s.duckdb.shards/%s", kbID, dbHash[:16])

	parts, totalSize, err := l.uploadQueryableSnapshotShards(ctx, prefix, localDBPath, partSize)
	if err != nil {
		return nil, err
	}

	manifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         shardManifestLayoutDuckDBs,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: totalSize,
		Shards:         parts,
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-manifest-*")
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

	info, err := l.BlobStore.UploadIfMatch(ctx, shardManifestKey(kbID), manifestPath, expectedManifestVersion)
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			l.recordManifestCASConflict(kbID)
		}
		return nil, err
	}
	l.recordShardCount(kbID, len(parts))
	return info, nil
}

// DownloadSnapshotFromShards downloads a sharded snapshot manifest and
// reconstructs a single DB file at dest.
func (l *KB) DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*SnapshotShardManifest, error) {
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if dest == "" {
		return nil, fmt.Errorf("dest cannot be empty")
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-manifest-download-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	manifestPath := filepath.Join(tmpDir, "manifest.json")

	if err := l.BlobStore.Download(ctx, shardManifestKey(kbID), manifestPath); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	var manifest SnapshotShardManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	l.recordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return nil, fmt.Errorf("manifest has no shards")
	}

	// CreateTemp in the same directory as dest keeps os.Rename atomic (same
	// filesystem). Remove the 0-byte placeholder immediately — DuckDB requires
	// creating its own file structure and rejects a pre-existing empty file.
	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "kbcore-reconstruct-*.duckdb")
	if err != nil {
		return nil, err
	}
	tmpDest := tmpFile.Name()
	tmpFile.Close()
	_ = os.Remove(tmpDest)
	defer os.Remove(tmpDest)

	db, err := l.openConfiguredDB(ctx, tmpDest)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	for i, shard := range manifest.Shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("part-%05d.duckdb", i))
		if err := l.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return nil, err
		}
		// Check size first (cheap stat) before computing SHA256 (full file read).
		info, err := os.Stat(partPath)
		if err != nil {
			return nil, err
		}
		if shard.SizeBytes > 0 && info.Size() != shard.SizeBytes {
			return nil, fmt.Errorf("shard %s size mismatch", shard.ShardID)
		}
		if shard.SHA256 != "" {
			sha, err := fileContentSHA256(partPath)
			if err != nil {
				return nil, err
			}
			if sha != shard.SHA256 {
				return nil, fmt.Errorf("shard %s checksum mismatch", shard.ShardID)
			}
		}

		alias := fmt.Sprintf("s%d", i)
		if err := mergeShardIntoDB(ctx, db, alias, partPath, i == 0); err != nil {
			return nil, err
		}
	}
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return nil, err
	}
	if err := checkpointAndCloseDB(ctx, db, "close reconstructed shard snapshot"); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpDest, dest); err != nil {
		return nil, err
	}

	return &manifest, nil
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

func (l *KB) uploadQueryableSnapshotShards(ctx context.Context, prefix, localDBPath string, partSize int64) ([]SnapshotShardMetadata, int64, error) {
	sourceDB, err := l.openConfiguredDB(ctx, localDBPath)
	if err != nil {
		return nil, 0, err
	}
	defer sourceDB.Close()

	hasTombstones, err := tableExists(ctx, sourceDB, "doc_tombstones")
	if err != nil {
		return nil, 0, err
	}
	activeRows, err := countActiveDocs(ctx, sourceDB, hasTombstones)
	if err != nil {
		return nil, 0, err
	}
	if activeRows <= 0 {
		return nil, 0, fmt.Errorf("cannot shard snapshot with zero active docs")
	}

	sourceInfo, err := os.Stat(localDBPath)
	if err != nil {
		return nil, 0, err
	}
	bytesPerRow := sourceInfo.Size() / activeRows
	if bytesPerRow <= 0 {
		bytesPerRow = 1
	}
	rowsPerShard := int(partSize / bytesPerRow)
	if rowsPerShard <= 0 {
		rowsPerShard = 1
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-queryable-shards-*")
	if err != nil {
		return nil, 0, err
	}
	defer os.RemoveAll(tmpDir)

	estimatedShards := int((activeRows + int64(rowsPerShard) - 1) / int64(rowsPerShard))
	parts := make([]SnapshotShardMetadata, 0, estimatedShards)
	totalSize := int64(0)
	for shardIndex, offset := 0, 0; int64(offset) < activeRows; shardIndex, offset = shardIndex+1, offset+rowsPerShard {
		shardPath := filepath.Join(tmpDir, fmt.Sprintf("shard-%05d.duckdb", shardIndex))
		vectorRows, graphAvailable, err := l.buildShardDBFromSourceRange(ctx, localDBPath, hasTombstones, shardPath, rowsPerShard, offset)
		if err != nil {
			return nil, 0, err
		}
		if vectorRows <= 0 {
			continue
		}

		sha, err := fileContentSHA256(shardPath)
		if err != nil {
			return nil, 0, err
		}
		info, err := os.Stat(shardPath)
		if err != nil {
			return nil, 0, err
		}
		partKey := fmt.Sprintf("%s/shard-%05d.duckdb", prefix, shardIndex)
		uploadInfo, err := l.BlobStore.UploadIfMatch(ctx, partKey, shardPath, "")
		if err != nil {
			return nil, 0, err
		}

		now := time.Now().UTC()
		parts = append(parts, SnapshotShardMetadata{
			ShardID:        fmt.Sprintf("shard-%05d", shardIndex),
			Key:            partKey,
			Version:        uploadInfo.Version,
			SizeBytes:      info.Size(),
			VectorRows:     vectorRows,
			CreatedAt:      now,
			SealedAt:       now,
			TombstoneRatio: 0,
			GraphAvailable: graphAvailable,
			SHA256:         sha,
		})
		totalSize += info.Size()
	}

	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("failed to build shard files")
	}
	return parts, totalSize, nil
}

func (l *KB) buildShardDBFromSourceRange(ctx context.Context, sourceDBPath string, hasTombstones bool, shardPath string, limit, offset int) (int64, bool, error) {
	if limit <= 0 {
		return 0, false, fmt.Errorf("limit must be > 0")
	}
	if offset < 0 {
		offset = 0
	}

	shardDB, err := l.openConfiguredDB(ctx, shardPath)
	if err != nil {
		return 0, false, err
	}
	defer shardDB.Close()

	if _, err := shardDB.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS src (READ_ONLY)", quoteSQLString(sourceDBPath))); err != nil {
		return 0, false, err
	}
	defer func() {
		_, _ = shardDB.ExecContext(context.Background(), "DETACH src")
	}()

	where := ""
	if hasTombstones {
		where = "WHERE NOT EXISTS (SELECT 1 FROM src.doc_tombstones t WHERE t.doc_id = d.id)"
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE docs AS
		SELECT d.id, d.content, d.embedding
		FROM src.docs d
		%s
		ORDER BY d.id
		LIMIT %d OFFSET %d
	`, where, limit, offset)
	if _, err := shardDB.ExecContext(ctx, createSQL); err != nil {
		return 0, false, err
	}
	if _, err := shardDB.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return 0, false, err
	}

	if err := EnsureGraphTables(ctx, shardDB); err != nil {
		return 0, false, err
	}
	if ok, err := sourceGraphTablesReady(ctx, shardDB); err != nil {
		return 0, false, err
	} else if ok {
		if _, err := shardDB.ExecContext(ctx, `
			INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id)
			SELECT de.doc_id, de.entity_id, de.weight, de.chunk_id
			FROM src.doc_entities de
			JOIN docs d ON d.id = de.doc_id
		`); err != nil {
			return 0, false, err
		}
		if _, err := shardDB.ExecContext(ctx, `
			INSERT OR IGNORE INTO entities (id, name)
			SELECT DISTINCT e.id, e.name
			FROM src.entities e
			JOIN doc_entities de ON de.entity_id = e.id
		`); err != nil {
			return 0, false, err
		}
		if _, err := shardDB.ExecContext(ctx, `
			INSERT INTO edges (src, dst, weight, rel_type, chunk_id)
			SELECT DISTINCT e.src, e.dst, e.weight, e.rel_type, e.chunk_id
			FROM src.edges e
			JOIN doc_entities de ON de.chunk_id = e.chunk_id
		`); err != nil {
			return 0, false, err
		}
	}

	var rowCount int64
	if err := shardDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&rowCount); err != nil {
		return 0, false, err
	}
	graphAvailable := hasGraphQueryData(ctx, shardDB)
	if err := checkpointAndCloseDB(ctx, shardDB, "close shard db"); err != nil {
		return 0, false, err
	}
	return rowCount, graphAvailable, nil
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
