package kb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DuckDBArtifactFormat struct {
	deps DuckDBArtifactDeps
}

type DuckDBArtifactDeps struct {
	BlobStore      BlobStore
	ManifestStore  ManifestStore
	CacheDir       string
	MemoryLimit    string
	ExtensionDir   string
	OfflineExt     bool
	ShardingPolicy ShardingPolicy

	Embed        func(context.Context, string) ([]float32, error)
	GraphBuilder func() *GraphBuilder

	EvictCacheIfNeeded func(context.Context, string) error
	LockFor            func(string) *sync.Mutex
	Metrics            ShardMetricsObserver
}

func (d DuckDBArtifactDeps) validate() error {
	if d.BlobStore == nil {
		return fmt.Errorf("blob store is required")
	}
	if d.ManifestStore == nil {
		return fmt.Errorf("manifest store is required")
	}
	if d.Embed == nil {
		return fmt.Errorf("embed function is required")
	}
	if d.GraphBuilder == nil {
		return fmt.Errorf("graph builder function is required")
	}
	if d.EvictCacheIfNeeded == nil {
		return fmt.Errorf("cache eviction function is required")
	}
	if d.LockFor == nil {
		return fmt.Errorf("lock provider function is required")
	}
	if d.Metrics == nil {
		return fmt.Errorf("shard metrics observer is required")
	}
	return nil
}

const (
	DuckDBFormatKind    = "duckdb_sharded"
	DuckDBFormatVersion = 1
)

func (f *DuckDBArtifactFormat) validateManifestFormat(manifest *SnapshotShardManifest) error {
	if manifest.FormatKind != DuckDBFormatKind {
		return fmt.Errorf("%w: manifest format_kind %q does not match expected %q", ErrArtifactFormatNotConfigured, manifest.FormatKind, DuckDBFormatKind)
	}
	if manifest.FormatVersion != DuckDBFormatVersion {
		return fmt.Errorf("%w: manifest format_version %d does not match expected %d", ErrArtifactFormatNotConfigured, manifest.FormatVersion, DuckDBFormatVersion)
	}
	return nil
}

func NewDuckDBArtifactFormat(deps DuckDBArtifactDeps) (*DuckDBArtifactFormat, error) {
	if err := deps.validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrArtifactFormatNotConfigured, err)
	}
	return &DuckDBArtifactFormat{deps: deps}, nil
}

func (f *DuckDBArtifactFormat) Kind() string {
	return DuckDBFormatKind
}

func (f *DuckDBArtifactFormat) Version() int {
	return DuckDBFormatVersion
}

func (f *DuckDBArtifactFormat) FileExt() string {
	return ".duckdb"
}

func (f *DuckDBArtifactFormat) lockFor(kbID string) *sync.Mutex {
	return f.deps.LockFor(kbID)
}

func (f *DuckDBArtifactFormat) openConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	if f.deps.ExtensionDir != "" {
		if _, err := db.Exec(fmt.Sprintf(`SET extension_directory = '%s'`, f.deps.ExtensionDir)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set extension_directory: %w", err)
		}
	}

	if f.deps.OfflineExt {
		if _, err := db.Exec(`SET autoinstall_known_extensions = false`); err != nil {
			db.Close()
			return nil, fmt.Errorf("disable autoinstall: %w", err)
		}
	}

	if _, err := db.Exec(`LOAD vss`); err != nil {
		if f.deps.OfflineExt {
			db.Close()
			return nil, fmt.Errorf("failed to load vss extension in offline mode (check extension_directory %q): %w", f.deps.ExtensionDir, err)
		}
		if _, installErr := db.Exec(`INSTALL vss`); installErr != nil {
			db.Close()
			return nil, fmt.Errorf("failed to install vss: %w", installErr)
		}
		if _, loadErr := db.Exec(`LOAD vss`); loadErr != nil {
			db.Close()
			return nil, fmt.Errorf("failed to load vss after install: %w", loadErr)
		}
	}

	if _, err := db.Exec(`SET hnsw_enable_experimental_persistence = true`); err != nil {
		db.Close()
		return nil, err
	}

	memLimit := f.deps.MemoryLimit
	if memLimit == "" {
		memLimit = "128MB"
	}
	_, err = db.Exec(fmt.Sprintf(`
		SET memory_limit = '%s';
		PRAGMA threads = 1;
	`, memLimit))
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (f *DuckDBArtifactFormat) BuildArtifacts(ctx context.Context, kbID, srcPath string, targetBytes int64) ([]SnapshotShardMetadata, error) {
	if targetBytes <= 0 {
		targetBytes = defaultSnapshotShardSize
	}
	dbHash, err := fileContentSHA256(srcPath)
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

func (f *DuckDBArtifactFormat) QueryRag(ctx context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
	if err := validateRagQueryRequest(req); err != nil {
		return nil, err
	}

	results, err := f.searchTopK(ctx, req.KBID, req.QueryVec, req.Options.TopK)
	if err != nil {
		return nil, err
	}
	expanded := expandedFromVector(results)
	if req.Options.MaxDistance == nil {
		return expanded, nil
	}
	maxDistance := *req.Options.MaxDistance
	filtered := make([]ExpandedResult, 0, len(expanded))
	for _, r := range expanded {
		if r.Distance <= maxDistance {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (f *DuckDBArtifactFormat) QueryGraph(ctx context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
	if err := validateGraphQueryRequest(req); err != nil {
		return nil, err
	}
	if err := f.ensureGraphModeAvailable(ctx, req.KBID); err != nil {
		return nil, err
	}

	options := normalizeExpansionOptions(req.Options.TopK, req.Options.Expansion)
	options.OfflineExt = f.deps.OfflineExt
	selection, err := f.resolveVectorQuerySelection(ctx, req.KBID, req.QueryVec)
	if err != nil {
		return nil, fmt.Errorf("select vector query path: %w", err)
	}
	parallelism := selection.Plan.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]ExpandedResult, len(selection.Plan.Shards))
	errCh := make(chan error, 1)
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for i := range selection.Plan.Shards {
		idx := i
		shard := selection.Plan.Shards[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			localPath, _, err := f.ensureLocalShardFile(ctx, req.KBID, shard)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			db, err := f.openConfiguredDB(ctx, localPath)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			defer db.Close()

			shardResults, err := searchExpandedWithDB(ctx, db, req.QueryVec, req.Options.TopK, options)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			results[idx] = shardResults
		}()
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	merged := mergeExpandedShardResults(results, req.Options.TopK)
	if req.Options.MaxDistance == nil {
		return merged, nil
	}
	maxDistance := *req.Options.MaxDistance
	filtered := make([]ExpandedResult, 0, len(merged))
	for _, r := range merged {
		if r.Distance <= maxDistance {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (f *DuckDBArtifactFormat) Upsert(ctx context.Context, req IngestUpsertRequest) (IngestResult, error) {
	if req.KBID == "" {
		return IngestResult{}, fmt.Errorf("kbID cannot be empty")
	}
	if len(req.Docs) == 0 {
		return IngestResult{}, nil
	}

	graphBuilder := f.deps.GraphBuilder()
	if req.Options.GraphEnabled != nil {
		if *req.Options.GraphEnabled {
			if graphBuilder == nil {
				return IngestResult{}, ErrGraphUnavailable
			}
		} else {
			graphBuilder = nil
		}
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, req.KBID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	seedVec, err := f.deps.Embed(ctx, req.Docs[0].Text)
	if err != nil {
		return IngestResult{}, fmt.Errorf("embed doc %q for shard bootstrap: %w", req.Docs[0].ID, err)
	}
	if len(seedVec) == 0 {
		return IngestResult{}, fmt.Errorf("embed doc %q for shard bootstrap: empty embedding", req.Docs[0].ID)
	}
	if err := f.ensureMutableShardDBLocked(ctx, req.KBID, kbDir, dbPath, len(seedVec), true); err != nil {
		return IngestResult{}, err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return IngestResult{}, err
	}
	defer db.Close()

	if err := f.applyUpsert(ctx, db, req.Docs, graphBuilder); err != nil {
		return IngestResult{}, err
	}

	policy := normalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommit(ctx, db, req.KBID, req.Upload, policy.TargetShardBytes); err != nil {
		return IngestResult{}, err
	}
	if req.Upload {
		if err := f.cleanupLegacySnapshotObjectsBestEffort(ctx, req.KBID); err != nil {
			return IngestResult{}, err
		}
	}

	return IngestResult{MutatedCount: len(req.Docs)}, nil
}

func (f *DuckDBArtifactFormat) Delete(ctx context.Context, req IngestDeleteRequest) (IngestResult, error) {
	if req.KBID == "" {
		return IngestResult{}, fmt.Errorf("kbID cannot be empty")
	}
	if len(req.DocIDs) == 0 {
		return IngestResult{}, nil
	}

	cleanIDs := make([]string, 0, len(req.DocIDs))
	for _, id := range req.DocIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return IngestResult{}, fmt.Errorf("doc id cannot be empty")
		}
		cleanIDs = append(cleanIDs, trimmed)
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, req.KBID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	if err := f.ensureMutableShardDBLocked(ctx, req.KBID, kbDir, dbPath, 0, false); err != nil {
		return IngestResult{}, err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return IngestResult{}, err
	}
	defer db.Close()

	if err := f.applyDelete(ctx, db, cleanIDs, req.Options); err != nil {
		return IngestResult{}, err
	}

	policy := normalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommit(ctx, db, req.KBID, req.Upload, policy.TargetShardBytes); err != nil {
		return IngestResult{}, err
	}

	return IngestResult{MutatedCount: len(cleanIDs)}, nil
}

// ensureMutableShardDBLocked ensures a mutable DuckDB file is present at
// dbPath and consistent with the current remote manifest.
// The caller must hold the per-KB write lock before calling this function.
func (f *DuckDBArtifactFormat) ensureMutableShardDBLocked(
	ctx context.Context,
	kbID string,
	kbDir string,
	dbPath string,
	embeddingDim int,
	allowBootstrap bool,
) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kbID cannot be empty")
	}

	manifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, kbID)
	if err != nil {
		return err
	}
	if manifestVersion == "" {
		if !allowBootstrap {
			return ErrKBUninitialized
		}
		if embeddingDim <= 0 {
			return fmt.Errorf("embedding dimension must be > 0")
		}
		return f.createEmptyMutableShardDBLocked(ctx, kbDir, dbPath, embeddingDim)
	}

	localVersionPath := localShardManifestVersionPath(kbDir)
	localVersion, readErr := readLocalShardManifestVersion(localVersionPath)
	hasLocalVersion := readErr == nil
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	if hasLocalVersion && localVersion == manifestVersion {
		if _, statErr := os.Stat(dbPath); statErr == nil {
			return nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}

	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}
	if _, err := f.downloadSnapshotFromShards(ctx, kbID, dbPath); err != nil {
		return err
	}
	if err := writeLocalShardManifestVersion(localVersionPath, manifestVersion); err != nil {
		return err
	}

	return nil
}

// createEmptyMutableShardDBLocked initialises an empty DuckDB file at dbPath.
func (f *DuckDBArtifactFormat) createEmptyMutableShardDBLocked(ctx context.Context, kbDir, dbPath string, embeddingDim int) error {
	if embeddingDim <= 0 {
		return fmt.Errorf("embedding dimension must be > 0")
	}
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}

	createDocsSQL := `
		CREATE TABLE IF NOT EXISTS docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[` + strconv.Itoa(embeddingDim) + `]
		)
	`
	if _, err := db.ExecContext(ctx, createDocsSQL); err != nil {
		_ = db.Close()
		return fmt.Errorf("create docs table for shard bootstrap: %w", err)
	}
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := EnsureGraphTables(ctx, db); err != nil {
		_ = db.Close()
		return err
	}

	if err := checkpointAndCloseDB(ctx, db, "close db after shard bootstrap"); err != nil {
		return err
	}
	return nil
}

func (f *DuckDBArtifactFormat) postMutationCommit(ctx context.Context, db *sql.DB, kbID string, upload bool, targetShardBytes int64) error {
	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")

	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	if err := checkpointDB(ctx, db); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db after mutation: %w", err)
	}
	dbClosed = true

	if upload {
		expectedManifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, kbID)
		if err != nil {
			return err
		}

		artifacts, err := f.BuildArtifacts(ctx, kbID, dbPath, targetShardBytes)
		if err != nil {
			return err
		}

		totalSize := int64(0)
		for _, a := range artifacts {
			totalSize += a.SizeBytes
		}
		manifest := SnapshotShardManifest{
			SchemaVersion:  1,
			Layout:         shardManifestLayoutDuckDBs,
			FormatKind:     DuckDBFormatKind,
			FormatVersion:  DuckDBFormatVersion,
			KBID:           kbID,
			CreatedAt:      time.Now().UTC(),
			TotalSizeBytes: totalSize,
			Shards:         artifacts,
		}

		newVersion, err := f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
		if err != nil {
			if errors.Is(err, ErrBlobVersionMismatch) {
				f.deps.Metrics.RecordShardCount(kbID, len(artifacts))
			}
			return err
		}
		f.deps.Metrics.RecordShardCount(kbID, len(artifacts))
		if err := writeLocalShardManifestVersion(localShardManifestVersionPath(kbDir), newVersion); err != nil {
			return err
		}
	}

	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return err
	}

	return nil
}

func (f *DuckDBArtifactFormat) cleanupLegacySnapshotObjectsBestEffort(ctx context.Context, kbID string) error {
	legacyKeys := []string{
		kbID + ".duckdb",
		kbID + ".snapshot.json",
	}
	for _, key := range legacyKeys {
		err := f.deps.BlobStore.Delete(ctx, key)
		if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			continue
		}
		slog.Default().WarnContext(ctx, "failed to delete legacy snapshot object", "kb_id", kbID, "key", key, "error", err)
	}
	return nil
}

// PrepareAndOpenDB prepares the working state and opens the DB for direct access.
func (f *DuckDBArtifactFormat) PrepareAndOpenDB(ctx context.Context, kbID string) (*sql.DB, error) {
	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	if err := f.ensureMutableShardDBLocked(ctx, kbID, kbDir, dbPath, 0, false); err != nil {
		return nil, err
	}
	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return nil, err
	}
	return f.openConfiguredDB(ctx, dbPath)
}

func (f *DuckDBArtifactFormat) uploadQueryableSnapshotShards(ctx context.Context, prefix, localDBPath string, partSize int64) ([]SnapshotShardMetadata, int64, error) {
	sourceDB, err := f.openConfiguredDB(ctx, localDBPath)
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
		vectorRows, graphAvailable, err := f.buildShardDBFromSourceRange(ctx, localDBPath, hasTombstones, shardPath, rowsPerShard, offset)
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
		uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, partKey, shardPath, "")
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

func (f *DuckDBArtifactFormat) buildShardDBFromSourceRange(ctx context.Context, sourceDBPath string, hasTombstones bool, shardPath string, limit, offset int) (int64, bool, error) {
	if limit <= 0 {
		return 0, false, fmt.Errorf("limit must be > 0")
	}
	if offset < 0 {
		offset = 0
	}

	shardDB, err := f.openConfiguredDB(ctx, shardPath)
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

func (f *DuckDBArtifactFormat) downloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*SnapshotShardManifest, error) {
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if dest == "" {
		return nil, fmt.Errorf("dest cannot be empty")
	}

	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		return nil, err
	}
	manifest := doc.Manifest
	if err := f.validateManifestFormat(&manifest); err != nil {
		return nil, err
	}
	f.deps.Metrics.RecordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return nil, fmt.Errorf("manifest has no shards")
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-shard-download-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "kbcore-reconstruct-*.duckdb")
	if err != nil {
		return nil, err
	}
	tmpDest := tmpFile.Name()
	tmpFile.Close()
	_ = os.Remove(tmpDest)
	defer os.Remove(tmpDest)

	db, err := f.openConfiguredDB(ctx, tmpDest)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	for i, shard := range manifest.Shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("part-%05d.duckdb", i))
		if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return nil, err
		}
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

func (f *DuckDBArtifactFormat) buildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []SnapshotShardMetadata) (SnapshotShardMetadata, error) {
	tmpDir, err := os.MkdirTemp("", "kbcore-compact-*")
	if err != nil {
		return SnapshotShardMetadata{}, err
	}
	defer os.RemoveAll(tmpDir)

	combinedPath := filepath.Join(tmpDir, "replacement.duckdb")
	db, err := f.openConfiguredDB(ctx, combinedPath)
	if err != nil {
		return SnapshotShardMetadata{}, err
	}
	defer db.Close()

	vectorRows := int64(0)
	graphAvailable := false
	for i, shard := range shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("in-%05d.duckdb", i))
		if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
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
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, replacementKey, combinedPath, "")
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

// BuildAndUploadCompactionReplacement merges the given shards into one replacement shard and uploads it.
func (f *DuckDBArtifactFormat) BuildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []SnapshotShardMetadata) (SnapshotShardMetadata, error) {
	return f.buildAndUploadCompactionReplacement(ctx, kbID, shards)
}

// DownloadSnapshotFromShards downloads and reconstructs a sharded snapshot into a single DB file.
func (f *DuckDBArtifactFormat) DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*SnapshotShardManifest, error) {
	return f.downloadSnapshotFromShards(ctx, kbID, dest)
}

func (f *DuckDBArtifactFormat) ensureGraphModeAvailable(ctx context.Context, kbID string) error {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			return ErrKBUninitialized
		}
		return fmt.Errorf("download shard manifest: %w", err)
	}
	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return err
	}
	if len(manifest.Shards) == 0 {
		return ErrGraphQueryUnavailable
	}
	for _, shard := range manifest.Shards {
		if !shard.GraphAvailable {
			return ErrGraphQueryUnavailable
		}
	}

	return nil
}

func (f *DuckDBArtifactFormat) applyUpsert(ctx context.Context, db *sql.DB, docs []Document, graphBuilder *GraphBuilder) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	var graphResult *GraphBuildResult
	var err error
	if graphBuilder != nil {
		graphResult, err = graphBuilder.Build(ctx, docs)
		if err != nil {
			return fmt.Errorf("build graph for upsert docs: %w", err)
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}

	stmtUndelete, err := tx.PrepareContext(ctx, `DELETE FROM doc_tombstones WHERE doc_id = ?`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare doc_tombstones delete: %w", err)
	}
	defer stmtUndelete.Close()

	for _, doc := range docs {
		if strings.TrimSpace(doc.ID) == "" {
			tx.Rollback()
			return fmt.Errorf("doc id cannot be empty")
		}
		if strings.TrimSpace(doc.Text) == "" {
			tx.Rollback()
			return fmt.Errorf("doc %q text cannot be empty", doc.ID)
		}

		vec, err := f.deps.Embed(ctx, doc.Text)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("embed doc %q: %w", doc.ID, err)
		}
		if len(vec) == 0 {
			tx.Rollback()
			return fmt.Errorf("embed doc %q: empty embedding", doc.ID)
		}

		vecStr := formatVectorForSQL(vec)
		if _, err := tx.ExecContext(ctx, `DELETE FROM docs WHERE id = ?`, doc.ID); err != nil {
			tx.Rollback()
			return fmt.Errorf("delete existing doc %q before upsert: %w", doc.ID, err)
		}
		upsertSQL := fmt.Sprintf(`INSERT INTO docs (id, content, embedding) VALUES (?, ?, %s::FLOAT[%d])`, vecStr, len(vec))
		if _, err := tx.ExecContext(ctx, upsertSQL, doc.ID, doc.Text); err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert doc %q: %w", doc.ID, err)
		}
		if _, err := stmtUndelete.ExecContext(ctx, doc.ID); err != nil {
			tx.Rollback()
			return fmt.Errorf("clear tombstone for doc %q: %w", doc.ID, err)
		}
	}

	if graphResult != nil {
		docIDs := make([]string, 0, len(docs))
		for _, d := range docs {
			docIDs = append(docIDs, d.ID)
		}
		if err := pruneGraphForDocsTx(ctx, tx, docIDs, true); err != nil {
			tx.Rollback()
			return err
		}
		if err := InsertGraphBuildResultTx(ctx, tx, graphResult); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert tx: %w", err)
	}

	return nil
}

func (f *DuckDBArtifactFormat) applyDelete(ctx context.Context, db *sql.DB, cleanIDs []string, opts DeleteDocsOptions) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete docs tx: %w", err)
	}

	placeholders := buildInClausePlaceholders(len(cleanIDs))
	args := make([]any, 0, len(cleanIDs))
	for _, id := range cleanIDs {
		args = append(args, id)
	}

	if opts.HardDelete {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM docs WHERE id IN (%s)`, placeholders), args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("hard delete docs: %w", err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM doc_tombstones WHERE doc_id IN (%s)`, placeholders), args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("remove tombstones: %w", err)
		}
	} else {
		stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO doc_tombstones (doc_id, deleted_at) VALUES (?, NOW())`)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("prepare tombstone upsert: %w", err)
		}
		for _, id := range cleanIDs {
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("soft delete doc %q: %w", id, err)
			}
		}
		if err := stmt.Close(); err != nil {
			tx.Rollback()
			return fmt.Errorf("close tombstone stmt: %w", err)
		}
	}

	if err := pruneGraphForDocsTx(ctx, tx, cleanIDs, opts.HardDelete && opts.CleanupGraph); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete docs tx: %w", err)
	}

	return nil
}
