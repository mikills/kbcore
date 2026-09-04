package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kb "github.com/mikills/minnow/kb"
	graph "github.com/mikills/minnow/kb/duckdb/internal/graph"
	"golang.org/x/sync/errgroup"
)

func (f *DuckDBArtifactFormat) Ingest(ctx context.Context, req kb.IngestUpsertRequest) (kb.IngestResult, error) {
	if empty, err := validateMutationDocs(req.KBID, len(req.Docs)); err != nil || empty {
		return kb.IngestResult{}, err
	}

	preparedDocs, err := f.prepareDocsForUpsert(ctx, req.Docs)
	if err != nil {
		return kb.IngestResult{}, err
	}

	graphBuilder, err := f.graphBuilderForUpsert(req.Options)
	if err != nil {
		return kb.IngestResult{}, err
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	db, err := f.openPreparedMutationDB(ctx, req.KBID, preparedDocs, req.Docs[0].ID, req.Options.DeferPublish)
	if err != nil {
		return kb.IngestResult{}, err
	}
	defer db.Close()
	expectedDim, err := duckDBEmbeddingDimension(ctx, db)
	if err != nil {
		return kb.IngestResult{}, err
	}
	if err := validatePreparedDocDimensions(preparedDocs, expectedDim, "upsert embedding dimension is incompatible with stored vectors"); err != nil {
		return kb.IngestResult{}, err
	}

	if err := f.applyUpsert(ctx, db, preparedDocs, graphBuilder); err != nil {
		return kb.IngestResult{}, logDuckDBMemoryOnError(ctx, db, "upsert", err)
	}

	if err := f.commitUpsertMutation(ctx, db, req); err != nil {
		return kb.IngestResult{}, err
	}
	return kb.IngestResult{MutatedCount: len(req.Docs)}, nil
}

func (f *DuckDBArtifactFormat) commitUpsertMutation(ctx context.Context, db *sql.DB, req kb.IngestUpsertRequest) error {
	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	commit := mutationCommitOptions{
		kbID: req.KBID, upload: req.Upload, deferred: req.Options.DeferPublish,
		targetShardBytes: policy.TargetShardBytes, maxAttempts: 1,
	}
	if err := f.postMutationCommitWithRetry(ctx, db, commit); err != nil {
		return err
	}
	return f.cleanupUploadedPreShardSnapshot(ctx, req)
}

func validateMutationDocs(kbID string, docCount int) (bool, error) {
	if kbID == "" {
		return false, fmt.Errorf(errEmptyKBID)
	}
	return docCount == 0, nil
}

func (f *DuckDBArtifactFormat) openPreparedMutationDB(
	ctx context.Context,
	kbID string,
	preparedDocs []preparedUpsertDoc,
	firstDocID string,
	deferred bool,
) (*sql.DB, error) {
	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	seedVec := preparedDocs[0].Embedding
	if len(seedVec) == 0 {
		return nil, fmt.Errorf("embed doc %q for shard bootstrap: empty embedding", firstDocID)
	}
	if err := f.ensureMutableShardDBLocked(ctx, mutableShardRequest{kbID: kbID, kbDir: kbDir, dbPath: dbPath, embeddingDim: len(seedVec), allowBootstrap: true, joinsSession: deferred}); err != nil {
		return nil, err
	}
	return f.openConfiguredDB(ctx, dbPath)
}

func (f *DuckDBArtifactFormat) graphBuilderForUpsert(opts kb.UpsertDocsOptions) (*kb.GraphBuilder, error) {
	graphBuilder := f.deps.GraphBuilder()
	if opts.GraphEnabled == nil {
		return graphBuilder, nil
	}
	if !*opts.GraphEnabled {
		return nil, nil
	}
	if graphBuilder == nil {
		return nil, kb.ErrGraphUnavailable
	}
	return graphBuilder, nil
}

func (f *DuckDBArtifactFormat) cleanupUploadedPreShardSnapshot(ctx context.Context, req kb.IngestUpsertRequest) error {
	if !req.Upload {
		return nil
	}
	return f.cleanupPreShardSnapshotObjectsBestEffort(ctx, req.KBID)
}

func (f *DuckDBArtifactFormat) Delete(ctx context.Context, req kb.IngestDeleteRequest) (kb.IngestResult, error) {
	if req.KBID == "" {
		return kb.IngestResult{}, fmt.Errorf(errEmptyKBID)
	}
	if len(req.DocIDs) == 0 {
		return kb.IngestResult{}, nil
	}

	cleanIDs := make([]string, 0, len(req.DocIDs))
	for _, id := range req.DocIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return kb.IngestResult{}, fmt.Errorf("doc id cannot be empty")
		}
		cleanIDs = append(cleanIDs, trimmed)
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, req.KBID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	if err := f.ensureMutableShardDBLocked(ctx, mutableShardRequest{kbID: req.KBID, kbDir: kbDir, dbPath: dbPath, joinsSession: req.Options.DeferPublish}); err != nil {
		// The orphans this caller wants gone cannot exist.
		if errors.Is(err, kb.ErrKBUninitialized) {
			return kb.IngestResult{}, nil
		}
		return kb.IngestResult{}, err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return kb.IngestResult{}, err
	}
	defer db.Close()

	if err := f.applyDelete(ctx, db, cleanIDs, req.Options); err != nil {
		return kb.IngestResult{}, logDuckDBMemoryOnError(ctx, db, "delete", err)
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	commit := mutationCommitOptions{
		kbID: req.KBID, upload: req.Upload, deferred: req.Options.DeferPublish,
		targetShardBytes: policy.TargetShardBytes, maxAttempts: 1,
	}
	if err := f.postMutationCommitWithRetry(ctx, db, commit); err != nil {
		return kb.IngestResult{}, err
	}

	return kb.IngestResult{MutatedCount: len(cleanIDs)}, nil
}

func (f *DuckDBArtifactFormat) PublishPrepared(
	ctx context.Context,
	req kb.PreparedPublishRequest,
) (kb.IngestResult, error) {
	if empty, err := validateMutationDocs(req.KBID, len(req.Docs)); err != nil || empty {
		return kb.IngestResult{}, err
	}

	preparedDocs, err := embeddedToPreparedDocs(req.Docs)
	if err != nil {
		return kb.IngestResult{}, err
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	db, err := f.openPreparedMutationDB(ctx, req.KBID, preparedDocs, req.Docs[0].ID, req.Options.DeferPublish)
	if err != nil {
		return kb.IngestResult{}, err
	}
	defer db.Close()
	expectedDim, err := duckDBEmbeddingDimension(ctx, db)
	if err != nil {
		return kb.IngestResult{}, err
	}
	if err := validatePreparedDocDimensions(preparedDocs, expectedDim, "prepared embedding dimension is incompatible with stored vectors"); err != nil {
		return kb.IngestResult{}, err
	}

	if err := f.applyPreparedUpsert(ctx, db, preparedDocs, req.GraphResult); err != nil {
		return kb.IngestResult{}, logDuckDBMemoryOnError(ctx, db, "prepared_upsert", err)
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommitWithRetry(ctx, db, mutationCommitOptions{kbID: req.KBID, upload: req.Upload, deferred: req.Options.DeferPublish, targetShardBytes: policy.TargetShardBytes, maxAttempts: 5}); err != nil {
		return kb.IngestResult{}, err
	}
	if err := f.cleanupPreparedPublishUpload(ctx, req); err != nil {
		return kb.IngestResult{}, err
	}
	return kb.IngestResult{MutatedCount: len(req.Docs)}, nil
}

func (f *DuckDBArtifactFormat) cleanupPreparedPublishUpload(ctx context.Context, req kb.PreparedPublishRequest) error {
	if !req.Upload {
		return nil
	}
	return f.cleanupPreShardSnapshotObjectsBestEffort(ctx, req.KBID)
}

func (f *DuckDBArtifactFormat) PublishPreparedStream(
	ctx context.Context,
	req kb.PreparedStreamRequest,
) (kb.IngestResult, error) {
	if err := validatePreparedStreamRequest(req); err != nil {
		return kb.IngestResult{}, err
	}
	kbDir := filepath.Join(f.deps.CacheDir, req.KBID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	mutated, err := f.applyPreparedStream(ctx, req, kbDir, dbPath)
	if err != nil {
		return kb.IngestResult{}, err
	}
	if err := f.commitPreparedStream(ctx, req.KBID, dbPath, req.Options); err != nil {
		return kb.IngestResult{}, err
	}
	return kb.IngestResult{MutatedCount: mutated}, nil
}

func validatePreparedStreamRequest(req kb.PreparedStreamRequest) error {
	if req.KBID == "" {
		return fmt.Errorf(errEmptyKBID)
	}
	if req.Next == nil {
		return fmt.Errorf("prepared stream next cannot be nil")
	}
	return nil
}

func (f *DuckDBArtifactFormat) applyPreparedStream(
	ctx context.Context,
	req kb.PreparedStreamRequest,
	kbDir string,
	dbPath string,
) (int, error) {
	mutated := 0
	for {
		docs, err := req.Next(ctx)
		if err != nil || len(docs) == 0 {
			return mutated, err
		}
		prepared, err := embeddedToPreparedDocs(docs)
		if err != nil {
			return mutated, err
		}
		if err := ensurePreparedSeedVector(prepared, docs); err != nil {
			return mutated, err
		}
		if err := f.applyPreparedStreamBatch(ctx, preparedStreamBatch{kbID: req.KBID, kbDir: kbDir, dbPath: dbPath, prepared: prepared, mutatedBefore: mutated, deferred: req.Options.DeferPublish}); err != nil {
			return mutated, err
		}
		mutated += len(prepared)
	}
}

func ensurePreparedSeedVector(prepared []preparedUpsertDoc, docs []kb.EmbeddedDocument) error {
	if len(prepared) == 0 || len(prepared[0].Embedding) > 0 {
		return nil
	}
	return fmt.Errorf("embed doc %q for shard bootstrap: empty embedding", docs[0].ID)
}

type preparedStreamBatch struct {
	kbID          string
	kbDir         string
	dbPath        string
	prepared      []preparedUpsertDoc
	mutatedBefore int
	deferred      bool
}

func (f *DuckDBArtifactFormat) applyPreparedStreamBatch(ctx context.Context, batch preparedStreamBatch) error {
	lock := f.lockFor(batch.kbID)
	lock.Lock()
	defer lock.Unlock()
	if err := f.ensureMutableShardDBLocked(ctx, mutableShardRequest{kbID: batch.kbID, kbDir: batch.kbDir, dbPath: batch.dbPath, embeddingDim: len(batch.prepared[0].Embedding), allowBootstrap: true, joinsSession: batch.deferred}); err != nil {
		return err
	}
	// Per batch, because the lock is released between batches and eviction
	// would reclaim anything not yet marked.
	if batch.deferred {
		if err := kb.MarkPendingSession(batch.kbDir); err != nil {
			return err
		}
	}
	db, err := f.openConfiguredDB(ctx, batch.dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := f.validatePreparedBatchDimensions(ctx, db, batch.prepared); err != nil {
		return err
	}
	writeStarted := time.Now()
	if err := f.applyPreparedUpsert(ctx, db, batch.prepared, nil); err != nil {
		return logDuckDBMemoryOnError(ctx, db, "prepared_stream_upsert", err)
	}
	slog.Default().
		InfoContext(ctx, "code index write batch complete", logKeyKBID, batch.kbID, "chunks", len(batch.prepared), "total_chunks", batch.mutatedBefore+len(batch.prepared), "duration_ms", time.Since(writeStarted).Milliseconds())
	return nil
}

func (f *DuckDBArtifactFormat) validatePreparedBatchDimensions(
	ctx context.Context,
	db *sql.DB,
	prepared []preparedUpsertDoc,
) error {
	expectedDim, err := duckDBEmbeddingDimension(ctx, db)
	if err != nil {
		return err
	}
	return validatePreparedDocDimensions(
		prepared,
		expectedDim,
		"prepared embedding dimension is incompatible with stored vectors",
	)
}

func (f *DuckDBArtifactFormat) commitPreparedStream(
	ctx context.Context,
	kbID string,
	dbPath string,
	opts kb.UpsertDocsOptions,
) error {
	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()
	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	commit := mutationCommitOptions{
		kbID: kbID, upload: !opts.DeferPublish, deferred: opts.DeferPublish,
		targetShardBytes: policy.TargetShardBytes, maxAttempts: 5,
	}
	if err := f.postMutationCommitWithRetry(ctx, db, commit); err != nil {
		return err
	}
	if opts.DeferPublish {
		return nil
	}
	return f.cleanupPreShardSnapshotObjectsBestEffort(ctx, kbID)
}

func (f *DuckDBArtifactFormat) CommitPrepared(ctx context.Context, kbID string) error {
	if kbID == "" {
		return fmt.Errorf(errEmptyKBID)
	}
	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	// Republishing unchanged rows costs a full rebuild and churns every reader.
	if !kb.HasPendingSession(kbDir) {
		return nil
	}
	if _, err := os.Stat(dbPath); err != nil {
		return err
	}
	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	// Only an explicit commit ends the session.
	commit := mutationCommitOptions{
		kbID: kbID, upload: true, endsSession: true,
		targetShardBytes: policy.TargetShardBytes, maxAttempts: 5,
	}
	if err := f.postMutationCommitWithRetry(ctx, db, commit); err != nil {
		return err
	}
	return f.cleanupPreShardSnapshotObjectsBestEffort(ctx, kbID)
}

// PrepareAndOpenDB prepares the working state and opens the DB for direct access.
func (f *DuckDBArtifactFormat) PrepareAndOpenDB(ctx context.Context, kbID string) (*sql.DB, error) {
	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	// Not exempt: writes here would ride out under another client's commit.
	if err := f.ensureMutableShardDBLocked(ctx, mutableShardRequest{kbID: kbID, kbDir: kbDir, dbPath: dbPath}); err != nil {
		return nil, err
	}
	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return nil, err
	}
	return f.openConfiguredDB(ctx, dbPath)
}

type mutableShardRequest struct {
	kbID           string
	kbDir          string
	dbPath         string
	embeddingDim   int
	allowBootstrap bool
	// joinsSession marks a write that belongs to the open session. Anything
	// else is refused while rows are pending.
	joinsSession bool
}

func (f *DuckDBArtifactFormat) ensureMutableShardDBLocked(ctx context.Context, req mutableShardRequest) error {
	if strings.TrimSpace(req.kbID) == "" {
		return fmt.Errorf(errEmptyKBID)
	}
	// An open session does not move the manifest, so the staleness check below
	// waves this through and publishes rows the client has not committed.
	if !req.joinsSession && kb.HasPendingSession(req.kbDir) {
		return kb.UnpublishedWritesError(req.kbID)
	}

	manifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, req.kbID)
	if err != nil {
		return err
	}
	if manifestVersion == "" {
		// Unpublished rows make a knowledge base look absent, not empty.
		if kb.HasPendingSession(req.kbDir) {
			err := f.reuseLocalShardDB(ctx, req)
			if err == nil {
				return nil
			}
			// Reported as absent, a delete answers success for rows it never
			// removed. A writer instead rebuilds, below.
			if !req.allowBootstrap {
				return fmt.Errorf("reuse local shard for %s: %w", req.kbID, err)
			}
		}
		return f.bootstrapMutableShardDB(ctx, req.kbDir, req.dbPath, req.embeddingDim, req.allowBootstrap)
	}
	localVersionPath := localShardManifestVersionPath(req.kbDir)
	fresh, err := localShardDBIsFresh(req.kbDir, req.dbPath, localVersionPath, manifestVersion)
	if err != nil {
		return err
	}
	if fresh {
		return f.ensureDocsMetadataColumn(ctx, req.dbPath)
	}
	if kb.HasPendingSession(req.kbDir) {
		return kb.UnpublishedWritesError(req.kbID)
	}
	// An emptied knowledge base has nothing to reconstruct from, and refusing
	// would wedge every later writer on a cold cache.
	emptied, err := f.manifestHasNoShards(ctx, req.kbID)
	if err != nil {
		return err
	}
	if emptied {
		return f.bootstrapMutableShardDB(ctx, req.kbDir, req.dbPath, req.embeddingDim, req.allowBootstrap)
	}
	return f.refreshMutableShardDB(
		ctx,
		mutableShardRefresh{
			kbID:             req.kbID,
			kbDir:            req.kbDir,
			dbPath:           req.dbPath,
			localVersionPath: localVersionPath,
			manifestVersion:  manifestVersion,
		},
	)
}

// manifestHasNoShards reports a published knowledge base holding nothing.
func (f *DuckDBArtifactFormat) manifestHasNoShards(ctx context.Context, kbID string) (bool, error) {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return false, nil
		}
		return false, err
	}
	return len(doc.Manifest.Shards) == 0, nil
}

// reuseLocalShardDB adopts a shard holding rows that were never published.
// Only a writer carries a dimension, so only a writer can repair the schema.
func (f *DuckDBArtifactFormat) reuseLocalShardDB(ctx context.Context, req mutableShardRequest) error {
	if _, err := os.Stat(req.dbPath); err != nil {
		return err
	}
	if req.embeddingDim > 0 {
		if err := f.createEmptyMutableShardDBLocked(ctx, req.kbDir, req.dbPath, req.embeddingDim); err != nil {
			return err
		}
	}
	return f.ensureDocsMetadataColumn(ctx, req.dbPath)
}

func (f *DuckDBArtifactFormat) bootstrapMutableShardDB(
	ctx context.Context,
	kbDir string,
	dbPath string,
	embeddingDim int,
	allowBootstrap bool,
) error {
	if !allowBootstrap {
		return kb.ErrKBUninitialized
	}
	if embeddingDim <= 0 {
		return fmt.Errorf("embedding dimension must be > 0")
	}
	return f.createEmptyMutableShardDBLocked(ctx, kbDir, dbPath, embeddingDim)
}

func localShardDBIsFresh(kbDir string, dbPath string, localVersionPath string, manifestVersion string) (bool, error) {
	localVersion, readErr := readLocalShardManifestVersion(localVersionPath)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return false, readErr
	}
	if readErr != nil || localVersion != manifestVersion {
		return false, nil
	}
	if _, statErr := os.Stat(dbPath); statErr == nil {
		return true, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return false, statErr
	}
	return false, nil
}

type mutableShardRefresh struct {
	kbID             string
	kbDir            string
	dbPath           string
	localVersionPath string
	manifestVersion  string
}

func (f *DuckDBArtifactFormat) refreshMutableShardDB(ctx context.Context, req mutableShardRefresh) error {
	if err := os.MkdirAll(req.kbDir, 0o755); err != nil {
		return err
	}
	if _, err := f.downloadSnapshotFromShards(ctx, req.kbID, req.dbPath); err != nil {
		return err
	}
	if err := f.ensureDocsMetadataColumn(ctx, req.dbPath); err != nil {
		return err
	}
	return writeLocalShardManifestVersion(req.localVersionPath, req.manifestVersion)
}

func (f *DuckDBArtifactFormat) createEmptyMutableShardDBLocked(
	ctx context.Context,
	kbDir, dbPath string,
	embeddingDim int,
) error {
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
			embedding FLOAT[` + strconv.Itoa(embeddingDim) + `],
			media_refs TEXT,
			metadata TEXT
		)
	`
	if _, err := db.ExecContext(ctx, createDocsSQL); err != nil {
		return closeMutableBootstrapDB(db, fmt.Errorf("create docs table for shard bootstrap: %w", err))
	}
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return closeMutableBootstrapDB(db, err)
	}
	if err := EnsureGraphTables(ctx, db); err != nil {
		return closeMutableBootstrapDB(db, err)
	}

	if err := CheckpointAndCloseDB(ctx, db, "close db after shard bootstrap"); err != nil {
		return err
	}
	return nil
}

func (f *DuckDBArtifactFormat) ensureDocsMetadataColumn(ctx context.Context, dbPath string) error {
	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	hasMetadata, err := docsColumnExists(ctx, db, "metadata")
	if err != nil {
		return err
	}
	if hasMetadata {
		return nil
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE docs ADD COLUMN metadata TEXT`); err != nil {
		return fmt.Errorf("add docs.metadata column: %w", err)
	}
	if err := CheckpointDB(ctx, db); err != nil {
		return err
	}
	return nil
}

type mutationCommitOptions struct {
	kbID string
	// deferred opens a session; merely skipping the upload does not.
	upload           bool
	deferred         bool
	endsSession      bool
	targetShardBytes int64
	maxAttempts      int
}

func (f *DuckDBArtifactFormat) postMutationCommitWithRetry(
	ctx context.Context,
	db *sql.DB,
	opts mutationCommitOptions,
) error {
	kbDir := filepath.Join(f.deps.CacheDir, opts.kbID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)

	// Before the checkpoint, so a crash cannot leave rows unmarked.
	if opts.deferred {
		if err := kb.MarkPendingSession(kbDir); err != nil {
			return err
		}
	}

	if err := checkpointAndCloseMutationDB(ctx, db); err != nil {
		return err
	}

	if opts.upload {
		if err := f.publishMutationSnapshotWithBackoff(ctx, mutationPublishRequest{kbID: opts.kbID, kbDir: kbDir, dbPath: dbPath, targetShardBytes: opts.targetShardBytes, maxAttempts: opts.maxAttempts}); err != nil {
			return err
		}
		// Before the housekeeping below, whose failure would otherwise leave
		// the marker set and the cache unreclaimable.
		if opts.endsSession {
			if err := kb.ClearPendingSession(kbDir); err != nil {
				return err
			}
		}
	}

	if err := f.deps.EvictCacheIfNeeded(ctx, opts.kbID); err != nil {
		if !opts.upload {
			return err
		}
		// Cache cleanup cannot invalidate a durable publish.
		slog.Default().WarnContext(ctx, "post-publish cache eviction failed",
			logKeyKBID, opts.kbID, logKeyError, err)
	}

	return nil
}

func closeMutableBootstrapDB(db *sql.DB, cause error) error {
	if err := db.Close(); err != nil {
		return fmt.Errorf("%w; close mutable bootstrap db: %v", cause, err)
	}
	return cause
}

func checkpointAndCloseMutationDB(ctx context.Context, db *sql.DB) error {
	dbClosed := false
	defer func() {
		if !dbClosed {
			if err := db.Close(); err != nil {
				slog.Default().Warn("close mutation db after failed checkpoint failed", logKeyError, err)
			}
		}
	}()
	if err := CheckpointDB(ctx, db); err != nil {
		return logDuckDBMemoryOnError(ctx, db, "checkpoint", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db after mutation: %w", err)
	}
	dbClosed = true
	return nil
}

type mutationPublishRequest struct {
	kbID             string
	kbDir            string
	dbPath           string
	targetShardBytes int64
	maxAttempts      int
}

func (f *DuckDBArtifactFormat) publishMutationSnapshotWithBackoff(
	ctx context.Context,
	req mutationPublishRequest,
) error {
	if req.maxAttempts <= 0 {
		req.maxAttempts = 1
	}
	state := mutationPublishRetry{maxAttempts: req.maxAttempts}
	var lastErr error
	for attempt := 1; attempt <= state.maxAttempts; attempt++ {
		newVersion, artifactCount, err := f.publishMutationSnapshotAttempt(
			ctx,
			req.kbID,
			req.dbPath,
			req.targetShardBytes,
		)
		if err != nil {
			lastErr = err
			if state.retry(ctx, req.kbID, attempt, err) {
				continue
			}
			return err
		}
		f.deps.Metrics.RecordShardCount(req.kbID, artifactCount)
		return writeLocalShardManifestVersion(localShardManifestVersionPath(req.kbDir), newVersion)
	}
	// Reporting success here would clear the marker with nothing published.
	return lastErr
}

type mutationPublishRetry struct{ maxAttempts int }

const (
	transientPublishMaxAttempts = 5
	transientPublishBaseDelay   = 100 * time.Millisecond
	transientPublishCapDelay    = 2 * time.Second
)

func (s *mutationPublishRetry) retry(ctx context.Context, kbID string, attempt int, err error) bool {
	shouldRetry := errors.Is(err, kb.ErrBlobVersionMismatch) || isTransientBlobError(err)
	if !shouldRetry {
		return false
	}
	if attempt >= s.maxAttempts && attempt >= transientPublishMaxAttempts {
		logOrphanedShardBlobs(ctx, kbID, attempt, err, nil)
		return false
	}
	if isTransientBlobError(err) && attempt >= s.maxAttempts && attempt < transientPublishMaxAttempts {
		s.maxAttempts = transientPublishMaxAttempts
	}
	sleepBackoff(attempt, transientPublishBaseDelay, transientPublishCapDelay)
	return true
}

func (f *DuckDBArtifactFormat) publishMutationSnapshotAttempt(
	ctx context.Context,
	kbID, dbPath string,
	targetShardBytes int64,
) (string, int, error) {
	expectedManifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, kbID)
	if err != nil {
		return "", 0, err
	}
	artifacts, err := f.BuildArtifacts(ctx, kbID, dbPath, targetShardBytes)
	if err != nil {
		return "", 0, err
	}
	if err := f.auditStagedShardBlobs(ctx, artifacts); err != nil {
		return "", 0, err
	}
	newVersion, err := f.publishMutationManifest(ctx, kbID, artifacts, expectedManifestVersion)
	if err != nil {
		return "", 0, err
	}
	if f.deps.ReconcileShardBlobs != nil {
		if gcErr := f.deps.ReconcileShardBlobs(ctx, kbID, artifacts); gcErr != nil {
			slog.Default().WarnContext(ctx, "orphaned shard reconcile failed",
				logKeyKBID, kbID, logKeyError, gcErr)
		}
	}
	return newVersion, len(artifacts), nil
}

func (f *DuckDBArtifactFormat) auditStagedShardBlobs(ctx context.Context, artifacts []kb.SnapshotShardMetadata) error {
	for _, a := range artifacts {
		if _, err := f.deps.BlobStore.Head(ctx, a.Key); err != nil {
			return fmt.Errorf("audit staged artifact %s: %w", a.Key, err)
		}
	}
	return nil
}

func (f *DuckDBArtifactFormat) publishMutationManifest(
	ctx context.Context,
	kbID string,
	artifacts []kb.SnapshotShardMetadata,
	expectedManifestVersion string,
) (string, error) {
	totalSize := int64(0)
	for _, a := range artifacts {
		totalSize += a.SizeBytes
	}
	manifest := kb.SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         kb.ShardManifestLayoutDuckDBs,
		FormatKind:     DuckDBFormatKind,
		FormatVersion:  DuckDBFormatVersion,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: totalSize,
		Shards:         artifacts,
	}
	return f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
}

func logOrphanedShardBlobs(ctx context.Context, kbID string, attempt int, err error, orphanedKeys []string) {
	slog.Default().WarnContext(ctx, "manifest publish failed; orphaned shard blobs may exist",
		logKeyKBID, kbID,
		"attempts", attempt,
		logKeyError, err,
		"orphaned_keys", orphanedKeys,
	)
}

// isTransientBlobError reports whether err looks like a retriable
// network/blob error. context.DeadlineExceeded is deliberately excluded.
// that's a caller-imposed timeout, and retrying inside the same context
// just wastes work.
func isTransientBlobError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	transientMarkers := []string{
		"connection reset",
		"i/o timeout",
		"temporary failure",
		"connection refused",
		"no such host",
		"broken pipe",
		"eof",
		"unexpected eof",
	}
	for _, marker := range transientMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func sleepBackoff(attempt int, base, cap time.Duration) {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cap {
			d = cap
			break
		}
	}
	time.Sleep(d)
}

type preparedUpsertDoc struct {
	Doc       kb.Document
	Embedding []float32
}

func embeddedToPreparedDocs(docs []kb.EmbeddedDocument) ([]preparedUpsertDoc, error) {
	prepared := make([]preparedUpsertDoc, 0, len(docs))
	for _, doc := range docs {
		if strings.TrimSpace(doc.ID) == "" {
			return nil, fmt.Errorf("doc id cannot be empty")
		}
		if strings.TrimSpace(doc.Text) == "" {
			return nil, fmt.Errorf("doc %q text cannot be empty", doc.ID)
		}
		if len(doc.Embedding) == 0 {
			return nil, fmt.Errorf("doc %q embedding cannot be empty", doc.ID)
		}
		prepared = append(prepared, preparedUpsertDoc{
			Doc: kb.Document{
				ID:        doc.ID,
				Text:      doc.Text,
				MediaIDs:  doc.MediaIDs,
				MediaRefs: doc.MediaRefs,
				Metadata:  doc.Metadata,
			},
			Embedding: doc.Embedding,
		})
	}
	return prepared, nil
}

func (f *DuckDBArtifactFormat) prepareDocsForUpsert(
	ctx context.Context,
	docs []kb.Document,
) ([]preparedUpsertDoc, error) {
	prepared := make([]preparedUpsertDoc, 0, len(docs))
	needed := make([]int, 0, len(docs))
	for i, doc := range docs {
		if strings.TrimSpace(doc.ID) == "" {
			return nil, fmt.Errorf("doc id cannot be empty")
		}
		if len(doc.Embedding) == 0 {
			if strings.TrimSpace(doc.Text) == "" {
				return nil, fmt.Errorf("doc %q: text or embedding is required", doc.ID)
			}
			needed = append(needed, i)
		}
		prepared = append(prepared, preparedUpsertDoc{Doc: doc, Embedding: doc.Embedding})
	}

	vectors, err := f.embedTexts(ctx, docs, needed)
	if err != nil {
		return nil, err
	}
	for n, i := range needed {
		if len(vectors[n]) == 0 {
			return nil, fmt.Errorf("embed doc %q: empty embedding", docs[i].ID)
		}
		prepared[i].Embedding = vectors[n]
	}
	return prepared, nil
}

// One request is bounded by both: some compatible endpoints cap inputs well
// below 32, and a large batch is what pushes a request past its timeout.
const (
	upsertEmbedBatchSize  = 32
	upsertEmbedBatchBytes = 256 << 10
)

// embedTexts embeds the documents at idx. Batches run concurrently: each writes
// its own span of out, so only the embedder's own limits are at stake.
func (f *DuckDBArtifactFormat) embedTexts(
	ctx context.Context,
	docs []kb.Document,
	idx []int,
) ([][]float32, error) {
	out := make([][]float32, len(idx))
	if len(idx) == 0 {
		return out, nil
	}
	if f.deps.EmbedBatch == nil {
		for n, i := range idx {
			vec, err := f.deps.Embed(ctx, docs[i].Text)
			if err != nil {
				return nil, fmt.Errorf("embed doc %q: %w", docs[i].ID, err)
			}
			out[n] = vec
		}
		return out, nil
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(f.embedParallelism())
	for start := 0; start < len(idx) && groupCtx.Err() == nil; {
		// Fixed here: the loop advances start before the goroutine reads it.
		from, to := start, batchEnd(docs, idx, start)
		group.Go(func() error {
			release, err := f.budget().AcquireEmbed(groupCtx)
			if err != nil {
				return err
			}
			defer release()
			inputs := make([]string, 0, to-from)
			for _, i := range idx[from:to] {
				inputs = append(inputs, docs[i].Text)
			}
			vectors, err := f.deps.EmbedBatch(groupCtx, inputs)
			if err != nil {
				return fmt.Errorf("embed docs %q..%q: %w", docs[idx[from]].ID, docs[idx[to-1]].ID, err)
			}
			if len(vectors) != len(inputs) {
				return fmt.Errorf("batch embed returned %d vectors for %d documents", len(vectors), len(inputs))
			}
			copy(out[from:to], vectors)
			return nil
		})
		start = to
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	// The loop stops scheduling on cancellation, so an early exit can leave out
	// half filled with a nil error behind it.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// batchEnd stops at whichever limit comes first, always taking one document so
// a single oversized one cannot stall the loop.
func batchEnd(docs []kb.Document, idx []int, start int) int {
	total := 0
	for end := start; end < len(idx); end++ {
		size := len(docs[idx[end]].Text)
		if end > start && (end-start >= upsertEmbedBatchSize || total+size > upsertEmbedBatchBytes) {
			return end
		}
		total += size
	}
	return len(idx)
}

func (f *DuckDBArtifactFormat) applyUpsert(
	ctx context.Context,
	db *sql.DB,
	docs []preparedUpsertDoc,
	graphBuilder *kb.GraphBuilder,
) error {

	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	var graphResult *kb.GraphBuildResult
	var err error
	if graphBuilder != nil {
		graphDocs := make([]kb.Document, 0, len(docs))
		for _, prepared := range docs {
			graphDocs = append(graphDocs, prepared.Doc)
		}
		graphResult, err = graphBuilder.Build(ctx, graphDocs)
		if err != nil {
			return fmt.Errorf("build graph for upsert docs: %w", err)
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}

	return applyPreparedDocsAndGraphAppender(ctx, db, docs, graphResult, graphResult != nil)
}

func (f *DuckDBArtifactFormat) applyPreparedUpsert(
	ctx context.Context,
	db *sql.DB,
	docs []preparedUpsertDoc,
	graphResult *kb.GraphBuildResult,
) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}
	if graphResult == nil {
		return applyPreparedDocsAppender(ctx, db, docs)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}
	return applyPreparedDocsTx(ctx, tx, docs, graphResult, graphResult != nil)
}

func ensureGraphTablesForTx(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS entities (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE IF NOT EXISTS edges (src TEXT, dst TEXT, weight DOUBLE, rel_type TEXT, chunk_id TEXT)`,
		`CREATE TABLE IF NOT EXISTS doc_entities (doc_id TEXT, entity_id TEXT, weight DOUBLE, chunk_id TEXT)`,
		`CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src)`,
		`CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges(dst)`,
		`CREATE INDEX IF NOT EXISTS idx_doc_entities_doc_id ON doc_entities(doc_id)`,
		`CREATE INDEX IF NOT EXISTS idx_doc_entities_entity_id ON doc_entities(entity_id)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure graph tables: %w", err)
		}
	}
	return nil
}

func applyDocDeleteTx(ctx context.Context, tx *sql.Tx, cleanIDs []string, hardDelete bool) error {
	if hardDelete {
		return applyHardDocDeleteTx(ctx, tx, cleanIDs)
	}
	return applySoftDocDeleteTx(ctx, tx, cleanIDs)
}

func applyHardDocDeleteTx(ctx context.Context, tx *sql.Tx, cleanIDs []string) error {
	placeholders := kb.BuildInClausePlaceholders(len(cleanIDs))
	args := stringArgs(cleanIDs)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM docs WHERE id IN (%s)`, placeholders), args...); err != nil {
		tx.Rollback()
		return fmt.Errorf("hard delete docs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM doc_tombstones WHERE doc_id IN (%s)`, placeholders), args...); err != nil {
		tx.Rollback()
		return fmt.Errorf("remove tombstones: %w", err)
	}
	return nil
}

func applySoftDocDeleteTx(ctx context.Context, tx *sql.Tx, cleanIDs []string) error {
	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO doc_tombstones (doc_id, deleted_at) VALUES (?, NOW())`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare tombstone upsert: %w", err)
	}
	defer stmt.Close()
	for _, id := range cleanIDs {
		if _, err := stmt.ExecContext(ctx, id); err != nil {
			tx.Rollback()
			return fmt.Errorf("soft delete doc %q: %w", id, err)
		}
	}
	return nil
}

func stringArgs(values []string) []any {
	args := make([]any, 0, len(values))
	for _, value := range values {
		args = append(args, value)
	}
	return args
}

func (f *DuckDBArtifactFormat) applyDelete(
	ctx context.Context,
	db *sql.DB,
	cleanIDs []string,
	opts kb.DeleteDocsOptions,
) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete docs tx: %w", err)
	}

	if err := applyDocDeleteTx(ctx, tx, cleanIDs, opts.HardDelete); err != nil {
		return err
	}

	if err := graph.PruneForDocsTx(ctx, tx, cleanIDs, opts.HardDelete && opts.CleanupGraph); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete docs tx: %w", err)
	}

	return nil
}
