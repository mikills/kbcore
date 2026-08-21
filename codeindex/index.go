package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	minnowcode "github.com/mikills/minnow/codeindex/indexer"
)

type indexResult struct {
	KBID           string `json:"kb_id"`
	IndexKey       string `json:"index_key"`
	Description    string `json:"description"`
	Ref            string `json:"ref,omitempty"`
	Root           string `json:"root"`
	ScannedFiles   int    `json:"scanned_files"`
	SkippedFiles   int    `json:"skipped_files"`
	IndexedFiles   int    `json:"indexed_files"`
	DeletedFiles   int    `json:"deleted_files"`
	UnchangedFiles int    `json:"unchanged_files"`
	ChunksIndexed  int    `json:"chunks_indexed"`
	ChunksDeleted  int    `json:"chunks_deleted"`
	StatePath      string `json:"state_path"`
}

func refreshIndex(ctx context.Context, cfg Config, cli indexCLIOptions) (indexResult, error) {
	target, err := resolveTarget(cli)
	if err != nil {
		return indexResult{}, err
	}
	operationTTL, err := cfg.operationTimeout()
	if err != nil {
		return indexResult{}, err
	}
	staleAfter := operationTTL + time.Minute
	target, previous, statePath, releaseLock, err := prepareRefreshTarget(target, staleAfter)
	if err != nil {
		return indexResult{}, err
	}
	defer releaseLock()
	opts, policy, err := indexOptions(cfg, cli, target)
	if err != nil {
		return indexResult{}, err
	}
	if err := policy.Check(ctx); err != nil {
		return indexResult{}, err
	}
	progress := newProgressReporter(cli.quiet)
	files, skipped, err := minnowcode.Scan(ctx, target.Root, opts, minnowcode.DefaultExcludePatterns)
	if err != nil {
		return indexResult{}, err
	}
	progress.scanned(len(files), skipped)
	if err := minnowcode.ValidateConfirmation(opts, len(files)); err != nil {
		return indexResult{}, err
	}
	// Not when the id is minted, since a run that stops at the prompt has
	// uploaded nothing to pin.
	if target.MintedKBID {
		saveReservedKBID(target)
	}
	own := journalRecovery{
		ownJournal: uploadJournalPath(target), statePath: statePath, staleAfter: staleAfter,
	}
	client, journal, invalidated, err := startUpload(ctx, cfg, target, own, progress)
	if err != nil {
		return indexResult{}, err
	}
	defer journal.close()
	// Recovery just deleted their chunks. The copy read before that still
	// calls them unchanged, which would leave them out of the index.
	forgetFiles(previous, invalidated)
	sink := &documentSink{
		client: client, kbID: target.KBID, policy: policy, journal: journal, progress: progress,
	}
	emit := func(ctx context.Context, docs []minnowcode.Document) error {
		progress.fileChunked(len(docs))
		return sink.emit(ctx, docs)
	}
	pipeline := pipelineFingerprint(opts)
	plan, err := buildIndexPlan(ctx, target, opts, pipeline, previous, files, skipped, emit)
	if err != nil {
		return indexResult{}, err
	}
	if err := sink.close(ctx); err != nil {
		return indexResult{}, err
	}
	if err := journal.recordStale(plan.stalePaths); err != nil {
		return indexResult{}, err
	}
	if err := sendDeletes(ctx, client, target.KBID, plan.deleteIDs); err != nil {
		return indexResult{}, err
	}
	// State must not record success before the deferred writes are published.
	if err := client.commit(ctx, target.KBID); err != nil {
		return indexResult{}, err
	}
	clearSession(target)
	plan.state.UpdatedAt = time.Now().UTC()
	savedPath, err := saveIndexState(target, plan.state)
	if err != nil {
		return indexResult{}, err
	}
	clearReservedKBID(target)
	if err := saveRegistrySelection(target, opts); err != nil {
		return indexResult{}, err
	}
	// State now records every emitted chunk, so a leftover journal is orphan-free.
	_ = journal.remove()
	plan.result.StatePath = savedPath
	progress.done(plan.result)
	return plan.result, nil
}

func startUpload(
	ctx context.Context,
	cfg Config,
	target indexTarget,
	own journalRecovery,
	progress *progressReporter,
) (*minnowClient, *uploadJournal, []string, error) {
	client, err := newMinnowClient(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := client.check(ctx); err != nil {
		return nil, nil, nil, fmt.Errorf("connect to Minnow at %s: %w", cfg.Minnow.URL, err)
	}
	// Publishing per batch is slower but never strands writes.
	if client.canDeferPublish {
		client.sessionKB = target.KBID
		client.sessionID = loadSession(target)
		client.onSession = func(id string) { saveSession(target, id) }
		client.onWait = progress.waitingForSession
	}
	journal, invalidated, err := startUploadJournal(ctx, client, target, own, progress)
	if err != nil {
		return nil, nil, nil, err
	}
	return client, journal, invalidated, nil
}

func forgetFiles(state indexState, paths []string) {
	for _, path := range paths {
		delete(state.Files, path)
	}
}

func prepareRefreshTarget(
	target indexTarget,
	staleAfter time.Duration,
) (indexTarget, indexState, string, func(), error) {
	releaseLock, err := acquireRefreshLocks(target, staleAfter)
	if err != nil {
		return indexTarget{}, indexState{}, "", nil, err
	}
	prepared := false
	defer func() {
		if !prepared {
			releaseLock()
		}
	}()
	if target.Git {
		if err := minnowcode.EnsureLocalStateIgnored(target.StateRoot); err != nil {
			return indexTarget{}, indexState{}, "", nil, err
		}
	}
	previous, statePath, stateExists, err := loadIndexState(target)
	if err != nil {
		return indexTarget{}, indexState{}, "", nil, err
	}
	target, err = assignIndexGeneration(target, previous, stateExists)
	if err != nil {
		return indexTarget{}, indexState{}, "", nil, err
	}
	prepared = true
	return target, previous, statePath, releaseLock, nil
}

func acquireRefreshLocks(target indexTarget, staleAfter time.Duration) (func(), error) {
	releaseCurrent, err := acquireIndexLock(target, staleAfter)
	if err != nil {
		return nil, err
	}
	if target.LegacyIndexKey == "" {
		return releaseCurrent, nil
	}
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	if indexStatePath(legacyTarget) == indexStatePath(target) {
		return releaseCurrent, nil
	}
	releaseLegacy, err := acquireIndexLock(legacyTarget, staleAfter)
	if err != nil {
		releaseCurrent()
		return nil, fmt.Errorf("acquire legacy index lock: %w", err)
	}
	return func() {
		releaseLegacy()
		releaseCurrent()
	}, nil
}

type indexPlan struct {
	state      indexState
	deleteIDs  []string
	stalePaths []string
	result     indexResult
}

func buildIndexPlan(
	ctx context.Context,
	target indexTarget,
	opts minnowcode.Options,
	pipeline string,
	previous indexState,
	files []minnowcode.ScannedFile,
	skipped int,
	emit emitFunc,
) (indexPlan, error) {
	plan := indexPlan{
		state: indexState{
			SourcePath:    previous.SourcePath,
			SchemaVersion: indexStateSchema, KBID: target.KBID, RepoID: target.RepoID,
			Ref: target.Ref, Root: target.Root, Pipeline: pipeline, Files: make(map[string]stateFile, len(files)),
		},
		result: indexResult{
			KBID: target.KBID, IndexKey: target.IndexKey, Description: target.Description,
			Ref: target.Ref, Root: target.Root, ScannedFiles: len(files), SkippedFiles: skipped,
		},
	}
	current := make(map[string]minnowcode.ScannedFile, len(files))
	for _, file := range files {
		current[file.RelPath] = file
	}
	for path, old := range previous.Files {
		file, exists := current[path]
		if !exists {
			plan.deleteIDs = append(plan.deleteIDs, old.ChunkIDs...)
			plan.stalePaths = append(plan.stalePaths, path)
			plan.result.DeletedFiles++
			continue
		}
		if previous.Pipeline == pipeline && old.Hash == file.Hash && old.Language == file.Language {
			plan.state.Files[path] = old
			plan.result.UnchangedFiles++
			delete(current, path)
			continue
		}
		plan.deleteIDs = append(plan.deleteIDs, old.ChunkIDs...)
		plan.stalePaths = append(plan.stalePaths, path)
	}
	paths := make([]string, 0, len(current))
	for path := range current {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	newChunkIDs := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		file := current[path]
		docs, _, err := minnowcode.BuildDocuments(ctx, target.Root, target.RepoID, file, opts)
		if err != nil {
			return indexPlan{}, err
		}
		chunkIDs := make([]string, 0, len(docs))
		for _, doc := range docs {
			chunkIDs = append(chunkIDs, doc.ID)
			newChunkIDs[doc.ID] = struct{}{}
		}
		if err := emit(ctx, docs); err != nil {
			return indexPlan{}, err
		}
		plan.state.Files[path] = stateFile{
			Hash: file.Hash, SizeBytes: file.SizeBytes, Language: file.Language, ChunkIDs: chunkIDs,
		}
		plan.result.IndexedFiles++
		plan.result.ChunksIndexed += len(docs)
	}
	filteredDeletes := plan.deleteIDs[:0]
	for _, id := range plan.deleteIDs {
		if _, replaced := newChunkIDs[id]; !replaced {
			filteredDeletes = append(filteredDeletes, id)
		}
	}
	plan.deleteIDs = filteredDeletes
	sort.Strings(plan.deleteIDs)
	plan.result.ChunksDeleted = len(plan.deleteIDs)
	return plan, nil
}

const codeIndexPipelineVersion = "codeindex.pipeline/v2"

func pipelineFingerprint(opts minnowcode.Options) string {
	return pipelineFingerprintForVersion(opts, codeIndexPipelineVersion)
}

func pipelineFingerprintForVersion(opts minnowcode.Options, version string) string {
	data, _ := json.Marshal(struct {
		Version      string   `json:"version"`
		Include      []string `json:"include"`
		Exclude      []string `json:"exclude"`
		MaxFileBytes int64    `json:"max_file_bytes"`
		ChunkSize    int      `json:"chunk_size"`
		Overlap      int      `json:"chunk_overlap"`
	}{version, opts.Include, opts.Exclude, opts.MaxFileBytes, opts.ChunkSize, opts.ChunkOverlap})
	return shortHash(string(data))
}

func indexOptions(cfg Config, cli indexCLIOptions, target indexTarget) (minnowcode.Options, minnowcode.ResourcePolicy, error) {
	requireConfirm := cfg.CodeIndex.RequireConfirm == nil || *cfg.CodeIndex.RequireConfirm
	throttle, err := time.ParseDuration(cfg.CodeIndex.Throttle)
	if err != nil || throttle < 0 {
		return minnowcode.Options{}, minnowcode.ResourcePolicy{}, fmt.Errorf("code_index.throttle must be a non-negative duration")
	}
	opts := minnowcode.Options{
		KBID: target.KBID, IndexKey: target.IndexKey, Description: target.Description, Root: target.Root,
		Include: append([]string(nil), cfg.CodeIndex.Include...), Exclude: append([]string(nil), cfg.CodeIndex.Exclude...),
		MaxFileBytes: cfg.CodeIndex.MaxFileBytes, ChunkSize: cfg.CodeIndex.ChunkSize,
		ChunkOverlap: cfg.CodeIndex.ChunkOverlap, IncludeUntracked: cfg.CodeIndex.IncludeUntracked || cli.includeUntracked,
		EmbedBatchSize: cfg.CodeIndex.RequestBatchSize, MaxBatchBytes: cfg.CodeIndex.MaxBatchBytes,
		Throttle: throttle, MaxHeapBytes: cfg.CodeIndex.MaxHeapBytes, MaxRSSBytes: cfg.CodeIndex.MaxRSSBytes,
		LargeRepoFiles: cfg.CodeIndex.LargeRepoFiles, RequireConfirm: requireConfirm,
		ConfirmedLarge: cli.yes || cli.force,
	}
	applyIndexResourceOverrides(&opts, cli)
	if cli.lowResource {
		applyLowResourceDefaults(&opts, cli)
	}
	opts = minnowcode.NormalizeOptions(opts)
	return opts, minnowcode.ResourcePolicyFromOptions(opts), nil
}

func applyIndexResourceOverrides(opts *minnowcode.Options, cli indexCLIOptions) {
	if cli.requestBatchSize > 0 {
		opts.EmbedBatchSize = cli.requestBatchSize
	}
	if cli.maxBatchBytes > 0 {
		opts.MaxBatchBytes = cli.maxBatchBytes
	}
	if cli.maxHeapBytes > 0 {
		opts.MaxHeapBytes = cli.maxHeapBytes
	}
	if cli.maxRSSBytes > 0 {
		opts.MaxRSSBytes = cli.maxRSSBytes
	}
	if cli.largeRepoFiles > 0 {
		opts.LargeRepoFiles = cli.largeRepoFiles
	}
	if cli.throttle > 0 {
		opts.Throttle = cli.throttle
	}
}

func applyLowResourceDefaults(opts *minnowcode.Options, cli indexCLIOptions) {
	if cli.requestBatchSize == 0 {
		opts.EmbedBatchSize = 16
	}
	if cli.maxBatchBytes == 0 {
		opts.MaxBatchBytes = 128 * 1024
	}
	if cli.throttle == 0 {
		opts.Throttle = 250 * time.Millisecond
	}
}

type emitFunc func(context.Context, []minnowcode.Document) error

type documentIngester interface {
	ingest(ctx context.Context, kbID string, docs []minnowcode.Document) error
}

type documentSink struct {
	client   documentIngester
	kbID     string
	policy   minnowcode.ResourcePolicy
	journal  uploadRecorder
	progress *progressReporter
	pending  []minnowcode.Document
	lengths  []int
	sent     bool
}

func (s *documentSink) emit(ctx context.Context, docs []minnowcode.Document) error {
	s.pending = append(s.pending, docs...)
	return s.drain(ctx, false)
}

func (s *documentSink) close(ctx context.Context) error {
	return s.drain(ctx, true)
}

func (s *documentSink) drain(ctx context.Context, final bool) error {
	for len(s.pending) > 0 {
		s.lengths = s.lengths[:0]
		for _, doc := range s.pending {
			s.lengths = append(s.lengths, len(doc.Text))
		}
		end := s.policy.BatchEndByTextBytes(s.lengths)
		if end <= 0 {
			end = 1
		}
		// More documents may still arrive to fill this batch.
		if !final && end == len(s.pending) {
			return nil
		}
		if err := s.policy.Check(ctx); err != nil {
			return err
		}
		if s.sent {
			if err := s.policy.ThrottleBatch(ctx); err != nil {
				return err
			}
		}
		if s.journal != nil {
			ids := make([]string, 0, end)
			for _, doc := range s.pending[:end] {
				ids = append(ids, doc.ID)
			}
			if err := s.journal.record(ids); err != nil {
				return err
			}
		}
		if err := s.client.ingest(ctx, s.kbID, s.pending[:end]); err != nil {
			return err
		}
		s.progress.chunksSent(end)
		s.sent = true
		kept := copy(s.pending, s.pending[end:])
		clear(s.pending[kept:]) // drop sent chunk text so it can be collected
		s.pending = s.pending[:kept]
	}
	return nil
}

type documentDeleter interface {
	delete(ctx context.Context, kbID string, ids []string) error
}

func sendDeletes(ctx context.Context, client documentDeleter, kbID string, deleteIDs []string) error {
	for len(deleteIDs) > 0 {
		end := min(len(deleteIDs), 200)
		if err := client.delete(ctx, kbID, deleteIDs[:end]); err != nil {
			return err
		}
		deleteIDs = deleteIDs[end:]
	}
	return nil
}

func saveRegistrySelection(target indexTarget, opts minnowcode.Options) error {
	registry, err := minnowcode.LoadRegistry(target.StateRoot)
	if err != nil {
		return err
	}
	registry.Indexes[target.IndexKey] = minnowcode.RegistryEntry{
		KBID: target.KBID, Root: minnowcode.RelativeRoot(target.StateRoot, target.Root),
		Description: target.Description, IncludeUntracked: opts.IncludeUntracked,
	}
	if target.LegacyIndexKey != "" && target.LegacyIndexKey != target.IndexKey {
		delete(registry.Indexes, target.LegacyIndexKey)
	}
	return minnowcode.SaveRegistry(target.StateRoot, registry)
}

// sessionPath holds the handle the server issued, so an interrupted run
// resumes its own session instead of waiting out the lease.
func sessionPath(target indexTarget) string {
	sum := sha256.Sum256([]byte(target.KBID))
	name := ".session-" + hex.EncodeToString(sum[:8])
	return filepath.Join(target.StateRoot, ".minnow", "codeindex", name)
}

func loadSession(target indexTarget) string {
	data, err := os.ReadFile(sessionPath(target))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// saveSession is best effort, since losing the handle only costs the wait.
func saveSession(target indexTarget, id string) {
	path := sessionPath(target)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(path, []byte(id), 0o600)
}

func clearSession(target indexTarget) {
	_ = os.Remove(sessionPath(target))
}
