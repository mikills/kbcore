package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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
	releaseLock, err := acquireIndexLock(target, operationTTL+time.Minute)
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
	files, skipped, err := minnowcode.Scan(ctx, target.Root, opts, minnowcode.DefaultExcludePatterns)
	if err != nil {
		return indexResult{}, err
	}
	if err := minnowcode.ValidateConfirmation(opts, len(files)); err != nil {
		return indexResult{}, err
	}
	previous, _, err := loadIndexState(target)
	if err != nil {
		return indexResult{}, err
	}
	pipeline := pipelineFingerprint(opts)
	plan, err := buildIndexPlan(ctx, target, opts, pipeline, previous, files, skipped)
	if err != nil {
		return indexResult{}, err
	}
	client, err := newMinnowClient(cfg)
	if err != nil {
		return indexResult{}, err
	}
	if err := client.check(ctx); err != nil {
		return indexResult{}, fmt.Errorf("connect to Minnow at %s: %w", cfg.Minnow.URL, err)
	}
	if err := sendIndexPlan(ctx, client, target.KBID, plan.documents, plan.deleteIDs, policy); err != nil {
		return indexResult{}, err
	}
	plan.state.UpdatedAt = time.Now().UTC()
	statePath, err := saveIndexState(target, plan.state)
	if err != nil {
		return indexResult{}, err
	}
	if err := saveRegistrySelection(target, opts); err != nil {
		return indexResult{}, err
	}
	plan.result.StatePath = statePath
	return plan.result, nil
}

type indexPlan struct {
	state     indexState
	documents []minnowcode.Document
	deleteIDs []string
	result    indexResult
}

func buildIndexPlan(
	ctx context.Context,
	target indexTarget,
	opts minnowcode.Options,
	pipeline string,
	previous indexState,
	files []minnowcode.ScannedFile,
	skipped int,
) (indexPlan, error) {
	plan := indexPlan{
		state: indexState{
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
	}
	paths := make([]string, 0, len(current))
	for path := range current {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		file := current[path]
		docs, _, err := minnowcode.BuildDocuments(ctx, target.Root, target.RepoID, file, opts)
		if err != nil {
			return indexPlan{}, err
		}
		chunkIDs := make([]string, 0, len(docs))
		for _, doc := range docs {
			chunkIDs = append(chunkIDs, doc.ID)
			plan.documents = append(plan.documents, doc)
		}
		plan.state.Files[path] = stateFile{
			Hash: file.Hash, SizeBytes: file.SizeBytes, Language: file.Language, ChunkIDs: chunkIDs,
		}
		plan.result.IndexedFiles++
		plan.result.ChunksIndexed += len(docs)
	}
	newChunkIDs := make(map[string]struct{}, len(plan.documents))
	for _, doc := range plan.documents {
		newChunkIDs[doc.ID] = struct{}{}
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

func pipelineFingerprint(opts minnowcode.Options) string {
	data, _ := json.Marshal(struct {
		Include      []string `json:"include"`
		Exclude      []string `json:"exclude"`
		MaxFileBytes int64    `json:"max_file_bytes"`
		ChunkSize    int      `json:"chunk_size"`
		Overlap      int      `json:"chunk_overlap"`
	}{opts.Include, opts.Exclude, opts.MaxFileBytes, opts.ChunkSize, opts.ChunkOverlap})
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

func sendIndexPlan(
	ctx context.Context,
	client *minnowClient,
	kbID string,
	docs []minnowcode.Document,
	deleteIDs []string,
	policy minnowcode.ResourcePolicy,
) error {
	for len(docs) > 0 {
		lengths := make([]int, len(docs))
		for i, doc := range docs {
			lengths[i] = len(doc.Text)
		}
		end := policy.BatchEndByTextBytes(lengths)
		if end <= 0 {
			end = 1
		}
		if err := client.ingest(ctx, kbID, docs[:end]); err != nil {
			return err
		}
		docs = docs[end:]
		if len(docs) != 0 {
			if err := policy.ThrottleBatch(ctx); err != nil {
				return err
			}
		}
	}
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
	return minnowcode.SaveRegistry(target.StateRoot, registry)
}
