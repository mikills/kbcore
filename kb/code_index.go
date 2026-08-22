package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	codeindex "github.com/mikills/minnow/kb/codeindex"
)

const (
	DefaultCodeChunkSize    = codeindex.DefaultChunkSize
	DefaultCodeChunkOverlap = codeindex.DefaultChunkOverlap
	DefaultCodeMaxFileBytes = codeindex.DefaultMaxFileBytes
	CodeIndexManifestSchema = "minnow.code_index/v1"
)

type CodeIndexOptions = codeindex.Options
type CodeIndexResult = codeindex.Result
type CodeIndexStatus = codeindex.Status
type CodeSearchOptions = codeindex.SearchOptions
type CodeSearchResult = codeindex.SearchResult
type CodebaseIndexRegistry = codeindex.Registry
type CodebaseIndexRegistryEntry = codeindex.RegistryEntry

var ErrCodeIndexRequiresConfirmation = codeindex.ErrRequiresConfirmation

type codeIndexManifest struct {
	SchemaVersion string                       `json:"schema_version"`
	KBID          string                       `json:"kb_id"`
	Root          string                       `json:"root"`
	RepoID        string                       `json:"repo_id"`
	UpdatedAt     time.Time                    `json:"updated_at"`
	Files         map[string]codeIndexedFile   `json:"files"`
	Chunks        map[string]CodeChunkMetadata `json:"chunks"`
}

type codeIndexedFile struct {
	Path      string   `json:"path"`
	Hash      string   `json:"hash"`
	SizeBytes int64    `json:"size_bytes"`
	Language  string   `json:"language,omitempty"`
	ChunkIDs  []string `json:"chunk_ids"`
}

type resolvedCodeIndexTarget = codeindex.Target

func (k *KB) IndexCodebase(ctx context.Context, opts CodeIndexOptions) (CodeIndexResult, error) {
	started := time.Now()
	if k == nil || k.BlobStore == nil {
		return CodeIndexResult{}, errors.New("code index: BlobStore not configured")
	}
	target, err := codeindex.ResolveTarget(opts)
	if err != nil {
		return CodeIndexResult{}, err
	}
	slog.Default().
		InfoContext(ctx, "code index start", logKeyKBID, target.KBID, "index_key", target.IndexKey, "root", target.Root, "repo_root", target.RepoRoot, "include_untracked", target.Options.IncludeUntracked)

	manifest, version, err := k.loadPreparedCodeIndexManifest(ctx, target.KBID)
	if err != nil {
		return CodeIndexResult{}, err
	}
	scanned, skipped, err := codeindex.Scan(ctx, target.Root, target.Options, codeindex.DefaultExcludePatterns)
	if err != nil {
		return CodeIndexResult{}, err
	}
	if err := codeindex.ValidateConfirmation(target.Options, len(scanned)); err != nil {
		return CodeIndexResult{}, err
	}

	result, deletions, superseded, nextFiles, nextChunks := diffCodeIndexManifest(target, manifest, scanned, skipped)
	if err := k.publishCodeIndexChanges(ctx, target, codeIndexPublishState{
		scanned: scanned, nextFiles: nextFiles, nextChunks: nextChunks, oldChunks: manifest.Chunks,
		deletions: deletions, superseded: superseded, result: &result,
	}); err != nil {
		return CodeIndexResult{}, err
	}
	deleteIDs := deletions.sortedIDs()
	result.ChunksDeleted = len(deleteIDs)
	if err := k.deleteRemovedCodeChunks(ctx, target.KBID, deleteIDs); err != nil {
		return CodeIndexResult{}, err
	}
	if err := k.saveCodeIndexState(ctx, target, nextFiles, nextChunks, version); err != nil {
		return CodeIndexResult{}, err
	}
	slog.Default().
		InfoContext(ctx, "code index complete", logKeyKBID, target.KBID, "indexed_files", result.IndexedFiles, "unchanged_files", result.UnchangedFiles, "changed_during_run", result.ChangedDuringRun, "chunks_indexed", result.ChunksIndexed, "total_duration_ms", time.Since(started).Milliseconds())
	return result, nil
}

func (k *KB) loadPreparedCodeIndexManifest(ctx context.Context, kbID string) (codeIndexManifest, string, error) {
	loadStarted := time.Now()
	manifest, version, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return codeIndexManifest{}, "", err
	}
	if manifest.Files == nil {
		manifest.Files = map[string]codeIndexedFile{}
	}
	if manifest.Chunks == nil {
		manifest.Chunks = map[string]CodeChunkMetadata{}
	}
	slog.Default().
		InfoContext(ctx, "code index manifest loaded", logKeyKBID, kbID, "files", len(manifest.Files), "chunks", len(manifest.Chunks), logKeyDurationMS, time.Since(loadStarted).Milliseconds())
	return manifest, version, nil
}

func diffCodeIndexManifest(
	target resolvedCodeIndexTarget,
	manifest codeIndexManifest,
	scanned []codeScannedFile,
	skipped int,
) (CodeIndexResult, codeIndexDeletionPlan, map[string]codeIndexedFile, map[string]codeIndexedFile, map[string]CodeChunkMetadata) {
	current := make(map[string]codeScannedFile, len(scanned))
	for _, file := range scanned {
		current[file.RelPath] = file
	}
	result := CodeIndexResult{
		KBID:         target.KBID,
		IndexKey:     target.IndexKey,
		Description:  target.Description,
		Root:         target.Root,
		ScannedFiles: len(scanned),
		SkippedFiles: skipped,
		ManifestKey:  codeIndexManifestKey(target.KBID),
	}
	deletions := make(codeIndexDeletionPlan)
	superseded := make(map[string]codeIndexedFile)
	nextFiles := make(map[string]codeIndexedFile, len(current))
	nextChunks := make(map[string]CodeChunkMetadata)
	for relPath, old := range manifest.Files {
		file, ok := current[relPath]
		if !ok {
			deletions.add(old.ChunkIDs)
			result.DeletedFiles++
			continue
		}
		if old.Hash == file.Hash && old.Language == file.Language {
			nextFiles[relPath] = old
			for _, id := range old.ChunkIDs {
				if chunk, ok := manifest.Chunks[id]; ok {
					nextChunks[id] = chunk
				}
			}
			result.UnchangedFiles++
			continue
		}
		deletions.add(old.ChunkIDs)
		superseded[relPath] = old
	}
	return result, deletions, superseded, nextFiles, nextChunks
}

type codeIndexDeletionPlan map[string]struct{}

func (p codeIndexDeletionPlan) add(ids []string) {
	for _, id := range ids {
		p[id] = struct{}{}
	}
}

func (p codeIndexDeletionPlan) remove(ids []string) {
	for _, id := range ids {
		delete(p, id)
	}
}

func (p codeIndexDeletionPlan) sortedIDs() []string {
	ids := make([]string, 0, len(p))
	for id := range p {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

type codeIndexPublishState struct {
	scanned    []codeScannedFile
	nextFiles  map[string]codeIndexedFile
	nextChunks map[string]CodeChunkMetadata
	oldChunks  map[string]CodeChunkMetadata
	deletions  codeIndexDeletionPlan
	superseded map[string]codeIndexedFile
	result     *CodeIndexResult
}

func (k *KB) publishCodeIndexChanges(
	ctx context.Context,
	target resolvedCodeIndexTarget,
	state codeIndexPublishState,
) error {
	streamStarted := time.Now()
	streamer := newCodeDocumentStreamer(k, target, state)
	graphEnabled := false
	if err := k.publishPreparedStream(ctx, PreparedStreamRequest{KBID: target.KBID, Options: UpsertDocsOptions{GraphEnabled: &graphEnabled}, Next: streamer.Next}); err != nil {
		return err
	}
	slog.Default().
		InfoContext(ctx, "code index stream publish complete", logKeyKBID, target.KBID, "indexed_files", state.result.IndexedFiles, "chunks_indexed", state.result.ChunksIndexed, logKeyDurationMS, time.Since(streamStarted).Milliseconds())
	return nil
}

func (k *KB) deleteRemovedCodeChunks(ctx context.Context, kbID string, deleteIDs []string) error {
	if len(deleteIDs) == 0 {
		return nil
	}
	deleteStarted := time.Now()
	if err := k.DeleteDocsAndUpload(ctx, kbID, deleteIDs, DeleteDocsOptions{HardDelete: true, CleanupGraph: true}); err != nil {
		return err
	}
	slog.Default().
		InfoContext(ctx, "code index delete complete", logKeyKBID, kbID, "chunks_deleted", len(deleteIDs), logKeyDurationMS, time.Since(deleteStarted).Milliseconds())
	return nil
}

func (k *KB) saveCodeIndexState(
	ctx context.Context,
	target resolvedCodeIndexTarget,
	nextFiles map[string]codeIndexedFile,
	nextChunks map[string]CodeChunkMetadata,
	version string,
) error {
	nextManifest := codeIndexManifest{
		SchemaVersion: CodeIndexManifestSchema,
		KBID:          target.KBID,
		Root:          target.Root,
		RepoID:        target.RepoID,
		UpdatedAt:     k.Clock.Now(),
		Files:         nextFiles,
		Chunks:        nextChunks,
	}
	manifestSaveStarted := time.Now()
	if err := k.saveCodeIndexManifest(ctx, nextManifest, version); err != nil {
		return err
	}
	slog.Default().
		InfoContext(ctx, "code index manifest saved", logKeyKBID, target.KBID, logKeyDurationMS, time.Since(manifestSaveStarted).Milliseconds())
	registry := target.Registry
	registry.Indexes[target.IndexKey] = CodebaseIndexRegistryEntry{
		KBID:             target.KBID,
		Root:             codeindex.RelativeRoot(target.RegistryRoot, target.Root),
		Description:      target.Description,
		IncludeUntracked: target.Options.IncludeUntracked,
	}
	return codeindex.SaveRegistry(target.RegistryRoot, registry)
}

func (k *KB) embedCodeDocuments(ctx context.Context, docs []Document, batchSize int) ([]EmbeddedDocument, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	batcher, ok := k.Embedder.(BatchEmbedder)
	if !ok {
		return k.embedCodeDocumentsOneByOne(ctx, docs)
	}
	out := make([]EmbeddedDocument, 0, len(docs))
	if batchSize <= 0 {
		batchSize = DefaultCodeEmbedBatchSize
	}
	for start := 0; start < len(docs); start += batchSize {
		end := min(start+batchSize, len(docs))
		inputs := make([]string, len(docs[start:end]))
		for i, doc := range docs[start:end] {
			inputs[i] = doc.Text
		}
		vectors, err := batcher.EmbedBatch(ctx, inputs)
		if err != nil {
			return nil, err
		}
		if len(vectors) != len(inputs) {
			return nil, fmt.Errorf("batch embed returned %d vectors for %d code chunks", len(vectors), len(inputs))
		}
		for i, doc := range docs[start:end] {
			out = append(
				out,
				EmbeddedDocument{ID: doc.ID, Text: doc.Text, Metadata: doc.Metadata, Embedding: vectors[i]},
			)
		}
	}
	return out, nil
}

func (k *KB) embedCodeDocumentsOneByOne(ctx context.Context, docs []Document) ([]EmbeddedDocument, error) {
	out := make([]EmbeddedDocument, 0, len(docs))
	for _, doc := range docs {
		vec, err := k.Embed(ctx, doc.Text)
		if err != nil {
			return nil, err
		}
		out = append(out, EmbeddedDocument{ID: doc.ID, Text: doc.Text, Metadata: doc.Metadata, Embedding: vec})
	}
	return out, nil
}

type codeDocumentStreamer struct {
	k          *KB
	root       string
	repoID     string
	files      []codeScannedFile
	opts       CodeIndexOptions
	policy     CodeIndexResourcePolicy
	nextFiles  map[string]codeIndexedFile
	nextChunks map[string]CodeChunkMetadata
	oldChunks  map[string]CodeChunkMetadata
	deletions  codeIndexDeletionPlan
	superseded map[string]codeIndexedFile
	result     *CodeIndexResult
	filePos    int
	pending    []Document
}

func newCodeDocumentStreamer(k *KB, target resolvedCodeIndexTarget, state codeIndexPublishState) *codeDocumentStreamer {
	return &codeDocumentStreamer{
		k:          k,
		root:       target.Root,
		repoID:     target.RepoID,
		files:      state.scanned,
		opts:       target.Options,
		policy:     codeindex.ResourcePolicyFromOptions(target.Options),
		nextFiles:  state.nextFiles,
		nextChunks: state.nextChunks,
		oldChunks:  state.oldChunks,
		deletions:  state.deletions,
		superseded: state.superseded,
		result:     state.result,
	}
}

func (s *codeDocumentStreamer) Next(ctx context.Context) ([]EmbeddedDocument, error) {
	if err := s.policy.Check(ctx); err != nil {
		return nil, err
	}
	if err := s.fillPending(ctx); err != nil {
		return nil, err
	}
	if len(s.pending) == 0 {
		return nil, nil
	}
	lengths := make([]int, len(s.pending))
	for i, doc := range s.pending {
		lengths[i] = len(doc.Text)
	}
	end := s.policy.BatchEndByTextBytes(lengths)
	if end <= 0 {
		end = 1
	}
	batch := s.pending[:end]
	s.pending = s.pending[end:]
	embedStarted := time.Now()
	embedded, err := s.k.embedCodeDocuments(ctx, batch, s.policy.EmbedBatchSize)
	if err != nil {
		return nil, err
	}
	slog.Default().
		InfoContext(ctx, "code index embed batch complete", "chunks", len(batch), "files_seen", s.filePos, "pending_chunks", len(s.pending), logKeyDurationMS, time.Since(embedStarted).Milliseconds())
	if err := s.policy.Check(ctx); err != nil {
		return nil, err
	}
	if err := s.policy.ThrottleBatch(ctx); err != nil {
		return nil, err
	}
	return embedded, nil
}

func (s *codeDocumentStreamer) fillPending(ctx context.Context) error {
	for s.shouldReadMoreFiles() {
		file := s.files[s.filePos]
		s.filePos++
		if _, ok := s.nextFiles[file.RelPath]; ok {
			continue
		}
		if err := s.addCodeFile(ctx, file); err != nil {
			return err
		}
	}
	return nil
}

func (s *codeDocumentStreamer) shouldReadMoreFiles() bool {
	total := 0
	for _, doc := range s.pending {
		total += len(doc.Text)
	}
	return len(s.pending) < s.policy.EmbedBatchSize && total < s.policy.MaxBatchBytes && s.filePos < len(s.files)
}

func (s *codeDocumentStreamer) addCodeFile(ctx context.Context, file codeScannedFile) error {
	docs, metas, err := buildCodeDocuments(ctx, s.root, s.repoID, file, s.opts)
	if errors.Is(err, codeindex.ErrFileChanged) {
		if old, ok := s.superseded[file.RelPath]; ok {
			s.nextFiles[file.RelPath] = old
			for _, id := range old.ChunkIDs {
				if chunk, exists := s.oldChunks[id]; exists {
					s.nextChunks[id] = chunk
				}
			}
			s.deletions.remove(old.ChunkIDs)
		}
		s.result.ChangedDuringRun++
		return nil
	}
	if err != nil {
		return err
	}
	chunkIDs := make([]string, 0, len(docs))
	for _, doc := range docs {
		chunkIDs = append(chunkIDs, doc.ID)
		s.pending = append(s.pending, doc)
	}
	for _, meta := range metas {
		s.nextChunks[meta.ID] = meta
	}
	s.nextFiles[file.RelPath] = codeIndexedFile{
		Path:      file.RelPath,
		Hash:      file.Hash,
		SizeBytes: file.SizeBytes,
		Language:  file.Language,
		ChunkIDs:  chunkIDs,
	}
	s.result.IndexedFiles++
	s.result.ChunksIndexed += len(docs)
	return nil
}

func (k *KB) publishCodeDocumentStream(
	ctx context.Context,
	kbID string,
	docs []Document,
	opts UpsertDocsOptions,
) error {
	pos := 0
	return k.publishPreparedStream(ctx, PreparedStreamRequest{
		KBID:    kbID,
		Options: opts,
		Next: func(ctx context.Context) ([]EmbeddedDocument, error) {
			if pos >= len(docs) {
				return nil, nil
			}
			end := min(pos+DefaultCodeEmbedBatchSize, len(docs))
			batch := docs[pos:end]
			pos = end
			embedStarted := time.Now()
			embedded, err := k.embedCodeDocuments(ctx, batch, DefaultCodeEmbedBatchSize)
			if err != nil {
				return nil, err
			}
			slog.Default().
				InfoContext(ctx, "code index embed batch complete", logKeyKBID, kbID, "chunks", len(batch), logKeyDurationMS, time.Since(embedStarted).Milliseconds())
			return embedded, nil
		},
	})
}

func ResolveCodeIndexSelection(root, indexKey, kbID string) (CodeIndexOptions, error) {
	return codeindex.ResolveSelection(root, indexKey, kbID)
}

func LoadCodebaseIndexRegistry(root string) (CodebaseIndexRegistry, error) {
	return codeindex.LoadRegistry(root)
}

func codeIndexManifestKey(kbID string) string { return kbID + ".code-index.json" }

func (k *KB) loadCodeIndexManifest(ctx context.Context, kbID string) (codeIndexManifest, string, error) {
	key := codeIndexManifestKey(kbID)
	info, err := k.BlobStore.Head(ctx, key)
	if err != nil {
		return emptyCodeIndexManifestForHeadError(kbID, err)
	}
	tmp, err := os.CreateTemp("", "minnow-code-index-*.json")
	if err != nil {
		return codeIndexManifest{}, "", err
	}
	path := tmp.Name()
	if err := tmp.Close(); err != nil {
		return codeIndexManifest{}, "", err
	}
	defer removeCodeIndexManifestTemp(path)
	if err := k.BlobStore.Download(ctx, key, path); err != nil {
		return codeIndexManifest{}, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return codeIndexManifest{}, "", err
	}
	var manifest codeIndexManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return codeIndexManifest{}, "", err
	}
	if manifest.SchemaVersion != CodeIndexManifestSchema {
		return codeIndexManifest{}, "", fmt.Errorf("unsupported code index manifest schema %q", manifest.SchemaVersion)
	}
	normalizeLoadedCodeIndexManifest(&manifest)
	return manifest, info.Version, nil
}

func emptyCodeIndexManifestForHeadError(kbID string, err error) (codeIndexManifest, string, error) {
	if !errors.Is(err, os.ErrNotExist) {
		return codeIndexManifest{}, "", err
	}
	return codeIndexManifest{
		SchemaVersion: CodeIndexManifestSchema,
		KBID:          kbID,
		Files:         map[string]codeIndexedFile{},
		Chunks:        map[string]CodeChunkMetadata{},
	}, "", nil
}

func normalizeLoadedCodeIndexManifest(manifest *codeIndexManifest) {
	if manifest.Files == nil {
		manifest.Files = map[string]codeIndexedFile{}
	}
	if manifest.Chunks == nil {
		manifest.Chunks = map[string]CodeChunkMetadata{}
	}
}

func (k *KB) saveCodeIndexManifest(ctx context.Context, manifest codeIndexManifest, expectedVersion string) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "minnow-code-index-*.json")
	if err != nil {
		return err
	}
	path := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		closeCodeIndexManifestTemp(tmp)
		removeCodeIndexManifestTemp(path)
		return err
	}
	if err := tmp.Close(); err != nil {
		removeCodeIndexManifestTemp(path)
		return err
	}
	defer removeCodeIndexManifestTemp(path)
	_, err = k.BlobStore.UploadIfMatch(ctx, codeIndexManifestKey(manifest.KBID), path, expectedVersion)
	return err
}

func closeCodeIndexManifestTemp(file *os.File) {
	if err := file.Close(); err != nil {
		slog.Default().Warn("code index manifest temp close failed", logKeyError, err)
	}
}

func removeCodeIndexManifestTemp(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Default().Warn("code index manifest temp remove failed", "path", path, logKeyError, err)
	}
}

func (k *KB) CodeIndexStatus(ctx context.Context, kbID string) (CodeIndexStatus, error) {
	if strings.TrimSpace(kbID) == "" {
		kbID = codeindex.DefaultKBIDForIndexKey("default")
	}
	manifest, version, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return CodeIndexStatus{}, err
	}
	if version == "" {
		return CodeIndexStatus{KBID: kbID, Indexed: false, ManifestKey: codeIndexManifestKey(kbID)}, nil
	}
	chunkCount := 0
	for _, file := range manifest.Files {
		chunkCount += len(file.ChunkIDs)
	}
	updatedAt := manifest.UpdatedAt
	return CodeIndexStatus{
		KBID:        kbID,
		Indexed:     true,
		Root:        manifest.Root,
		RepoID:      manifest.RepoID,
		UpdatedAt:   &updatedAt,
		FileCount:   len(manifest.Files),
		ChunkCount:  chunkCount,
		ManifestKey: codeIndexManifestKey(kbID),
	}, nil
}

func (k *KB) SearchCode(ctx context.Context, kbID, query string, opts CodeSearchOptions) ([]CodeSearchResult, error) {
	if strings.TrimSpace(kbID) == "" {
		kbID = codeindex.DefaultKBIDForIndexKey("default")
	}
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query is required")
	}
	opts = normalizeCodeSearchOptions(opts)
	manifest, _, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return nil, err
	}
	vec, err := k.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	results, err := k.Search(ctx, kbID, vec, &SearchOptions{TopK: codeSearchFanout(opts.TopK)})
	if err != nil {
		return nil, err
	}
	return filterCodeSearchResultsWithMetadata(results, manifest, opts), nil
}

func filterCodeSearchResultsWithMetadata(
	results []ExpandedResult,
	manifest codeIndexManifest,
	opts CodeSearchOptions,
) []CodeSearchResult {
	out := make([]CodeSearchResult, 0, opts.TopK)
	pathFilter := strings.TrimSpace(opts.Path)
	langFilter := strings.ToLower(strings.TrimSpace(opts.Language))
	for _, result := range results {
		meta, ok := manifest.Chunks[result.ID]
		if !ok {
			meta, ok = codeMetadataFromResult(result.Metadata)
		}
		if !ok || !codeSearchMetaMatches(meta, pathFilter, langFilter) {
			continue
		}
		out = append(out, CodeSearchResult{
			ID: result.ID, Content: result.Content, Distance: result.Distance,
			Path: meta.Path, Language: meta.Language, Symbol: meta.Symbol, Kind: meta.Kind,
			StartLine: meta.StartLine, EndLine: meta.EndLine,
		})
		if len(out) >= opts.TopK {
			break
		}
	}
	return out
}

func normalizeCodeSearchOptions(opts CodeSearchOptions) CodeSearchOptions {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	return opts
}

func codeSearchFanout(topK int) int {
	searchK := topK * 20
	if searchK < 200 {
		return 200
	}
	return searchK
}

func filterCodeSearchResults(
	results []ExpandedResult,
	manifest codeIndexManifest,
	opts CodeSearchOptions,
) []CodeSearchResult {
	out := make([]CodeSearchResult, 0, opts.TopK)
	pathFilter := strings.TrimSpace(opts.Path)
	langFilter := strings.ToLower(strings.TrimSpace(opts.Language))
	for _, result := range results {
		if appendCodeSearchResult(
			&out,
			result,
			codeSearchFilter{manifest: manifest, path: pathFilter, language: langFilter, topK: opts.TopK},
		) {
			break
		}
	}
	return out
}

type codeSearchFilter struct {
	manifest codeIndexManifest
	path     string
	language string
	topK     int
}

func appendCodeSearchResult(out *[]CodeSearchResult, result ExpandedResult, filter codeSearchFilter) bool {
	meta, ok := filter.manifest.Chunks[result.ID]
	if !ok || !codeSearchMetaMatches(meta, filter.path, filter.language) {
		return false
	}
	*out = append(
		*out,
		CodeSearchResult{
			ID:        result.ID,
			Content:   result.Content,
			Distance:  result.Distance,
			Path:      meta.Path,
			Language:  meta.Language,
			Symbol:    meta.Symbol,
			Kind:      meta.Kind,
			StartLine: meta.StartLine,
			EndLine:   meta.EndLine,
		},
	)
	return len(*out) >= filter.topK
}

func codeSearchMetaMatches(meta CodeChunkMetadata, pathFilter string, langFilter string) bool {
	if pathFilter != "" && !strings.Contains(meta.Path, pathFilter) {
		return false
	}
	if langFilter != "" && strings.ToLower(meta.Language) != langFilter {
		return false
	}
	return true
}

func filterRemoteCodeSearchResults(results []ExpandedResult, opts CodeSearchOptions) []CodeSearchResult {
	return filterCodeSearchResultsWithMetadata(results, codeIndexManifest{}, opts)
}

func codeMetadataFromResult(metadata map[string]any) (CodeChunkMetadata, bool) {
	path, ok := metadataString(metadata, "code_path")
	if !ok {
		return CodeChunkMetadata{}, false
	}
	language, _ := metadataString(metadata, "code_language")
	symbol, _ := metadataString(metadata, "code_symbol")
	kind, _ := metadataString(metadata, "code_kind")
	hash, _ := metadataString(metadata, "code_file_hash")
	return CodeChunkMetadata{
		Path: path, Language: language, Symbol: symbol, Kind: kind, Hash: hash,
		StartLine: metadataInt(metadata["code_start_line"]), EndLine: metadataInt(metadata["code_end_line"]),
	}, true
}

func metadataString(metadata map[string]any, key string) (string, bool) {
	value, ok := metadata[key].(string)
	return value, ok && value != ""
}

func metadataInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}
