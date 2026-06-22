package duckdb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
)

// NewArtifactFormat creates a new DuckDBArtifactFormat from the given deps.
func NewArtifactFormat(deps DuckDBArtifactDeps) (*DuckDBArtifactFormat, error) {
	if err := deps.validate(); err != nil {
		return nil, fmt.Errorf("duckdb artifact format: %v", err)
	}
	return &DuckDBArtifactFormat{deps: deps}, nil
}

type DuckDBArtifactFormat struct {
	deps DuckDBArtifactDeps
	pool shardConnPool
}

type DuckDBArtifactDeps struct {
	BlobStore      kb.BlobStore
	ManifestStore  kb.ManifestStore
	CacheDir       string
	MemoryLimit    string
	ExtensionDir   string
	OfflineExt     bool
	DuckDBThreads  int
	ShardingPolicy kb.ShardingPolicy

	Embed        func(context.Context, string) ([]float32, error)
	GraphBuilder func() *kb.GraphBuilder

	EvictCacheIfNeeded         func(context.Context, string) error
	LockFor                    func(string) *sync.Mutex
	AcquireWriteLease          func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error)
	EnqueueReplacedShardsForGC func(kbID string, shards []kb.SnapshotShardMetadata, now time.Time)
	Metrics                    kb.ShardMetricsObserver
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
	if kb.NormalizeShardingPolicy(d.ShardingPolicy).CompactionEnabled && d.AcquireWriteLease == nil {
		return fmt.Errorf("AcquireWriteLease is required when compaction is enabled")
	}
	return nil
}

const (
	DuckDBFormatKind    = "duckdb_sharded"
	DuckDBFormatVersion = 2
)

func (f *DuckDBArtifactFormat) validateManifestFormat(manifest *kb.SnapshotShardManifest) error {
	if manifest.FormatKind != DuckDBFormatKind {
		return fmt.Errorf(
			"%w: manifest format_kind %q does not match expected %q",
			kb.ErrArtifactFormatNotConfigured,
			manifest.FormatKind,
			DuckDBFormatKind,
		)
	}
	if manifest.FormatVersion != DuckDBFormatVersion {
		return fmt.Errorf(
			"%w: manifest format_version %d is not supported (expected %d)",
			kb.ErrArtifactFormatNotConfigured,
			manifest.FormatVersion,
			DuckDBFormatVersion,
		)
	}
	// A manifest carries one FormatVersion that applies to all shards.
	// Reject any encoded per-shard contradiction by requiring shards to
	// leave their version unset (i.e. rely on the manifest-level version).
	// If a future change adds a per-shard override it must match the
	// manifest version, so surface the invariant here.
	if err := validateShardsShareManifestVersion(manifest); err != nil {
		return err
	}
	return nil
}

// validateShardsShareManifestVersion enforces that the manifest-level
// FormatVersion is consistent across the shard set. A manifest carries a
// single FormatVersion today, so the check reduces to a sanity pass: every
// shard must be non-empty and belong to the manifest. Future per-shard
// version metadata must match the manifest version or this function must
// reject the manifest.
func validateShardsShareManifestVersion(manifest *kb.SnapshotShardManifest) error {
	if len(manifest.Shards) == 0 {
		return nil
	}
	for _, shard := range manifest.Shards {
		if strings.TrimSpace(shard.ShardID) == "" {
			return fmt.Errorf("%w: manifest contains shard with empty shard_id", kb.ErrArtifactFormatNotConfigured)
		}
	}
	return nil
}

// Close drains the shard connection pool.
func (f *DuckDBArtifactFormat) Close() {
	f.pool.CloseAll()
}

// ClosePooledConns closes pooled connections matching the path prefix.
// Satisfies the PooledConnCloser interface used by cache eviction.
func (f *DuckDBArtifactFormat) ClosePooledConns(pathPrefix string) {
	f.pool.CloseByPrefix(pathPrefix)
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

func (f *DuckDBArtifactFormat) FetchVectors(ctx context.Context, kbID string, ids []string) ([]kb.VectorRecord, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kb_id is required")
	}
	if len(ids) == 0 {
		return []kb.VectorRecord{}, nil
	}
	shards, err := f.resolveShards(ctx, kbID)
	if err != nil {
		return nil, err
	}
	return f.fetchVectorsFromShards(ctx, kbID, shards, ids)
}

func (f *DuckDBArtifactFormat) resolveShards(ctx context.Context, kbID string) ([]kb.SnapshotShardMetadata, error) {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return nil, kb.ErrKBUninitialized
		}
		return nil, err
	}
	if err := f.validateManifestFormat(&doc.Manifest); err != nil {
		return nil, err
	}
	return doc.Manifest.Shards, nil
}

func (f *DuckDBArtifactFormat) fetchVectorsFromShards(ctx context.Context, kbID string, shards []kb.SnapshotShardMetadata, ids []string) ([]kb.VectorRecord, error) {
	remaining := make([]string, 0, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) != "" {
			remaining = append(remaining, id)
		}
	}
	out := make([]kb.VectorRecord, 0, len(remaining))
	for _, shard := range shards {
		if len(remaining) == 0 {
			break
		}
		conn, err := f.openCachedShardConn(ctx, kbID, shard)
		if err != nil {
			return nil, fmt.Errorf("fetch vectors shard %s: %w", shard.ShardID, err)
		}
		metaByID, err := queryMetadataByIDs(ctx, conn.db, remaining)
		conn.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("fetch vectors shard %s: %w", shard.ShardID, err)
		}
		notFound := remaining[:0]
		for _, id := range remaining {
			if meta, ok := metaByID[id]; ok {
				out = append(out, kb.VectorRecord{ID: id, Metadata: meta})
			} else {
				notFound = append(notFound, id)
			}
		}
		remaining = notFound
	}
	return out, nil
}

func (f *DuckDBArtifactFormat) QueryRag(ctx context.Context, req kb.RagQueryRequest) ([]kb.ExpandedResult, error) {
	if err := kb.ValidateRagQueryRequest(req); err != nil {
		return nil, err
	}

	results, err := f.searchTopK(ctx, req.KBID, req.QueryVec, req.Options.TopK, req.Options.Filter)
	if err != nil {
		return nil, err
	}
	expanded := kb.ExpandedFromVector(results)
	return filterExpandedByMaxDistance(expanded, req.Options.MaxDistance), nil
}

func (f *DuckDBArtifactFormat) QueryBM25(ctx context.Context, req kb.BM25QueryRequest) ([]kb.ExpandedResult, error) {
	if strings.TrimSpace(req.KBID) == "" {
		return nil, fmt.Errorf("kb_id is required")
	}
	if strings.TrimSpace(req.QueryText) == "" {
		return nil, fmt.Errorf("query text is required")
	}
	results, err := f.searchBM25AllShards(ctx, req.KBID, req.QueryText, req.Options.TopK, req.Options.Filter)
	if err != nil {
		return nil, err
	}
	expanded := kb.ExpandedFromVector(results)
	return filterExpandedByMaxDistance(expanded, req.Options.MaxDistance), nil
}

func (f *DuckDBArtifactFormat) searchBM25AllShards(ctx context.Context, kbID, queryText string, k int, filter *search.FilterExpr) ([]kb.QueryResult, error) {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return nil, kb.ErrKBUninitialized
		}
		return nil, err
	}
	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return nil, err
	}
	if len(manifest.Shards) == 0 {
		return nil, kb.ErrKBUninitialized
	}

	allResults := make([]kb.QueryResult, 0)
	for _, shard := range manifest.Shards {
		conn, err := f.openCachedShardConn(ctx, kbID, shard)
		if err != nil {
			return nil, fmt.Errorf("open shard %s for bm25: %w", shard.ShardID, err)
		}
		rows, err := queryBM25WithDB(ctx, conn.db, queryText, k, filter)
		conn.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("bm25 shard %s: %w", shard.ShardID, err)
		}
		allResults = append(allResults, rows...)
	}
	return deduplicateBM25Results(allResults, k), nil
}

func deduplicateBM25Results(results []kb.QueryResult, k int) []kb.QueryResult {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Distance != results[j].Distance {
			return results[i].Distance > results[j].Distance
		}
		return results[i].ID < results[j].ID
	})
	seen := make(map[string]struct{}, len(results))
	deduped := make([]kb.QueryResult, 0, len(results))
	for _, r := range results {
		if _, ok := seen[r.ID]; !ok {
			seen[r.ID] = struct{}{}
			deduped = append(deduped, r)
		}
	}
	if k > 0 && len(deduped) > k {
		return deduped[:k]
	}
	return deduped
}

func (f *DuckDBArtifactFormat) QueryGraph(ctx context.Context, req kb.GraphQueryRequest) ([]kb.ExpandedResult, error) {
	if err := kb.ValidateGraphQueryRequest(req); err != nil {
		return nil, err
	}

	options := kb.NormalizeExpansionOptions(req.Options.TopK, req.Options.Expansion)
	options.OfflineExt = f.deps.OfflineExt
	selection, err := f.resolveVectorQuerySelection(ctx, req.KBID, req.QueryVec)
	if err != nil {
		return nil, fmt.Errorf("select vector query path: %w", err)
	}
	if err := ensureGraphSelectionAvailable(selection); err != nil {
		return nil, err
	}
	if err := validateQueryVectorDimensionForShards(req.QueryVec, selection.Plan.Shards); err != nil {
		return nil, err
	}

	merged, err := f.runGraphExpansionAcrossShards(ctx, req, selection, options)
	if err != nil {
		return nil, err
	}
	results := filterExpandedByMaxDistance(merged, req.Options.MaxDistance)
	return filterExpandedByPredicate(results, req.Options.Filter), nil
}

func (f *DuckDBArtifactFormat) runGraphExpansionAcrossShards(
	ctx context.Context,
	req kb.GraphQueryRequest,
	selection *vectorQuerySelection,
	options kb.ExpansionOptions,
) ([]kb.ExpandedResult, error) {
	parallelism := selection.Plan.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]kb.ExpandedResult, len(selection.Plan.Shards))
	errCh := make(chan error, 1)
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	graphShardCount := 0
	for i := range selection.Plan.Shards {
		idx := i
		shard := selection.Plan.Shards[i]
		if !shard.GraphAvailable {
			continue
		}
		graphShardCount++
		f.startGraphShardQuery(
			ctx,
			req,
			shardGraphQueryWork{
				idx:     idx,
				shard:   shard,
				options: options,
				sem:     sem,
				errCh:   errCh,
				results: results,
				cancel:  cancel,
				wg:      &wg,
			},
		)
	}
	if graphShardCount == 0 {
		return nil, kb.ErrGraphQueryUnavailable
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	return mergeExpandedShardResults(results, req.Options.TopK), nil
}

type shardGraphQueryWork struct {
	idx     int
	shard   kb.SnapshotShardMetadata
	options kb.ExpansionOptions
	sem     chan struct{}
	errCh   chan error
	results [][]kb.ExpandedResult
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
}

func (f *DuckDBArtifactFormat) startGraphShardQuery(
	ctx context.Context,
	req kb.GraphQueryRequest,
	work shardGraphQueryWork,
) {
	work.wg.Add(1)
	go f.runGraphShardQuery(ctx, req, work)
}

func (f *DuckDBArtifactFormat) runGraphShardQuery(
	ctx context.Context,
	req kb.GraphQueryRequest,
	work shardGraphQueryWork,
) {
	defer work.wg.Done()
	select {
	case work.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-work.sem }()
	shardResults, err := f.queryGraphSingleShard(ctx, req, work.shard, work.options)
	if err != nil {
		select {
		case work.errCh <- err:
		default:
		}
		work.cancel()
		return
	}
	work.results[work.idx] = shardResults
}

func (f *DuckDBArtifactFormat) queryGraphSingleShard(
	ctx context.Context,
	req kb.GraphQueryRequest,
	shard kb.SnapshotShardMetadata,
	options kb.ExpansionOptions,
) ([]kb.ExpandedResult, error) {
	localPath, _, err := f.ensureLocalShardFile(ctx, req.KBID, shard)
	if err != nil {
		return nil, err
	}
	conn, err := f.pool.GetOrOpen(ctx, localPath, f.openConfiguredDB)
	if err != nil {
		return nil, err
	}
	defer conn.mu.Unlock()

	return searchExpandedWithDB(ctx, conn.db, req.QueryVec, req.Options.TopK, options)
}

func filterExpandedByMaxDistance(results []kb.ExpandedResult, maxDistance *float64) []kb.ExpandedResult {
	if maxDistance == nil {
		return results
	}
	cutoff := *maxDistance
	filtered := make([]kb.ExpandedResult, 0, len(results))
	for _, r := range results {
		if r.Distance <= cutoff {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func filterExpandedByPredicate(results []kb.ExpandedResult, filter *search.FilterExpr) []kb.ExpandedResult {
	if filter == nil {
		return results
	}
	out := make([]kb.ExpandedResult, 0, len(results))
	for _, r := range results {
		if filter.Match(r.Metadata) {
			out = append(out, r)
		}
	}
	return out
}

func ensureGraphSelectionAvailable(selection *vectorQuerySelection) error {
	if selection == nil || len(selection.Plan.Shards) == 0 {
		return kb.ErrGraphQueryUnavailable
	}
	for _, shard := range selection.Plan.Shards {
		if shard.GraphAvailable {
			return nil
		}
	}
	return kb.ErrGraphQueryUnavailable
}

func (f *DuckDBArtifactFormat) ensureGraphModeAvailable(ctx context.Context, kbID string) error {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return kb.ErrKBUninitialized
		}
		return fmt.Errorf("download shard manifest: %w", err)
	}
	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return err
	}
	if len(manifest.Shards) == 0 {
		return kb.ErrGraphQueryUnavailable
	}
	hasGraphShard := false
	for _, shard := range manifest.Shards {
		if shard.GraphAvailable {
			hasGraphShard = true
			break
		}
	}
	if !hasGraphShard {
		return kb.ErrGraphQueryUnavailable
	}

	return nil
}
