package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/shardcache"
	"github.com/mikills/minnow/kb/duckdb/internal/vectorplan"
	"github.com/mikills/minnow/kb/search"
)

var twoPhaseMaterializationEnabled = true

type vectorQueryPath int

const (
	vectorQueryPathSmallShardFast vectorQueryPath = iota
	vectorQueryPathShardFanout
)

type shardQueryPlan = vectorplan.QueryPlan

type vectorQuerySelection struct {
	Path vectorQueryPath
	Plan shardQueryPlan
}

func (f *DuckDBArtifactFormat) selectVectorQueryPath(ctx context.Context, kbID string) (vectorQueryPath, error) {
	selection, err := f.resolveVectorQuerySelection(ctx, kbID, nil, false)
	if err != nil {
		return vectorQueryPathShardFanout, err
	}
	return selection.Path, nil
}

func (f *DuckDBArtifactFormat) resolveVectorQuerySelection(
	ctx context.Context,
	kbID string,
	queryVec []float32,
	allShards bool,
) (*vectorQuerySelection, error) {
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
	f.deps.Metrics.RecordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return nil, kb.ErrKBUninitialized
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if allShards || len(manifest.Shards) <= policy.SmallKBMaxShards {
		return &vectorQuerySelection{
			Path: vectorQueryPathSmallShardFast,
			Plan: vectorplan.SmallShardPlan(policy, manifest.Shards),
		}, nil
	}

	plan := vectorplan.PlanShardFanout(policy, manifest, queryVec)
	if plan.Fanout <= 0 {
		return nil, kb.ErrKBUninitialized
	}
	return &vectorQuerySelection{Path: vectorQueryPathShardFanout, Plan: plan}, nil
}

func (f *DuckDBArtifactFormat) searchTopK(
	ctx context.Context,
	kbID string,
	queryVec []float32,
	k int,
	filter *search.FilterExpr,
	documentIDs []string,
) ([]kb.QueryResult, error) {
	if k <= 0 {
		return []kb.QueryResult{}, nil
	}
	if len(queryVec) == 0 {
		return nil, fmt.Errorf("query vector cannot be empty")
	}

	selection, err := f.resolveVectorQuerySelection(ctx, kbID, queryVec, documentIDs != nil)
	if err != nil {
		return nil, fmt.Errorf("select vector query path: %w", err)
	}
	if err := validateQueryVectorDimensionForShards(queryVec, selection.Plan.Shards); err != nil {
		return nil, err
	}
	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	localTopK := vectorplan.LocalTopK(k, policy)
	results, shardErr := f.queryTopKFromShards(
		ctx,
		shardTopKQuery{
			kbID: kbID, queryVec: queryVec, k: k, localTopK: localTopK,
			plan: selection.Plan, filter: filter, documentIDs: documentIDs,
		},
	)
	if shardErr != nil {
		f.deps.Metrics.RecordShardExecutionFailure(kbID)
		return nil, shardErr
	}
	return results, nil
}

type shardTopKQuery struct {
	kbID        string
	queryVec    []float32
	k           int
	localTopK   int
	plan        shardQueryPlan
	filter      *search.FilterExpr
	documentIDs []string
}

func (f *DuckDBArtifactFormat) queryTopKFromShards(
	ctx context.Context,
	query shardTopKQuery,
) ([]kb.QueryResult, error) {
	if query.plan.Fanout <= 0 || len(query.plan.Shards) == 0 {
		return []kb.QueryResult{}, nil
	}
	f.deps.Metrics.RecordShardFanout(query.kbID, query.plan.Fanout, query.plan.Capped)
	f.deps.Metrics.RecordShardExecution(query.kbID, query.plan.Fanout)

	workload := shardQueryWorkload{
		kbID:        query.kbID,
		shards:      query.plan.Shards,
		queryVec:    query.queryVec,
		k:           query.localTopK,
		parallelism: query.plan.Parallelism,
		filter:      query.filter,
		documentIDs: query.documentIDs,
	}
	if shouldUseTwoPhaseMaterialization(query.plan.Fanout, query.localTopK, query.k) {
		return f.queryTopKRefsFromShards(ctx, workload, query.k)
	}
	shardResults, err := f.queryShardsTopK(ctx, workload)
	if err != nil {
		return nil, err
	}
	return vectorplan.MergeTopK(shardResults, query.k), nil
}

type shardQueryWorkload struct {
	kbID        string
	shards      []kb.SnapshotShardMetadata
	queryVec    []float32
	k           int
	parallelism int
	filter      *search.FilterExpr
	documentIDs []string
}

func shouldUseTwoPhaseMaterialization(fanout int, localTopK int, finalTopK int) bool {
	return twoPhaseMaterializationEnabled && fanout > 1 && localTopK > finalTopK
}

func (f *DuckDBArtifactFormat) queryTopKRefsFromShards(
	ctx context.Context,
	workload shardQueryWorkload,
	finalTopK int,
) ([]kb.QueryResult, error) {
	shardRefs, err := f.queryShardsTopKRefs(ctx, workload)
	if err != nil {
		return nil, err
	}
	merged := mergeRankedShardRefs(shardRefs, finalTopK)
	refsByShard := make([][]rankedDocRef, len(workload.shards))
	for _, ref := range merged {
		refsByShard[ref.ShardIndex] = append(refsByShard[ref.ShardIndex], ref.Ref)
	}
	resultsByShard := make([][]kb.QueryResult, len(workload.shards))
	for shardIndex, refs := range refsByShard {
		if len(refs) == 0 {
			continue
		}
		results, err := f.hydrateShardRankedResults(ctx, workload.kbID, workload.shards[shardIndex], refs)
		if err != nil {
			return nil, err
		}
		resultsByShard[shardIndex] = results
	}
	out := make([]kb.QueryResult, 0, len(merged))
	for _, ref := range merged {
		if result, ok := findHydratedRankedResult(resultsByShard[ref.ShardIndex], ref.Ref.ID); ok {
			out = append(out, result)
		}
	}
	return out, nil
}

func findHydratedRankedResult(results []kb.QueryResult, id string) (kb.QueryResult, bool) {
	for _, result := range results {
		if result.ID == id {
			return result, true
		}
	}
	return kb.QueryResult{}, false
}

func (f *DuckDBArtifactFormat) queryShardsTopK(
	ctx context.Context,
	workload shardQueryWorkload,
) ([][]kb.QueryResult, error) {
	if len(workload.shards) == 0 || workload.k <= 0 {
		return [][]kb.QueryResult{}, nil
	}
	parallelism := workload.parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(workload.shards) {
		parallelism = len(workload.shards)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]kb.QueryResult, len(workload.shards))
	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := range workload.shards {
		f.startVectorShardQuery(
			ctx,
			vectorShardQueryWork{
				kbID:        workload.kbID,
				idx:         i,
				shard:       workload.shards[i],
				queryVec:    workload.queryVec,
				k:           workload.k,
				filter:      workload.filter,
				documentIDs: workload.documentIDs,
				sem:         sem,
				errCh:       errCh,
				results:     results,
				cancel:      cancel,
				wg:          &wg,
			},
		)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	return results, nil
}

type vectorShardQueryWork struct {
	kbID        string
	idx         int
	shard       kb.SnapshotShardMetadata
	queryVec    []float32
	k           int
	filter      *search.FilterExpr
	documentIDs []string
	sem         chan struct{}
	errCh       chan error
	results     [][]kb.QueryResult
	cancel      context.CancelFunc
	wg          *sync.WaitGroup
}

type vectorShardRefQueryWork struct {
	kbID        string
	idx         int
	shard       kb.SnapshotShardMetadata
	queryVec    []float32
	k           int
	filter      *search.FilterExpr
	documentIDs []string
	sem         chan struct{}
	errCh       chan error
	results     [][]rankedDocRef
	cancel      context.CancelFunc
	wg          *sync.WaitGroup
}

func (f *DuckDBArtifactFormat) startVectorShardQuery(ctx context.Context, work vectorShardQueryWork) {
	work.wg.Add(1)
	go f.runVectorShardQuery(ctx, work)
}

func (f *DuckDBArtifactFormat) runVectorShardQuery(ctx context.Context, work vectorShardQueryWork) {
	defer work.wg.Done()
	select {
	case work.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-work.sem }()
	rows, err := f.querySingleShardTopK(ctx, work.kbID, work.shard, shardQueryInput{
		queryVec: work.queryVec, k: work.k, filter: work.filter, documentIDs: work.documentIDs,
	})
	if err != nil {
		select {
		case work.errCh <- err:
		default:
		}
		work.cancel()
		return
	}
	work.results[work.idx] = rows
}

func (f *DuckDBArtifactFormat) queryShardsTopKRefs(
	ctx context.Context,
	workload shardQueryWorkload,
) ([][]rankedDocRef, error) {
	if len(workload.shards) == 0 || workload.k <= 0 {
		return [][]rankedDocRef{}, nil
	}
	parallelism := workload.parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(workload.shards) {
		parallelism = len(workload.shards)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([][]rankedDocRef, len(workload.shards))
	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for i := range workload.shards {
		f.startVectorShardRefQuery(ctx, vectorShardRefQueryWork{
			kbID: workload.kbID, idx: i, shard: workload.shards[i], queryVec: workload.queryVec,
			k: workload.k, filter: workload.filter, documentIDs: workload.documentIDs,
			sem: sem, errCh: errCh, results: results, cancel: cancel, wg: &wg,
		})
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	return results, nil
}

func (f *DuckDBArtifactFormat) startVectorShardRefQuery(ctx context.Context, work vectorShardRefQueryWork) {
	work.wg.Add(1)
	go f.runVectorShardRefQuery(ctx, work)
}

func (f *DuckDBArtifactFormat) runVectorShardRefQuery(ctx context.Context, work vectorShardRefQueryWork) {
	defer work.wg.Done()
	select {
	case work.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-work.sem }()
	rows, err := f.querySingleShardTopKRefs(ctx, work.kbID, work.shard, shardQueryInput{
		queryVec: work.queryVec, k: work.k, filter: work.filter, documentIDs: work.documentIDs,
	})
	if err != nil {
		select {
		case work.errCh <- err:
		default:
		}
		work.cancel()
		return
	}
	work.results[work.idx] = rows
}

// shardQueryInput carries the per-query parameters passed to each shard.
type shardQueryInput struct {
	queryVec    []float32
	k           int
	filter      *search.FilterExpr
	documentIDs []string
}

func (f *DuckDBArtifactFormat) querySingleShardTopK(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
	in shardQueryInput,
) ([]kb.QueryResult, error) {
	conn, err := f.openCachedShardConn(ctx, kbID, shard)
	if err != nil {
		return nil, err
	}
	defer conn.mu.Unlock()
	results, err := queryTopKWithDB(ctx, conn.db, in.queryVec, in.k, vectorQueryOpts{
		filter: in.filter, documentIDs: in.documentIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("query shard %s: %w", shard.ShardID, err)
	}
	return results, nil
}

func (f *DuckDBArtifactFormat) querySingleShardTopKRefs(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
	in shardQueryInput,
) ([]rankedDocRef, error) {
	conn, err := f.openCachedShardConn(ctx, kbID, shard)
	if err != nil {
		return nil, err
	}
	defer conn.mu.Unlock()
	refs, err := queryTopKRefsWithDB(ctx, conn.db, in.queryVec, in.k, vectorQueryOpts{
		filter: in.filter, documentIDs: in.documentIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("query shard refs %s: %w", shard.ShardID, err)
	}
	return refs, nil
}

func (f *DuckDBArtifactFormat) hydrateShardRankedResults(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
	refs []rankedDocRef,
) ([]kb.QueryResult, error) {
	conn, err := f.openCachedShardConn(ctx, kbID, shard)
	if err != nil {
		return nil, err
	}
	defer conn.mu.Unlock()
	results, err := hydrateRankedResults(ctx, conn.db, refs)
	if err != nil {
		return nil, fmt.Errorf("hydrate shard %s: %w", shard.ShardID, err)
	}
	return results, nil
}

func (f *DuckDBArtifactFormat) openCachedShardConn(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
) (*shardConn, error) {
	for ensureAttempts := 0; ; ensureAttempts++ {
		localPath, hit, err := f.ensureLocalShardFile(ctx, kbID, shard)
		if err != nil {
			// Cache eviction can remove the directory while a cold shard is being
			// materialized. Retry filesystem races; permanent blob/checksum errors
			// are returned immediately.
			if errors.Is(err, os.ErrNotExist) && ensureAttempts < 3 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}
			return nil, fmt.Errorf("ensure shard file %s: %w", shard.ShardID, err)
		}
		f.deps.Metrics.RecordShardCacheAccess(kbID, hit)
		conn, err := f.pool.GetOrOpen(ctx, localPath, func(ctx context.Context, path string) (*sql.DB, error) {
			if _, statErr := os.Stat(path); statErr != nil {
				return nil, statErr
			}
			return f.openConfiguredDB(ctx, path)
		})
		if !errors.Is(err, errShardConnPoolEvicting) && !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return nil, fmt.Errorf("open shard %s: %w", shard.ShardID, err)
			}
			return conn, nil
		}
		if err := f.pool.waitForEviction(ctx, localPath); err != nil {
			return nil, err
		}
		// The evictor may have removed the file after the first ensure. Loop so
		// the shard cache can materialize it again before opening DuckDB.
	}
}

func (f *DuckDBArtifactFormat) ensureLocalShardFile(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
) (string, bool, error) {
	return shardcache.Manager{
		CacheDir:           f.deps.CacheDir,
		BlobStore:          f.deps.BlobStore,
		EvictCacheIfNeeded: f.deps.EvictCacheIfNeeded,
		ReserveCache:       f.deps.ReserveCache,
	}.EnsureLocalFile(ctx, kbID, shard)
}
