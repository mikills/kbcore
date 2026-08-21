package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/vectorplan"
)

// registerFormatOnHarness creates and registers a DuckDBArtifactFormat on the
// harness KB instance. Call this after harness.Setup().
func registerFormatOnHarness(t *testing.T, h *kb.TestHarness) {
	t.Helper()
	registerFormatReturning(t, h)
}

func registerFormatReturning(t *testing.T, h *kb.TestHarness) *DuckDBArtifactFormat {
	t.Helper()
	loader := h.KB()
	af, err := NewArtifactFormat(DuckDBArtifactDeps{
		BlobStore:      loader.BlobStore,
		ManifestStore:  loader.ManifestStore,
		CacheDir:       h.CacheDir(),
		MemoryLimit:    "128MB",
		ShardingPolicy: loader.ShardingPolicy,
		Embed:          loader.Embed,
		GraphBuilder:   func() *kb.GraphBuilder { return loader.GraphBuilder },
		EvictCacheIfNeeded: func(ctx context.Context, protectKBID string) error {
			return loader.EvictCacheIfNeeded(ctx, protectKBID)
		},
		LockFor: loader.LockFor,
		AcquireWriteLease: func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
			return loader.AcquireWriteLease(ctx, kbID)
		},
		EnqueueReplacedShardsForGC: loader.EnqueueReplacedShardsForGC,
		Metrics:                    loader,
	})
	require.NoError(t, err)
	require.NoError(t, loader.RegisterFormat(af))
	return af
}

// fixtureEmbedder produces deterministic, normalized embeddings derived from
// the FNV-64a hash of the input text.
type fixtureEmbedder struct {
	dim int
}

func newFixtureEmbedder(dim int) kb.Embedder {
	return fixtureEmbedder{dim: dim}
}

func (e fixtureEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}

	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(input))
	seed := int64(hasher.Sum64())

	rng := rand.New(rand.NewSource(seed))
	vec := make([]float32, e.dim)
	for i := range vec {
		vec[i] = float32(rng.NormFloat64())
	}

	return normalizeVec(vec), nil
}

func normalizeVec(vec []float32) []float32 {
	var sumSq float32
	for _, v := range vec {
		sumSq += v * v
	}
	norm := float32(math.Sqrt(float64(sumSq)))
	if norm == 0 {
		return vec
	}
	normalized := make([]float32, len(vec))
	for i, v := range vec {
		normalized[i] = v / norm
	}
	return normalized
}

// testDuckDBFormat creates a DuckDBArtifactFormat for unit tests that only
// exercise pure planning/merging logic and never touch a real DuckDB database.
func testDuckDBFormat(t *testing.T, policy kb.ShardingPolicy) *DuckDBArtifactFormat {
	t.Helper()
	bs := &kb.LocalBlobStore{Root: t.TempDir()}
	cacheDir := t.TempDir()
	ms := &kb.BlobManifestStore{Store: bs}
	af, err := NewArtifactFormat(DuckDBArtifactDeps{
		BlobStore:      bs,
		ManifestStore:  ms,
		CacheDir:       cacheDir,
		MemoryLimit:    "128MB",
		ShardingPolicy: policy,
		Embed: func(ctx context.Context, s string) ([]float32, error) {
			return nil, nil
		},
		GraphBuilder:       func() *kb.GraphBuilder { return nil },
		EvictCacheIfNeeded: func(ctx context.Context, s string) error { return nil },
		LockFor:            func(s string) *sync.Mutex { return &sync.Mutex{} },
		AcquireWriteLease:  noopAcquireWriteLease(),
		Metrics:            &noopMetrics{},
	})
	require.NoError(t, err)
	return af
}

// noopMetrics satisfies kb.ShardMetricsObserver for tests that do not
// inspect metrics output.
type noopMetrics struct{}

func (noopMetrics) RecordShardCount(string, int)        {}
func (noopMetrics) RecordShardExecutionFailure(string)  {}
func (noopMetrics) RecordShardFanout(string, int, bool) {}
func (noopMetrics) RecordShardExecution(string, int)    {}
func (noopMetrics) RecordShardCacheAccess(string, bool) {}

// noopAcquireWriteLease returns a no-op lease for tests that need a
// non-nil AcquireWriteLease to satisfy validate().
func noopAcquireWriteLease() func(context.Context, string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
	mgr := kb.NewInMemoryWriteLeaseManager()
	return func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
		lease, err := mgr.Acquire(ctx, kbID, 30*time.Second)
		return mgr, lease, err
	}
}

// uploadTestShardManifest writes a synthetic shard manifest into the blob
// store backing the given KB so that selectVectorQueryPath can resolve it.
func uploadTestShardManifest(t *testing.T, loader *kb.KB, kbID string, shardCount int) {
	t.Helper()

	manifest := kb.SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         kb.ShardManifestLayoutDuckDBs,
		FormatKind:     DuckDBFormatKind,
		FormatVersion:  DuckDBFormatVersion,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: int64(shardCount),
		Shards:         make([]kb.SnapshotShardMetadata, 0, shardCount),
	}
	for i := range shardCount {
		manifest.Shards = append(manifest.Shards, kb.SnapshotShardMetadata{
			ShardID:    fmt.Sprintf("shard-%03d", i),
			Key:        fmt.Sprintf("%s/shard-%03d.duckdb", kbID, i),
			SizeBytes:  1,
			VectorRows: 1,
			CreatedAt:  time.Now().UTC(),
		})
	}

	data, err := json.Marshal(manifest)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = loader.BlobStore.UploadIfMatch(context.Background(), kb.ShardManifestKey(kbID), path, "")
	require.NoError(t, err)
}

// requireDuckDBFormat extracts the *DuckDBArtifactFormat from a KB via the
// default format registry.
func requireDuckDBFormat(t *testing.T, loader *kb.KB) *DuckDBArtifactFormat {
	t.Helper()
	af := loader.DefaultFormat()
	format, ok := af.(*DuckDBArtifactFormat)
	require.True(t, ok, "expected *DuckDBArtifactFormat, got %T", af)
	return format
}

func TestShardVectorQuery(t *testing.T) {
	t.Run("search_shard_path", testKBSearchShardPath)
	t.Run("topk_shard_execution_modes", testKBTopKShardExecutionModes)
	t.Run("topk_shard_fanout_plan", testKBTopKShardFanoutPlan)
	t.Run("topk_shard_local_multiplier", testKBTopKShardLocalMultiplier)
	t.Run("topk_shard_merge", testKBTopKShardMerge)
	t.Run("returns_metadata", testKBQueryReturnsMetadata)
	t.Run("replaces_metadata_on_upsert", testKBUpsertReplacesMetadata)
	t.Run("invalid_metadata_fails", testKBInvalidMetadataFails)
	t.Run("migrates_fresh_local_cache_metadata_column", testKBMigratesFreshLocalCacheMetadataColumn)
}

func testKBQueryReturnsMetadata(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-query-metadata"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()
	docs := []kb.Document{{
		ID:   "doc-1",
		Text: "document with metadata",
		Metadata: map[string]any{
			"tag":    "alpha",
			"tenant": "t1",
			"rank":   float64(1),
		},
	}}
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))
	_, err := loader.ManifestStore.Get(ctx, kbID)
	require.NoError(t, err)
	af := requireDuckDBFormat(t, loader)
	dbPath := filepath.Join(harness.CacheDir(), kbID, "inspect.duckdb")
	_, err = af.DownloadSnapshotFromShards(ctx, kbID, dbPath)
	require.NoError(t, err)
	db, err := af.OpenConfiguredDB(ctx, dbPath)
	require.NoError(t, err)
	defer db.Close()
	var stored sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT metadata FROM docs WHERE id = 'doc-1'`).Scan(&stored))
	t.Logf("stored metadata raw: valid=%v value=%q", stored.Valid, stored.String)
	var inserted sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT metadata FROM docs WHERE id = 'doc-1'`).Scan(&inserted))
	require.True(t, inserted.Valid)
	vec, err := loader.Embed(ctx, docs[0].Text)
	require.NoError(t, err)
	results, err := loader.Search(ctx, kbID, vec, &kb.SearchOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "alpha", results[0].Metadata["tag"])
	require.Equal(t, "t1", results[0].Metadata["tenant"])
	require.Equal(t, float64(1), results[0].Metadata["rank"])
}

func testKBMigratesFreshLocalCacheMetadataColumn(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-migrate-fresh-cache-metadata"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{
		ID:   "seed",
		Text: "seed document",
	}}))
	manifestVersion, err := loader.ManifestStore.HeadVersion(ctx, kbID)
	require.NoError(t, err)
	require.NotEmpty(t, manifestVersion)

	af := requireDuckDBFormat(t, loader)
	kbDir := filepath.Join(harness.CacheDir(), kbID)
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	require.NoError(t, os.Remove(dbPath))
	db, err := af.OpenConfiguredDB(ctx, dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE docs (
		id TEXT,
		content TEXT,
		embedding FLOAT[8],
		media_refs TEXT
	)`)
	require.NoError(t, err)
	require.NoError(t, ensureDocTombstonesTable(ctx, db))
	require.NoError(t, EnsureGraphTables(ctx, db))
	require.NoError(t, CheckpointAndCloseDB(ctx, db, "close old fresh cache db"))
	require.NoError(t, writeLocalShardManifestVersion(localShardManifestVersionPath(kbDir), manifestVersion))

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{
		ID:       "doc-1",
		Text:     "document after metadata migration",
		Metadata: map[string]any{"tag": "migrated"},
	}}))
	vec, err := loader.Embed(ctx, "document after metadata migration")
	require.NoError(t, err)
	results, err := loader.Search(ctx, kbID, vec, &kb.SearchOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "migrated", results[0].Metadata["tag"])
}

func testKBUpsertReplacesMetadata(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-replace-metadata"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{
		ID:       "doc-1",
		Text:     "replace metadata doc",
		Metadata: map[string]any{"tag": "a"},
	}}))
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{
		ID:       "doc-1",
		Text:     "replace metadata doc",
		Metadata: map[string]any{"tag": "b", "state": "updated"},
	}}))
	vec, err := loader.Embed(ctx, "replace metadata doc")
	require.NoError(t, err)
	results, err := loader.Search(ctx, kbID, vec, &kb.SearchOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "b", results[0].Metadata["tag"])
	_, exists := results[0].Metadata["state"]
	require.True(t, exists)
}

func testKBInvalidMetadataFails(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-invalid-metadata"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()
	err := loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{
		ID:   "doc-1",
		Text: "bad metadata",
		Metadata: map[string]any{
			"bad": make(chan int),
		},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata")
}

func testKBSearchShardPath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name       string
		shardCount int
		smallMax   int
		wantPath   vectorQueryPath
		wantErr    error
	}{
		{name: "no_manifest", shardCount: 0, smallMax: 2, wantErr: kb.ErrKBUninitialized},
		{name: "small_kb_fast_path", shardCount: 2, smallMax: 2, wantPath: vectorQueryPathSmallShardFast},
		{name: "fanout_path", shardCount: 3, smallMax: 2, wantPath: vectorQueryPathShardFanout},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			kbID := fmt.Sprintf("kb-search-shard-path-%s", tc.name)
			harness := kb.NewTestHarness(t, kbID).
				WithOptions(kb.WithShardingPolicy(kb.ShardingPolicy{SmallKBMaxShards: tc.smallMax})).
				Setup()
			registerFormatOnHarness(t, harness)
			t.Cleanup(harness.Cleanup)

			if tc.shardCount > 0 {
				uploadTestShardManifest(t, harness.KB(), kbID, tc.shardCount)
			}

			gotPath, err := requireDuckDBFormat(t, harness.KB()).selectVectorQueryPath(ctx, kbID)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantPath, gotPath)
		})
	}
}

func testKBTopKShardExecutionModes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	makeDocs := func(n int) []kb.Document {
		docs := make([]kb.Document, 0, n)
		for i := range n {
			docs = append(docs, kb.Document{
				ID:   fmt.Sprintf("doc-%02d", i),
				Text: fmt.Sprintf("document content %02d with stable text", i),
			})
		}
		return docs
	}

	tests := []struct {
		name         string
		setup        func(t *testing.T) (*kb.KB, string, []float32, vectorQueryPath, []kb.QueryResult)
		expectExec   bool
		checkMetrics bool
	}{
		{
			name: "small_shard_fast_exec",
			setup: func(t *testing.T) (*kb.KB, string, []float32, vectorQueryPath, []kb.QueryResult) {
				kbID := "kb-shard-exec-small"
				policy := kb.ShardingPolicy{
					ShardTriggerVectorRows: 1,
					TargetShardBytes:       1024,
					SmallKBMaxShards:       64,
				}
				harness := kb.NewTestHarness(t, kbID).
					WithEmbedder(newFixtureEmbedder(3)).
					WithOptions(kb.WithShardingPolicy(policy)).
					Setup()
				registerFormatOnHarness(t, harness)
				t.Cleanup(harness.Cleanup)

				require.NoError(t, harness.KB().UpsertDocsAndUpload(ctx, kbID, makeDocs(12)))
				queryVec, err := harness.KB().Embed(ctx, "document content 01 with stable text")
				require.NoError(t, err)
				return harness.KB(), kbID, queryVec, vectorQueryPathSmallShardFast, nil
			},
			expectExec:   true,
			checkMetrics: true,
		},
		{
			name: "fanout_exec",
			setup: func(t *testing.T) (*kb.KB, string, []float32, vectorQueryPath, []kb.QueryResult) {
				kbID := "kb-shard-exec-fanout"
				policy := kb.ShardingPolicy{
					ShardTriggerVectorRows: 1,
					TargetShardBytes:       1024,
					SmallKBMaxShards:       1,
					QueryShardFanout:       4,
				}
				harness := kb.NewTestHarness(t, kbID).
					WithEmbedder(newFixtureEmbedder(3)).
					WithOptions(kb.WithShardingPolicy(policy)).
					Setup()
				registerFormatOnHarness(t, harness)
				t.Cleanup(harness.Cleanup)

				require.NoError(t, harness.KB().UpsertDocsAndUpload(ctx, kbID, makeDocs(12)))
				queryVec, err := harness.KB().Embed(ctx, "document content 01 with stable text")
				require.NoError(t, err)
				return harness.KB(), kbID, queryVec, vectorQueryPathShardFanout, nil
			},
			expectExec:   true,
			checkMetrics: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, queryVec, wantPath, baseline := tc.setup(t)

			path, err := requireDuckDBFormat(t, loader).selectVectorQueryPath(ctx, kbID)
			require.NoError(t, err)
			assert.Equal(t, wantPath, path)

			results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{TopK: 5})
			require.NoError(t, err)
			assert.NotEmpty(t, results)

			if len(baseline) > 0 {
				require.Len(t, results, len(baseline))
				for i := range baseline {
					assert.Equal(t, baseline[i].ID, results[i].ID)
					assert.Equal(t, baseline[i].Content, results[i].Content)
				}
			}

			if tc.checkMetrics {
				metricsText := loader.ShardingOpenMetricsText()
				if tc.expectExec {
					assert.Contains(t, metricsText, fmt.Sprintf("minnow_query_shard_exec_total{kb_id=\"%s\"}", kbID))
				} else {
					assert.NotContains(t, metricsText, fmt.Sprintf("minnow_query_shard_exec_total{kb_id=\"%s\"}", kbID))
				}
			}
		})
	}
}

func testKBTopKShardFanoutPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		policy          kb.ShardingPolicy
		shardCount      int
		shards          []kb.SnapshotShardMetadata
		queryVec        []float32
		wantFanout      int
		wantParallelism int
		wantCapped      bool
		assertExact     bool
		wantAtLeastOne  bool
		wantOrder       []string
	}{
		{
			name: "bounded_by_adaptive_cap",
			policy: kb.ShardingPolicy{
				QueryShardFanout:            10,
				QueryShardFanoutAdaptiveMax: 6,
				QueryShardParallelism:       9,
			},
			shardCount:      12,
			wantFanout:      6,
			wantParallelism: 6,
			wantCapped:      true,
			assertExact:     true,
		},
		{
			name: "lower_bound_safety_at_least_one_shard",
			policy: kb.ShardingPolicy{
				QueryShardFanout:            0,
				QueryShardFanoutAdaptiveMax: 0,
				QueryShardParallelism:       0,
			},
			shardCount:      3,
			wantFanout:      0,
			wantParallelism: 0,
			wantCapped:      false,
			assertExact:     false,
			wantAtLeastOne:  true,
		},
		{
			name: "bounded_by_available_shards",
			policy: kb.ShardingPolicy{
				QueryShardFanout:            4,
				QueryShardFanoutAdaptiveMax: 6,
				QueryShardParallelism:       4,
			},
			shardCount:      2,
			wantFanout:      2,
			wantParallelism: 2,
			wantCapped:      false,
			assertExact:     true,
		},
		{
			name: "centroid_ordering",
			policy: kb.ShardingPolicy{
				QueryShardFanout:            4,
				QueryShardFanoutAdaptiveMax: 6,
				QueryShardParallelism:       4,
			},
			shards: []kb.SnapshotShardMetadata{
				{ShardID: "s0", VectorRows: 10, Centroid: []float32{0.10, 0.10}},
				{ShardID: "s1", VectorRows: 10, Centroid: []float32{0.40, 0.40}},
				{ShardID: "s2", VectorRows: 10, Centroid: []float32{0.20, 0.20}},
				{ShardID: "s3", VectorRows: 10, Centroid: []float32{0.30, 0.30}},
				{ShardID: "s4", VectorRows: 10, Centroid: []float32{0.90, 0.90}},
				{ShardID: "s5", VectorRows: 10, Centroid: []float32{0.80, 0.80}},
			},
			queryVec:        []float32{0, 0},
			wantFanout:      4,
			wantParallelism: 4,
			wantCapped:      false,
			assertExact:     true,
			wantOrder:       []string{"s0", "s2", "s3", "s1"},
		},
		{
			name: "parallelism_bound",
			policy: kb.ShardingPolicy{
				QueryShardFanout:            4,
				QueryShardFanoutAdaptiveMax: 6,
				QueryShardParallelism:       2,
			},
			shardCount:      8,
			wantFanout:      4,
			wantParallelism: 2,
			wantCapped:      false,
			assertExact:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			manifest := &kb.SnapshotShardManifest{}
			if len(tc.shards) > 0 {
				manifest.Shards = append(manifest.Shards, tc.shards...)
			} else {
				manifest.Shards = make([]kb.SnapshotShardMetadata, 0, tc.shardCount)
				for i := 0; i < tc.shardCount; i++ {
					manifest.Shards = append(manifest.Shards, kb.SnapshotShardMetadata{
						ShardID:    fmt.Sprintf("shard-%03d", i),
						VectorRows: int64(100 - i),
					})
				}
			}

			plan := vectorplan.PlanShardFanout(kb.NormalizeShardingPolicy(tc.policy), manifest, tc.queryVec)
			if tc.assertExact {
				assert.Equal(t, tc.wantFanout, plan.Fanout)
				assert.Equal(t, tc.wantParallelism, plan.Parallelism)
			} else {
				assert.GreaterOrEqual(t, plan.Fanout, 1)
				assert.LessOrEqual(t, plan.Fanout, tc.shardCount)
				assert.GreaterOrEqual(t, plan.Parallelism, 1)
				assert.LessOrEqual(t, plan.Parallelism, plan.Fanout)
			}
			assert.Equal(t, tc.wantCapped, plan.Capped)
			if tc.assertExact {
				require.Len(t, plan.Shards, tc.wantFanout)
			} else {
				require.Len(t, plan.Shards, plan.Fanout)
			}
			if tc.wantAtLeastOne {
				assert.GreaterOrEqual(t, plan.Fanout, 1)
			}
			if len(tc.wantOrder) > 0 {
				gotOrder := make([]string, 0, len(plan.Shards))
				for _, s := range plan.Shards {
					gotOrder = append(gotOrder, s.ShardID)
				}
				assert.Equal(t, tc.wantOrder, gotOrder)
			}
		})
	}
}

func testKBTopKShardLocalMultiplier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		k      int
		policy kb.ShardingPolicy
		want   int
	}{
		{name: "default_multiplier", k: 10, policy: kb.ShardingPolicy{}, want: 20},
		{name: "custom_multiplier", k: 10, policy: kb.ShardingPolicy{QueryShardLocalTopKMult: 3}, want: 30},
		{name: "negative_k", k: -1, policy: kb.ShardingPolicy{QueryShardLocalTopKMult: 3}, want: 0},
		{name: "multiplier_floor", k: 10, policy: kb.ShardingPolicy{QueryShardLocalTopKMult: 0}, want: 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := vectorplan.LocalTopK(tc.k, kb.NormalizeShardingPolicy(tc.policy))
			assert.Equal(t, tc.want, got)
		})
	}
}

func testKBTopKShardMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       [][]kb.QueryResult
		k           int
		wantIDs     []string
		wantContent []string
	}{
		{
			name: "global_topk_by_distance",
			input: [][]kb.QueryResult{
				{{ID: "s0-a", Content: "a", Distance: 0.10}, {ID: "s0-b", Content: "b", Distance: 0.40}},
				{{ID: "s1-a", Content: "c", Distance: 0.20}, {ID: "s1-b", Content: "d", Distance: 0.30}},
			},
			k:           3,
			wantIDs:     []string{"s0-a", "s1-a", "s1-b"},
			wantContent: []string{"a", "c", "d"},
		},
		{
			name: "deterministic_tie_breaking",
			input: [][]kb.QueryResult{
				{{ID: "b-id", Content: "same", Distance: 0.50}, {ID: "c-id", Content: "same", Distance: 0.50}},
				{{ID: "a-id", Content: "same", Distance: 0.50}, {ID: "c-id", Content: "alt", Distance: 0.50}},
			},
			k:           4,
			wantIDs:     []string{"a-id", "b-id", "c-id", "c-id"},
			wantContent: []string{"same", "same", "alt", "same"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := vectorplan.MergeTopK(tc.input, tc.k)
			require.Len(t, got, len(tc.wantIDs))
			for i := range got {
				assert.Equal(t, tc.wantIDs[i], got[i].ID)
				assert.Equal(t, tc.wantContent[i], got[i].Content)
			}
		})
	}
}

// buildBenchShards creates a slice of SnapshotShardMetadata with centroids
// of the given dimension for use in ranking benchmarks.
func buildBenchShards(n, dim int) []kb.SnapshotShardMetadata {
	shards := make([]kb.SnapshotShardMetadata, n)
	for i := range shards {
		c := make([]float32, dim)
		for j := range c {
			c[j] = float32(i*dim+j) * 0.01
		}
		shards[i] = kb.SnapshotShardMetadata{
			ShardID:    "shard-%04d",
			Key:        "key-%04d",
			VectorRows: int64(100 + i),
			Centroid:   c,
		}
	}
	return shards
}

// buildBenchQueryVec returns a query vector of the given dimension.
func buildBenchQueryVec(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(i) * 0.001
	}
	return v
}

// BenchmarkRankShardsForQuery measures full ranking by centroid proximity.
func BenchmarkRankShardsForQuery(b *testing.B) {
	const dim = 64
	for _, n := range []int{4, 16, 64, 256, 1024} {
		shards := buildBenchShards(n, dim)
		qVec := buildBenchQueryVec(dim)
		b.Run(fmt.Sprintf("shards=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = vectorplan.RankShards(shards, qVec)
			}
		})
	}
}

func BenchmarkSelectTopShardsForQuery(b *testing.B) {
	const dim = 64
	const fanout = 6
	for _, n := range []int{16, 64, 256, 1024} {
		shards := buildBenchShards(n, dim)
		qVec := buildBenchQueryVec(dim)
		b.Run(fmt.Sprintf("shards=%d_fanout=%d", n, fanout), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = vectorplan.SelectTopShards(shards, qVec, fanout)
			}
		})
	}
}

// BenchmarkShardRankScore measures shardRankScore directly, which currently
// calls math.Sqrt even though ordering only needs squared distance.
func BenchmarkShardRankScore(b *testing.B) {
	const dim = 64
	shards := buildBenchShards(1, dim)
	qVec := buildBenchQueryVec(dim)
	b.ReportAllocs()
	for b.Loop() {
		_ = vectorplan.RankShards(shards, qVec)
	}
}

// BenchmarkMergeShardTopKResults measures result merging from multiple shards.
func BenchmarkMergeShardTopKResults(b *testing.B) {
	for _, shardCount := range []int{2, 4, 8} {
		for _, resultsPerShard := range []int{10, 100} {
			shardResults := make([][]kb.QueryResult, shardCount)
			for i := range shardResults {
				rows := make([]kb.QueryResult, resultsPerShard)
				for j := range rows {
					rows[j] = kb.QueryResult{
						ID:       "doc-%04d-%04d",
						Distance: float64(i*resultsPerShard+j) * 0.001,
					}
				}
				shardResults[i] = rows
			}
			seenK := map[int]struct{}{}
			for _, k := range []int{10, shardCount * resultsPerShard / 2} {
				if _, ok := seenK[k]; ok {
					continue
				}
				seenK[k] = struct{}{}
				b.Run(fmt.Sprintf("shards=%d_results=%d_k=%d", shardCount, resultsPerShard, k), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_ = vectorplan.MergeTopK(shardResults, k)
					}
				})
			}
		}
	}
}
