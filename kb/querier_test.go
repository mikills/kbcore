package kb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestKB creates a test KB and returns loader and kbID using TestHarness
func setupTestKB(t *testing.T) (*KB, string) {
	t.Helper()
	kbID := "kb-test"
	embedder := newDemoEmbedder()
	harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
	t.Cleanup(harness.Cleanup)
	if err := buildDemoKB(kbID, harness.BlobRoot(), embedder); err != nil {
		require.FailNowf(t, "test failed", "failed to build test KB: %v", err)
	}
	return harness.KB(), kbID
}

// setupGraphKBForTest builds a graph KB with the given edges and returns a loader
// and query vector embedded from "doc-a".
func setupGraphKBForTest(t *testing.T, kbID string, edges []graphEdge) (*KB, []float32) {
	t.Helper()
	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
	t.Cleanup(harness.Cleanup)
	require.NoError(t, buildGraphKB(kbID, harness.BlobRoot(), embedder, edges))
	queryVec, err := embedder.Embed(context.Background(), "doc-a")
	require.NoError(t, err)
	return harness.KB(), queryVec
}

// requireResultContainsID asserts that at least one result has the given ID.
func requireResultContainsID(t *testing.T, results []ExpandedResult, id string) {
	t.Helper()
	for _, r := range results {
		if r.ID == id {
			return
		}
	}
	require.Failf(t, "assertion failed", "expected results to contain id=%q, got %+v", id, results)
}

// setupEmptyBlobStore creates a loader with no KBs (for non-existent KB tests)
func setupEmptyBlobStore(t *testing.T) *KB {
	t.Helper()
	harness := NewTestHarness(t, "empty").Setup()
	t.Cleanup(harness.Cleanup)
	return harness.KB()
}

func uploadTestShardManifest(t *testing.T, loader *KB, kbID string, shardCount int) {
	t.Helper()

	graphAvailability := make([]bool, shardCount)
	uploadTestShardManifestWithGraphAvailability(t, loader, kbID, graphAvailability)
}

func uploadTestShardManifestWithGraphAvailability(t *testing.T, loader *KB, kbID string, graphAvailability []bool) {
	t.Helper()

	shardCount := len(graphAvailability)
	manifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         shardManifestLayoutDuckDBs,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: int64(shardCount),
		Shards:         make([]SnapshotShardMetadata, 0, shardCount),
	}
	for i := 0; i < shardCount; i++ {
		manifest.Shards = append(manifest.Shards, SnapshotShardMetadata{
			ShardID:        fmt.Sprintf("shard-%03d", i),
			Key:            fmt.Sprintf("%s/shard-%03d.duckdb", kbID, i),
			SizeBytes:      1,
			VectorRows:     1,
			CreatedAt:      time.Now().UTC(),
			GraphAvailable: graphAvailability[i],
		})
	}

	data, err := json.Marshal(manifest)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = loader.BlobStore.UploadIfMatch(context.Background(), shardManifestKey(kbID), path, "")
	require.NoError(t, err)
}

type graphEdge struct {
	src     string
	dst     string
	weight  float64
	relType string
}

func buildGraphKB(kbID, blobRoot string, embedder Embedder, edges []graphEdge) error {
	if embedder == nil {
		return fmt.Errorf("embedder is required")
	}

	buildDir := filepath.Join(blobRoot, "build", kbID)
	if err := os.MkdirAll(buildDir, 0o755); err != nil {
		return err
	}

	dbPath := filepath.Join(buildDir, "vectors.duckdb")
	db, err := sql.Open("duckdb", dbPath+"?access_mode=read_write")
	if err != nil {
		return err
	}
	defer db.Close()

	if err := testLoadVSS(db); err != nil {
		return err
	}
	if _, err := db.Exec(`SET hnsw_enable_experimental_persistence = true`); err != nil {
		return fmt.Errorf("failed to enable hnsw persistence: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[3]
		)
	`); err != nil {
		return fmt.Errorf("failed to create docs table: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE edges (
			src TEXT,
			dst TEXT,
			weight DOUBLE,
			rel_type TEXT,
			chunk_id TEXT
		)
	`); err != nil {
		return fmt.Errorf("failed to create edges table: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE entities (
			id TEXT PRIMARY KEY,
			name TEXT
		)
	`); err != nil {
		return fmt.Errorf("failed to create entities table: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE doc_entities (
			doc_id TEXT,
			entity_id TEXT,
			weight DOUBLE,
			chunk_id TEXT
		)
	`); err != nil {
		return fmt.Errorf("failed to create doc_entities table: %w", err)
	}

	docs := []struct {
		id      string
		content string
	}{
		{"a", "doc-a"},
		{"b", "doc-b"},
		{"c", "doc-c"},
	}
	entities := []struct {
		id   string
		name string
	}{
		{"ent-a", "Entity A"},
		{"ent-b", "Entity B"},
		{"ent-c", "Entity C"},
	}

	embeddings := make([][]float32, 0, len(docs))
	for _, doc := range docs {
		vec, err := embedder.Embed(context.Background(), doc.content)
		if err != nil {
			return fmt.Errorf("embed %s: %w", doc.id, err)
		}
		if len(vec) != 3 {
			return fmt.Errorf("expected 3-dimensional embedding for %s, got %d", doc.id, len(vec))
		}
		embeddings = append(embeddings, vec)
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO docs VALUES
			('%s', '%s', %s),
			('%s', '%s', %s),
			('%s', '%s', %s)
	`,
		docs[0].id, docs[0].content, formatVectorForSQL(embeddings[0]),
		docs[1].id, docs[1].content, formatVectorForSQL(embeddings[1]),
		docs[2].id, docs[2].content, formatVectorForSQL(embeddings[2]),
	)
	if _, err := db.Exec(insertSQL); err != nil {
		return fmt.Errorf("failed to insert docs: %w", err)
	}

	if _, err := db.Exec(`CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return fmt.Errorf("failed to create vss index: %w", err)
	}

	for _, ent := range entities {
		if _, err := db.Exec(
			`INSERT INTO entities (id, name) VALUES (?, ?)`,
			ent.id, ent.name,
		); err != nil {
			return fmt.Errorf("failed to insert entity %s: %w", ent.id, err)
		}
	}

	for i, doc := range docs {
		ent := entities[i]
		if _, err := db.Exec(
			`INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id) VALUES (?, ?, ?, ?)`,
			doc.id, ent.id, 1.0, doc.id,
		); err != nil {
			return fmt.Errorf("failed to insert doc_entity %s->%s: %w", doc.id, ent.id, err)
		}
	}

	for _, edge := range edges {
		chunkID := strings.TrimPrefix(edge.src, "ent-")
		if _, err := db.Exec(
			`INSERT INTO edges (src, dst, weight, rel_type, chunk_id) VALUES (?, ?, ?, ?, ?)`,
			edge.src, edge.dst, edge.weight, edge.relType, chunkID,
		); err != nil {
			return fmt.Errorf("failed to insert edge %s->%s: %w", edge.src, edge.dst, err)
		}
	}

	// Close DB to checkpoint WAL before publishing shard artifacts.
	db.Close()
	publisher := NewKB(
		&LocalBlobStore{Root: blobRoot},
		filepath.Join(buildDir, "publisher-cache"),
		WithMemoryLimit("128MB"),
		WithEmbedder(embedder),
	)
	if _, err := publisher.UploadSnapshotShardedIfMatch(context.Background(), kbID, dbPath, "", defaultSnapshotShardSize); err != nil {
		return fmt.Errorf("publish sharded graph snapshot: %w", err)
	}
	return nil
}

func TestKBVectorQuery(t *testing.T) {
	t.Run("topk_cases", testKBTopKCases)
	t.Run("search_by_distance_cases", testKBSearchByDistanceCases)
	t.Run("result_ordering", testKBResultOrdering)
	t.Run("search_shard_path", testKBSearchShardPath)
	t.Run("topk_shard_execution_modes", testKBTopKShardExecutionModes)
	t.Run("shard_execution_fallback", testKBShardExecutionFallback)
	t.Run("topk_shard_fanout_plan", testKBTopKShardFanoutPlan)
	t.Run("topk_shard_merge", testKBTopKShardMerge)
}

func TestKBGraphQuery(t *testing.T) {
	t.Run("search_expanded_basic", testKBSearchExpandedBasic)
	t.Run("graph_mode_unavailable_scenarios", testKBGraphModeUnavailableScenarios)
	t.Run("search_expanded_invalid_args", testKBSearchExpandedInvalidArgs)
	t.Run("search_expanded_invalid_options_defaulted", testKBSearchExpandedInvalidOptionsDefaulted)
	t.Run("search_default_vector", testKBSearchDefaultVector)
	t.Run("search_graph_mode", testKBSearchGraphMode)
	t.Run("search_adaptive_mode", testKBSearchAdaptive)
	t.Run("search_expanded_bidirectional", testKBSearchExpandedBidirectional)
}

func testKBTopKCases(t *testing.T) {
	ctx := context.Background()
	type testCase struct {
		name       string
		setup      func(*testing.T) (*KB, string, []float32, int)
		wantErr    bool
		wantLen    int
		checkFirst string
	}

	tests := []testCase{
		{
			name: "basic",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{0.1, 0.2, 0.3}, 1
			},
			wantLen:    1,
			checkFirst: "a",
		},
		{
			name: "k_exceeds_total",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{0.1, 0.2, 0.3}, 100
			},
			wantLen: 2,
		},
		{
			name: "invalid_k_zero",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{0.1, 0.2, 0.3}, 0
			},
			wantLen: 0,
		},
		{
			name: "invalid_vector_empty",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{}, 1
			},
			wantErr: true,
		},
		{
			name: "invalid_vector_nil",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, nil, 1
			},
			wantErr: true,
		},
		{
			name: "non_existent_kb",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				return setupEmptyBlobStore(t), "non-existent-kb", []float32{0.1, 0.2, 0.3}, 1
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, vec, k := tc.setup(t)
			results, err := loader.TopK(ctx, kbID, vec, k)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, results, tc.wantLen)
			if tc.checkFirst != "" {
				assert.Equal(t, tc.checkFirst, results[0].ID)
				assert.GreaterOrEqual(t, results[0].Distance, 0.0)
			}
		})
	}
}

func testKBSearchByDistanceCases(t *testing.T) {
	ctx := context.Background()
	type testCase struct {
		name           string
		setup          func(*testing.T) (*KB, string, []float32, float64)
		wantErr        bool
		wantLen        int
		checkThreshold float64
		wantAtLeastOne bool
	}

	tests := []testCase{
		{
			name: "basic",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{0.1, 0.2, 0.3}, 10.0
			},
			checkThreshold: 10.0,
			wantAtLeastOne: true,
		},
		{
			name: "no_matches",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{1.0, 1.0, 1.0}, 0.0001
			},
			wantLen: 0,
		},
		{
			name: "invalid_threshold_zero",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{0.1, 0.2, 0.3}, 0
			},
			wantLen: 0,
		},
		{
			name: "invalid_vector",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{}, 1.0
			},
			wantErr: true,
		},
		{
			name: "non_existent_kb",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				return setupEmptyBlobStore(t), "non-existent-kb", []float32{0.1, 0.2, 0.3}, 1.0
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, vec, threshold := tc.setup(t)
			results, err := loader.SearchByDistance(ctx, kbID, vec, threshold)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantAtLeastOne {
				require.NotEmpty(t, results)
			} else {
				require.Len(t, results, tc.wantLen)
			}
			if tc.checkThreshold > 0 {
				for _, r := range results {
					assert.Less(t, r.Distance, tc.checkThreshold)
				}
			}
		})
	}
}

// test result ordering for both TopK and SearchByDistance
func testKBResultOrdering(t *testing.T) {
	ctx := context.Background()
	loader, kbID := setupTestKB(t)

	t.Run("TopK", func(t *testing.T) {
		results, err := loader.TopK(ctx, kbID, []float32{0.1, 0.2, 0.3}, 2)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 2, "need at least 2 results for ordering test")

		for i := 1; i < len(results); i++ {
			if results[i].Distance < results[i-1].Distance {
				assert.Failf(t, "assertion failed", "results not ordered: result[%d].Distance=%f < result[%d].Distance=%f",
					i, results[i].Distance, i-1, results[i-1].Distance)
			}
		}
	})

	t.Run("SearchByDistance", func(t *testing.T) {
		results, err := loader.SearchByDistance(ctx, kbID, []float32{0.1, 0.2, 0.3}, 10.0)
		require.NoError(t, err)

		if len(results) < 2 {
			t.Skip("need at least 2 results for ordering test")
		}

		for i := 1; i < len(results); i++ {
			if results[i].Distance < results[i-1].Distance {
				assert.Failf(t, "assertion failed", "results not ordered: result[%d].Distance=%f < result[%d].Distance=%f",
					i, results[i].Distance, i-1, results[i-1].Distance)
			}
		}
	})
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
		{name: "no_manifest", shardCount: 0, smallMax: 2, wantErr: ErrKBUninitialized},
		{name: "small_kb_fast_path", shardCount: 2, smallMax: 2, wantPath: vectorQueryPathSmallShardFast},
		{name: "fanout_path", shardCount: 3, smallMax: 2, wantPath: vectorQueryPathShardFanout},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			kbID := fmt.Sprintf("kb-search-shard-path-%s", tc.name)
			harness := NewTestHarness(t, kbID).
				WithOptions(WithShardingPolicy(ShardingPolicy{SmallKBMaxShards: tc.smallMax})).
				Setup()
			t.Cleanup(harness.Cleanup)

			if tc.shardCount > 0 {
				uploadTestShardManifest(t, harness.KB(), kbID, tc.shardCount)
			}

			gotPath, err := harness.KB().selectVectorQueryPath(ctx, kbID)
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
	makeDocs := func(n int) []Document {
		docs := make([]Document, 0, n)
		for i := 0; i < n; i++ {
			docs = append(docs, Document{
				ID:   fmt.Sprintf("doc-%02d", i),
				Text: fmt.Sprintf("document content %02d with stable text", i),
			})
		}
		return docs
	}

	tests := []struct {
		name         string
		setup        func(t *testing.T) (*KB, string, []float32, vectorQueryPath, []QueryResult)
		expectExec   bool
		checkMetrics bool
	}{
		{
			name: "small_shard_fast_exec",
			setup: func(t *testing.T) (*KB, string, []float32, vectorQueryPath, []QueryResult) {
				kbID := "kb-shard-exec-small"
				policy := ShardingPolicy{
					ShardTriggerVectorRows: 1,
					TargetShardBytes:       1024,
					SmallKBMaxShards:       64,
				}
				harness := NewTestHarness(t, kbID).
					WithEmbedder(newFixtureEmbedder(3)).
					WithOptions(WithShardingPolicy(policy)).
					Setup()
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
			setup: func(t *testing.T) (*KB, string, []float32, vectorQueryPath, []QueryResult) {
				kbID := "kb-shard-exec-fanout"
				policy := ShardingPolicy{
					ShardTriggerVectorRows: 1,
					TargetShardBytes:       1024,
					SmallKBMaxShards:       1,
					QueryShardFanout:       4,
				}
				harness := NewTestHarness(t, kbID).
					WithEmbedder(newFixtureEmbedder(3)).
					WithOptions(WithShardingPolicy(policy)).
					Setup()
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, queryVec, wantPath, baseline := tc.setup(t)

			path, err := loader.selectVectorQueryPath(ctx, kbID)
			require.NoError(t, err)
			assert.Equal(t, wantPath, path)

			results, err := loader.TopK(ctx, kbID, queryVec, 5)
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
					assert.Contains(t, metricsText, fmt.Sprintf("kbcore_query_shard_exec_total{kb_id=\"%s\"}", kbID))
				} else {
					assert.NotContains(t, metricsText, fmt.Sprintf("kbcore_query_shard_exec_total{kb_id=\"%s\"}", kbID))
				}
			}
		})
	}
}

func testKBShardExecutionFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name string
		run  func(*KB, context.Context, string, []float32) ([]QueryResult, error)
	}{
		{
			name: "top_k",
			run: func(loader *KB, runCtx context.Context, kbID string, queryVec []float32) ([]QueryResult, error) {
				return loader.TopK(runCtx, kbID, queryVec, 3)
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			kbID := "kb-shard-fallback-" + tc.name
			policy := ShardingPolicy{
				ShardTriggerVectorRows: 1,
				TargetShardBytes:       1024,
				SmallKBMaxShards:       1,
			}
			harness := NewTestHarness(t, kbID).
				WithEmbedder(newFixtureEmbedder(3)).
				WithOptions(WithShardingPolicy(policy)).
				Setup()
			t.Cleanup(harness.Cleanup)

			docs := []Document{
				{ID: "doc-a", Text: "alpha text"},
				{ID: "doc-b", Text: "beta text"},
				{ID: "doc-c", Text: "gamma text"},
				{ID: "doc-d", Text: "delta text"},
			}
			require.NoError(t, harness.KB().UpsertDocsAndUpload(ctx, kbID, docs))

			doc, err := harness.KB().ManifestStore.Get(ctx, kbID)
			require.NoError(t, err)
			require.NotEmpty(t, doc.Manifest.Shards)
			doc.Manifest.Shards[0].Key = kbID + "/missing-shard-file.duckdb"

			_, err = harness.KB().ManifestStore.UpsertIfMatch(ctx, kbID, doc.Manifest, doc.Version)
			require.NoError(t, err)

			queryVec, err := harness.KB().Embed(ctx, "alpha text")
			require.NoError(t, err)

			results, err := tc.run(harness.KB(), ctx, kbID, queryVec)
			require.Error(t, err)
			require.Empty(t, results)

			metricsText := harness.KB().ShardingOpenMetricsText()
			assert.Contains(t, metricsText, fmt.Sprintf("kbcore_query_shard_exec_failures_total{kb_id=\"%s\"} 1", kbID))
		})
	}
}

func testKBTopKShardFanoutPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		policy          ShardingPolicy
		shardCount      int
		shards          []SnapshotShardMetadata
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
			policy: ShardingPolicy{
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
			policy: ShardingPolicy{
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
			policy: ShardingPolicy{
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
			policy: ShardingPolicy{
				QueryShardFanout:            4,
				QueryShardFanoutAdaptiveMax: 6,
				QueryShardParallelism:       4,
			},
			shards: []SnapshotShardMetadata{
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
			policy: ShardingPolicy{
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			loader := &KB{ShardingPolicy: tc.policy}
			manifest := &SnapshotShardManifest{}
			if len(tc.shards) > 0 {
				manifest.Shards = append(manifest.Shards, tc.shards...)
			} else {
				manifest.Shards = make([]SnapshotShardMetadata, 0, tc.shardCount)
				for i := 0; i < tc.shardCount; i++ {
					manifest.Shards = append(manifest.Shards, SnapshotShardMetadata{
						ShardID:    fmt.Sprintf("shard-%03d", i),
						VectorRows: int64(100 - i),
					})
				}
			}

			plan := loader.planShardFanout(normalizeShardingPolicy(tc.policy), manifest, tc.queryVec)
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

func testKBTopKShardMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       [][]QueryResult
		k           int
		wantIDs     []string
		wantContent []string
	}{
		{
			name: "global_topk_by_distance",
			input: [][]QueryResult{
				{{ID: "s0-a", Content: "a", Distance: 0.10}, {ID: "s0-b", Content: "b", Distance: 0.40}},
				{{ID: "s1-a", Content: "c", Distance: 0.20}, {ID: "s1-b", Content: "d", Distance: 0.30}},
			},
			k:           3,
			wantIDs:     []string{"s0-a", "s1-a", "s1-b"},
			wantContent: []string{"a", "c", "d"},
		},
		{
			name: "deterministic_tie_breaking",
			input: [][]QueryResult{
				{{ID: "b-id", Content: "same", Distance: 0.50}, {ID: "c-id", Content: "same", Distance: 0.50}},
				{{ID: "a-id", Content: "same", Distance: 0.50}, {ID: "c-id", Content: "alt", Distance: 0.50}},
			},
			k:           4,
			wantIDs:     []string{"a-id", "b-id", "c-id", "c-id"},
			wantContent: []string{"same", "same", "alt", "same"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := mergeShardTopKResults(tc.input, tc.k)
			require.Len(t, got, len(tc.wantIDs))
			for i := range got {
				assert.Equal(t, tc.wantIDs[i], got[i].ID)
				assert.Equal(t, tc.wantContent[i], got[i].Content)
			}
		})
	}
}

func testKBSearchExpandedBasic(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-basic"
	loader, queryVec := setupGraphKBForTest(t, kbID, []graphEdge{
		{src: "ent-a", dst: "ent-c", weight: 1.0, relType: "related"},
	})

	results, err := loader.SearchExpanded(ctx, kbID, queryVec, 2, &ExpansionOptions{
		SeedK:               1,
		Hops:                1,
		MaxNeighborsPerNode: 25,
		Alpha:               0.7,
		Decay:               0.7,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	requireResultContainsID(t, results, "c")
}

func testKBGraphModeUnavailableScenarios(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		setup func(t *testing.T) (*KB, string, []float32)
	}{
		{
			name: "base_unavailable",
			setup: func(t *testing.T) (*KB, string, []float32) {
				loader, kbID := setupTestKB(t)
				return loader, kbID, []float32{0.1, 0.2, 0.3}
			},
		},
		{
			name: "shard_unavailable",
			setup: func(t *testing.T) (*KB, string, []float32) {
				loader, kbID := setupTestKB(t)
				uploadTestShardManifestWithGraphAvailability(t, loader, kbID, []bool{false, false})
				return loader, kbID, []float32{0.1, 0.2, 0.3}
			},
		},
		{
			name: "graph_kb_no_edges",
			setup: func(t *testing.T) (*KB, string, []float32) {
				kbID := "kb-search-graph-empty"
				embedder := newFixtureEmbedder(3)
				harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
				t.Cleanup(harness.Cleanup)
				require.NoError(t, buildGraphKB(kbID, harness.BlobRoot(), embedder, nil))
				queryVec, err := embedder.Embed(ctx, "doc-a")
				require.NoError(t, err)
				return harness.KB(), kbID, queryVec
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, queryVec := tc.setup(t)
			_, err := loader.Search(ctx, kbID, queryVec, 2, &SearchOptions{Mode: SearchModeGraph})
			require.Error(t, err)
			require.ErrorIs(t, err, ErrGraphQueryUnavailable)
		})
	}
}

func testKBSearchExpandedInvalidArgs(t *testing.T) {
	ctx := context.Background()
	loader, kbID := setupTestKB(t)

	tests := []struct {
		name string
		k    int
		vec  []float32
	}{
		{
			name: "invalid k",
			k:    0,
			vec:  []float32{0.1, 0.2, 0.3},
		},
		{
			name: "empty vector",
			k:    1,
			vec:  []float32{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loader.SearchExpanded(ctx, kbID, tc.vec, tc.k, nil)
			require.Error(t, err)
		})
	}
}

func testKBSearchExpandedInvalidOptionsDefaulted(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-invalid-opts"
	loader, queryVec := setupGraphKBForTest(t, kbID, []graphEdge{
		{src: "a", dst: "c", weight: 1.0, relType: "related"},
	})

	_, err := loader.SearchExpanded(ctx, kbID, queryVec, 2, &ExpansionOptions{
		SeedK:               -5,
		Hops:                -1,
		MaxNeighborsPerNode: -10,
		Alpha:               2.5,
		Decay:               -0.1,
	})
	require.NoError(t, err)
}

func testKBSearchDefaultVector(t *testing.T) {
	ctx := context.Background()
	loader, kbID := setupTestKB(t)

	vectorResults, err := loader.TopK(ctx, kbID, []float32{0.1, 0.2, 0.3}, 2)
	require.NoError(t, err)

	searchResults, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, 2, nil)
	require.NoError(t, err)

	require.Len(t, searchResults, len(vectorResults))
	for i := range vectorResults {
		assert.Equal(t, vectorResults[i].ID, searchResults[i].ID, "result %d", i)
	}
}

func testKBSearchGraphMode(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-search-graph"
	loader, queryVec := setupGraphKBForTest(t, kbID, []graphEdge{
		{src: "ent-a", dst: "ent-c", weight: 1.0, relType: "related"},
	})

	results, err := loader.Search(ctx, kbID, queryVec, 2, &SearchOptions{
		Mode: SearchModeGraph,
		Expansion: &ExpansionOptions{
			SeedK:               1,
			Hops:                1,
			MaxNeighborsPerNode: 25,
			Alpha:               0.7,
			Decay:               0.7,
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	requireResultContainsID(t, results, "c")
}

func testKBSearchAdaptive(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-search-adaptive"
	loader, queryVec := setupGraphKBForTest(t, kbID, []graphEdge{
		{src: "ent-a", dst: "ent-c", weight: 1.0, relType: "related"},
	})
	for i := range queryVec {
		queryVec[i] += 0.1
	}

	results, err := loader.Search(ctx, kbID, queryVec, 2, &SearchOptions{
		Mode:           SearchModeAdaptive,
		AdaptiveMinSim: 0.95,
		Expansion: &ExpansionOptions{
			SeedK:               1,
			Hops:                1,
			MaxNeighborsPerNode: 25,
			Alpha:               0.7,
			Decay:               0.7,
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	requireResultContainsID(t, results, "c")
}

func testKBSearchExpandedBidirectional(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-bidir"
	// Edge goes from ent-c → ent-a (reverse only).
	// Querying near doc-a should still reach doc-c via reverse traversal.
	loader, queryVec := setupGraphKBForTest(t, kbID, []graphEdge{
		{src: "ent-c", dst: "ent-a", weight: 1.0, relType: "related"},
	})

	results, err := loader.SearchExpanded(ctx, kbID, queryVec, 3, &ExpansionOptions{
		SeedK:               1,
		Hops:                1,
		MaxNeighborsPerNode: 25,
		Alpha:               0.7,
		Decay:               0.7,
	})
	require.NoError(t, err)
	requireResultContainsID(t, results, "c")
}
