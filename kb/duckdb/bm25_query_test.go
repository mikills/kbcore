package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestBM25Search(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-bm25-test"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	docs := []kb.Document{
		{ID: "doc-go", Text: "golang programming language concurrency goroutines"},
		{ID: "doc-py", Text: "python programming language machine learning numpy"},
		{ID: "doc-rs", Text: "rust programming language memory safety ownership"},
		{ID: "doc-js", Text: "javascript typescript frontend web development"},
	}
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))

	t.Run("bm25_returns_ranked_results", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
			Mode:      kb.SearchModeBM25,
			TopK:      10,
			QueryText: "golang concurrency",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, results)
		ids := make([]string, len(results))
		for i, r := range results {
			ids[i] = r.ID
		}
		assert.Contains(t, ids, "doc-go", "golang doc should rank for 'golang concurrency'")
	})

	t.Run("bm25_top_k_limits_results", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
			Mode:      kb.SearchModeBM25,
			TopK:      2,
			QueryText: "programming language",
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 2)
	})

	t.Run("hybrid_returns_combined_results", func(t *testing.T) {
		vec, err := loader.Embed(ctx, "programming language")
		require.NoError(t, err)
		results, err := loader.Search(ctx, kbID, vec, &kb.SearchOptions{
			Mode:      kb.SearchModeHybrid,
			TopK:      4,
			QueryText: "golang concurrency goroutines",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, results)
		ids := make([]string, len(results))
		for i, r := range results {
			ids[i] = r.ID
		}
		assert.Contains(t, ids, "doc-go")
	})

	t.Run("bm25_missing_query_text_errors", func(t *testing.T) {
		_, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
			Mode:  kb.SearchModeBM25,
			TopK:  5,
		})
		require.Error(t, err)
	})
}

func TestMergeHybridRRF(t *testing.T) {
	vec := []kb.QueryResult{
		{ID: "a", Distance: 0.1},
		{ID: "b", Distance: 0.2},
		{ID: "c", Distance: 0.3},
	}
	bm25 := []kb.QueryResult{
		{ID: "c", Distance: 0.9},
		{ID: "d", Distance: 0.8},
		{ID: "a", Distance: 0.7},
	}

	merged := kb.MergeHybridRRF(vec, bm25, 4)
	require.Len(t, merged, 4)

	ids := make([]string, len(merged))
	for i, r := range merged {
		ids[i] = r.ID
	}
	// "a" appears in both lists at rank 0 and 2 — should score highest via RRF
	assert.Equal(t, "a", ids[0], "a ranks in both lists, should be first")

	t.Run("respects_topk", func(t *testing.T) {
		result := kb.MergeHybridRRF(vec, bm25, 2)
		assert.Len(t, result, 2)
	})

	t.Run("deduplicates", func(t *testing.T) {
		seen := make(map[string]int)
		for _, r := range merged {
			seen[r.ID]++
		}
		for id, count := range seen {
			assert.Equal(t, 1, count, "duplicate id %q in merged results", id)
		}
	})
}
