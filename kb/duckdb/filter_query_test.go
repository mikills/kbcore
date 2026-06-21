package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
)

func TestMetadataFilterQuery(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-filter-test"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	docs := []kb.Document{
		{ID: "doc-a", Text: "alpha content", Metadata: map[string]any{"tenant": "acme", "rank": float64(1)}},
		{ID: "doc-b", Text: "beta content", Metadata: map[string]any{"tenant": "acme", "rank": float64(5)}},
		{ID: "doc-c", Text: "gamma content", Metadata: map[string]any{"tenant": "globex", "rank": float64(3)}},
		{ID: "doc-d", Text: "delta content", Metadata: map[string]any{"tenant": "globex", "rank": float64(7)}},
		{ID: "doc-e", Text: "epsilon content"},
	}
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))

	queryVec, err := loader.Embed(ctx, "content")
	require.NoError(t, err)

	t.Run("eq_string_filter", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{
			TopK:   10,
			Filter: &search.FilterExpr{Field: "tenant", Op: search.FilterOpEq, Value: "acme"},
		})
		require.NoError(t, err)
		ids := resultIDs(results)
		assert.Contains(t, ids, "doc-a")
		assert.Contains(t, ids, "doc-b")
		assert.NotContains(t, ids, "doc-c")
		assert.NotContains(t, ids, "doc-d")
		assert.NotContains(t, ids, "doc-e")
	})

	t.Run("gt_number_filter", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{
			TopK:   10,
			Filter: &search.FilterExpr{Field: "rank", Op: search.FilterOpGt, Value: float64(3)},
		})
		require.NoError(t, err)
		ids := resultIDs(results)
		assert.Contains(t, ids, "doc-b")
		assert.Contains(t, ids, "doc-d")
		assert.NotContains(t, ids, "doc-a")
		assert.NotContains(t, ids, "doc-c")
	})

	t.Run("and_compound_filter", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{
			TopK: 10,
			Filter: &search.FilterExpr{And: []search.FilterExpr{
				{Field: "tenant", Op: search.FilterOpEq, Value: "acme"},
				{Field: "rank", Op: search.FilterOpGt, Value: float64(3)},
			}},
		})
		require.NoError(t, err)
		ids := resultIDs(results)
		assert.Equal(t, []string{"doc-b"}, ids)
	})

	t.Run("in_filter", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{
			TopK: 10,
			Filter: &search.FilterExpr{
				Field: "tenant", Op: search.FilterOpIn, Value: []any{"acme", "globex"},
			},
		})
		require.NoError(t, err)
		ids := resultIDs(results)
		assert.Len(t, ids, 4)
		assert.NotContains(t, ids, "doc-e")
	})

	t.Run("no_match_returns_empty", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{
			TopK:   10,
			Filter: &search.FilterExpr{Field: "tenant", Op: search.FilterOpEq, Value: "nobody"},
		})
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("nil_filter_returns_all", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{TopK: 10})
		require.NoError(t, err)
		assert.Len(t, results, 5)
	})

	t.Run("invalid_filter_returns_error", func(t *testing.T) {
		_, err := loader.Search(ctx, kbID, queryVec, &kb.SearchOptions{
			TopK:   10,
			Filter: &search.FilterExpr{Field: "bad-field", Op: search.FilterOpEq, Value: "x"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid character")
	})
}

func resultIDs(results []kb.ExpandedResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	return ids
}
