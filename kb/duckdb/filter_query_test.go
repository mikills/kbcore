package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

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

func TestLargeScope(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ids := make([]string, 36_816)
	for i := range ids {
		ids[i] = fmt.Sprintf("sha256:%064d", i)
	}
	var count int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM unnest(?::VARCHAR[])`, documentScopeArgs(ids)...,
	).Scan(&count))
	require.Equal(t, len(ids), count)
	clause, err := buildWhereClause(nil, true)
	require.NoError(t, err)
	require.NotContains(t, clause, ids[0])
}

func TestScopedVectorScale(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`
		CREATE TABLE docs AS
		SELECT
			'doc-' || lpad(i::VARCHAR, 5, '0') AS id,
			''::VARCHAR AS content,
			list_resize([]::FLOAT[], 512, 0)::FLOAT[512] AS embedding,
			NULL::VARCHAR AS media_refs,
			NULL::VARCHAR AS metadata
		FROM range(36816) t(i)
	`)
	require.NoError(t, err)
	ids := make([]string, 36_816)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%05d", i)
	}
	query := make([]float32, 512)
	for i := range query {
		query[i] = 1
	}
	started := time.Now()
	results, err := queryTopKWithDB(
		context.Background(), db, query, 10,
		vectorQueryOpts{validateDimension: true, documentIDs: ids},
	)
	require.NoError(t, err)
	require.Len(t, results, 10)
	require.Less(t, time.Since(started), 2*time.Second)
}

func TestScopeQuery(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-scope-query"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	docs := make([]kb.Document, 0, 222)
	for i := range 221 {
		docs = append(docs, kb.Document{
			ID: fmt.Sprintf("other-%03d", i), Text: "other", Embedding: make([]float32, 8),
		})
	}
	docs = append(docs, kb.Document{
		ID: "scoped", Text: "scoped", Embedding: []float32{1, 1, 1, 1, 1, 1, 1, 1},
	})
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))
	_, err := loader.ReplaceScope(ctx, kbID, "branch", []string{"scoped"}, "")
	require.NoError(t, err)

	results, err := loader.Search(ctx, kbID, make([]float32, 8), &kb.SearchOptions{
		TopK: 1, ScopeID: "branch",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"scoped"}, resultIDs(results))
	require.ErrorIs(
		t, loader.DeleteDocsAndUpload(ctx, kbID, []string{"scoped"}, kb.DeleteDocsOptions{}),
		kb.ErrScopedDocuments,
	)
	scheduled, err := loader.ScheduleScopeGC(ctx, kbID, []string{"other-001"})
	require.NoError(t, err)
	require.Equal(t, []string{"other-001"}, scheduled)
	_, err = loader.ReplaceScope(ctx, kbID, "protect", []string{"other-001"}, "")
	require.NoError(t, err)
	count, err := loader.SweepScopeGC(ctx, time.Now().UTC().Add(kb.ScopeGCGrace))
	require.NoError(t, err)
	require.Zero(t, count)
	deleted, err := loader.ScheduleScopeGC(ctx, kbID, []string{"other-000", "scoped"})
	require.NoError(t, err)
	require.Equal(t, []string{"other-000", "scoped"}, deleted)
	count, err = loader.SweepScopeGC(ctx, time.Now().UTC().Add(kb.ScopeGCGrace))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	records, err := loader.FetchVectors(ctx, kbID, []string{"other-000", "scoped"})
	require.NoError(t, err)
	require.Equal(t, []string{"scoped"}, []string{records[0].ID})
	results, err = loader.Search(ctx, kbID, make([]float32, 8), &kb.SearchOptions{
		TopK: 1, ScopeID: "branch",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"scoped"}, resultIDs(results))
}

func resultIDs(results []kb.ExpandedResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	return ids
}
