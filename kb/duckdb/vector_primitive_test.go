package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
)

func TestVectorPrimitive(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-vec-primitive"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	vec := func(vals ...float32) []float32 { return vals }

	t.Run("upsert_pre_computed_vector", func(t *testing.T) {
		docs := []kb.Document{
			{ID: "v-a", Embedding: vec(1, 0, 0, 0, 0, 0, 0, 0), Metadata: map[string]any{"kind": "alpha"}},
			{ID: "v-b", Embedding: vec(0, 1, 0, 0, 0, 0, 0, 0), Metadata: map[string]any{"kind": "beta"}},
			{ID: "v-c", Embedding: vec(0, 0, 1, 0, 0, 0, 0, 0), Metadata: map[string]any{"kind": "gamma"}},
		}
		require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))

		results, err := loader.SearchRaw(ctx, kbID, vec(1, 0, 0, 0, 0, 0, 0, 0), 3, nil)
		require.NoError(t, err)
		require.NotEmpty(t, results)
		assert.Equal(t, "v-a", results[0].ID)
	})

	t.Run("text_and_vector_in_same_kb", func(t *testing.T) {
		kbID2 := "kb-mixed"
		harness2 := kb.NewTestHarness(t, kbID2).
			WithEmbedder(newFixtureEmbedder(8)).
			Setup()
		t.Cleanup(harness2.Cleanup)
		registerFormatOnHarness(t, harness2)
		loader2 := harness2.KB()

		require.NoError(t, loader2.UpsertDocsAndUpload(ctx, kbID2, []kb.Document{
			{ID: "text-doc", Text: "hello world"},
			{ID: "vec-doc", Embedding: vec(0, 0, 0, 0, 1, 0, 0, 0)},
		}))

		results, err := loader2.SearchRaw(ctx, kbID2, vec(0, 0, 0, 0, 1, 0, 0, 0), 2, nil)
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("vector_with_metadata_filter", func(t *testing.T) {
		results, err := loader.SearchRaw(ctx, kbID, vec(1, 0, 0, 0, 0, 0, 0, 0), 10,
			&search.FilterExpr{Field: "kind", Op: search.FilterOpEq, Value: "alpha"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "v-a", results[0].ID)
	})

	t.Run("requires_either_text_or_embedding", func(t *testing.T) {
		err := loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
			{ID: "bad-doc"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "text or embedding is required")
	})
}
