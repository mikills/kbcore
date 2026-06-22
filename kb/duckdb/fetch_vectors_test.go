package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestFetchVectors(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-fetch-vectors"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	docs := []kb.Document{
		{ID: "a", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Metadata: map[string]any{"tag": "alpha", "rank": float64(1)}},
		{ID: "b", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Metadata: map[string]any{"tag": "beta"}},
		{ID: "c", Embedding: []float32{0, 0, 1, 0, 0, 0, 0, 0}},
	}
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))

	t.Run("fetches_existing_ids", func(t *testing.T) {
		records, err := loader.FetchVectors(ctx, kbID, []string{"a", "b"})
		require.NoError(t, err)
		require.Len(t, records, 2)
		byID := make(map[string]kb.VectorRecord)
		for _, r := range records {
			byID[r.ID] = r
		}
		assert.Equal(t, "alpha", byID["a"].Metadata["tag"])
		assert.Equal(t, float64(1), byID["a"].Metadata["rank"])
		assert.Equal(t, "beta", byID["b"].Metadata["tag"])
	})

	t.Run("missing_ids_omitted", func(t *testing.T) {
		records, err := loader.FetchVectors(ctx, kbID, []string{"a", "does-not-exist"})
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, "a", records[0].ID)
	})

	t.Run("doc_without_metadata_returns_nil_metadata", func(t *testing.T) {
		records, err := loader.FetchVectors(ctx, kbID, []string{"c"})
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Empty(t, records[0].Metadata)
	})

	t.Run("empty_ids_returns_empty", func(t *testing.T) {
		records, err := loader.FetchVectors(ctx, kbID, []string{})
		require.NoError(t, err)
		assert.Empty(t, records)
	})
}
