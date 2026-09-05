package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

// TestBackupParity exercises the Phase 1 byte-copy clone end to end: ingest
// into a source KB, capture a backup, clone to a new KB, and require the
// clone to answer the same query with the same top hit.
func TestBackupParity(t *testing.T) {
	t.Run("clone_round_trip", func(t *testing.T) {
		ctx := context.Background()
		harness := kb.NewTestHarness(t, "kb-src").
			WithEmbedder(newFixtureEmbedder(8)).
			Setup()
		t.Cleanup(harness.Cleanup)
		registerFormatOnHarness(t, harness)
		loader := harness.KB()

		require.NoError(t, loader.UpsertDocsAndUpload(ctx, "kb-src", []kb.Document{
			{ID: "alpha", Text: "alpha document about databases"},
			{ID: "beta", Text: "beta document about networking"},
		}))

		_, err := loader.CreateBackup(ctx, "kb-src", "b1")
		require.NoError(t, err)
		require.NoError(t, loader.CloneKBFromBackup(ctx, "kb-src", "b1", "kb-dst"))

		vec, err := loader.Embed(ctx, "alpha document about databases")
		require.NoError(t, err)
		srcRes, err := loader.Search(ctx, "kb-src", vec, &kb.SearchOptions{TopK: 2})
		require.NoError(t, err)
		dstRes, err := loader.Search(ctx, "kb-dst", vec, &kb.SearchOptions{TopK: 2})
		require.NoError(t, err)
		require.NotEmpty(t, srcRes)
		require.NotEmpty(t, dstRes)
		require.Equal(t, srcRes[0].ID, dstRes[0].ID, "clone must preserve query parity")
		require.Equal(t, len(srcRes), len(dstRes))
	})

	t.Run("snapshot_lists_guard_clone", func(t *testing.T) {
		ctx := context.Background()
		harness := kb.NewTestHarness(t, "kb-snap").
			WithEmbedder(newFixtureEmbedder(8)).
			Setup()
		t.Cleanup(harness.Cleanup)
		registerFormatOnHarness(t, harness)
		loader := harness.KB()

		require.NoError(t, loader.UpsertDocsAndUpload(ctx, "kb-snap", []kb.Document{
			{ID: "a", Text: "snapshot guard document"},
		}))
		_, err := loader.CreateSnapshot(ctx, "kb-snap", "s1")
		require.NoError(t, err)
		err = loader.DeleteKnowledgeBase(ctx, "kb-snap")
		require.Error(t, err)
		require.ErrorIs(t, err, kb.ErrDeleteBlockedByBackups)
	})
}
