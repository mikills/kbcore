package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

// A deferred session is only worth anything if the rows stay invisible until
// the commit and then appear.
func TestDeferredSessionPublishesOnCommit(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-deferred-session"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
		{ID: "published", Text: "already visible to readers"},
	}))

	search := func() []string {
		results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
			Mode: kb.SearchModeBM25, TopK: 10, QueryText: "visible readers deferred",
		})
		require.NoError(t, err)
		ids := make([]string, len(results))
		for i, r := range results {
			ids[i] = r.ID
		}
		return ids
	}
	require.Contains(t, search(), "published")

	deferBatch(t, loader, kbID, []kb.Document{{ID: "deferred-a", Text: "deferred visible readers"}})
	deferBatch(t, loader, kbID, []kb.Document{{ID: "deferred-b", Text: "deferred visible readers"}})

	require.True(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID), "deferred writes were not marked")

	require.NoError(t, loader.CommitPreparedDocs(ctx, kbID))
	require.False(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID), "commit left the session open")

	// Clearing the cache leaves only what reached the published manifest, so a
	// search after it proves the commit published rather than just cleared the
	// marker.
	require.NoError(t, loader.ClearCache())

	found := search()
	require.Contains(t, found, "deferred-a", "commit did not publish the session")
	require.Contains(t, found, "deferred-b", "commit published only part of the session")
	require.Contains(t, found, "published", "commit dropped rows published before the session")
}

// Nothing is durable until the commit. If a deferred write reached the manifest
// on its own, the session would be pointless and the marker would be lying.
func TestDeferredWritesAreNotPublishedWithoutACommit(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-deferred-uncommitted"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
		{ID: "published", Text: "durable row"},
	}))
	deferBatch(t, loader, kbID, []kb.Document{{ID: "uncommitted", Text: "durable row uncommitted"}})

	require.NoError(t, loader.ClearCache())

	results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
		Mode: kb.SearchModeBM25, TopK: 10, QueryText: "durable row uncommitted",
	})
	require.NoError(t, err)
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	require.Contains(t, ids, "published")
	require.NotContains(t, ids, "uncommitted", "a deferred write reached the manifest without a commit")
}

// The streaming path is a second way in, used by code indexing.
func TestDeferredStreamWaitsForItsCommit(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-deferred-stream"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	format := registerFormatReturning(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
		{ID: "published", Text: "durable row"},
	}))

	vector, err := loader.Embedder.Embed(ctx, "streamed row uncommitted")
	require.NoError(t, err)
	sent := false
	_, err = format.PublishPreparedStream(ctx, kb.PreparedStreamRequest{
		KBID:    kbID,
		Options: kb.UpsertDocsOptions{DeferPublish: true},
		Next: func(context.Context) ([]kb.EmbeddedDocument, error) {
			if sent {
				return nil, nil
			}
			sent = true
			return []kb.EmbeddedDocument{
				{ID: "streamed", Text: "streamed row uncommitted", Embedding: vector},
			}, nil
		},
	})
	require.NoError(t, err)

	require.True(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID),
		"a deferred stream left its rows unmarked")

	require.NoError(t, loader.ClearCache())
	results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
		Mode: kb.SearchModeBM25, TopK: 10, QueryText: "streamed row uncommitted",
	})
	require.NoError(t, err)
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	require.Contains(t, ids, "published")
	require.NotContains(t, ids, "streamed", "a deferred stream reached the manifest without a commit")
}

// An abandoned session would otherwise block eviction, compaction and every
// later write for its knowledge base, with nothing to clear it.
func TestReapAbandonedSession(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-abandoned-session"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
		{ID: "seed", Text: "seed row"},
	}))
	deferBatch(t, loader, kbID, []kb.Document{{ID: "orphan", Text: "seed row orphan"}})
	require.True(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID))

	reaped, err := loader.ReapAbandonedSessions(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.False(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID))

	results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
		Mode: kb.SearchModeBM25, TopK: 10, QueryText: "seed row orphan",
	})
	require.NoError(t, err)
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	require.Contains(t, ids, "orphan", "the reaper cleared the marker without publishing")
}

func TestReapCachePressure(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-abandoned-session-over-budget"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	deferBatch(t, loader, kbID, []kb.Document{{ID: "durable", Text: "durable abandoned row"}})
	require.True(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID))
	loader.SetMaxCacheBytes(1)

	reaped, err := loader.ReapAbandonedSessions(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.False(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID))

	loader.SetMaxCacheBytes(0)
	require.NoError(t, loader.ClearCache())
	results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
		Mode: kb.SearchModeBM25, TopK: 10, QueryText: "durable abandoned row",
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "durable", results[0].ID)
}

// A live session belongs to a client that is still writing.
func TestReapLeavesLiveSessionAlone(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-live-session"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{ID: "seed", Text: "seed row"}}))
	deferBatch(t, loader, kbID, []kb.Document{{ID: "in-flight", Text: "seed row in flight"}})

	_, err := loader.IngestSessionsFor().Hold(ctx, kbID, "")
	require.NoError(t, err)

	reaped, err := loader.ReapAbandonedSessions(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, reaped, "the reaper published a session a client still holds")
	require.True(t, kb.HasPendingSession(harness.CacheDir()+"/"+kbID))
}

// deferBatch publishes one batch the way the ingest worker does, holding it for
// a later commit. UpsertDocsAndUpload* always upload, so they cannot defer.
func deferBatch(t *testing.T, loader *kb.KB, kbID string, docs []kb.Document) {
	t.Helper()
	embedded := make([]kb.EmbeddedDocument, len(docs))
	for i, d := range docs {
		vector, err := loader.Embedder.Embed(context.Background(), d.Text)
		require.NoError(t, err)
		embedded[i] = kb.EmbeddedDocument{ID: d.ID, Text: d.Text, Metadata: d.Metadata, Embedding: vector}
	}
	require.NoError(t, loader.PublishPreparedDocs(
		context.Background(), kbID, embedded, nil, kb.UpsertDocsOptions{DeferPublish: true},
	))
}

// An interrupted run leaves a journal of every chunk it uploaded, and the next
// run deletes them.
func TestDeletingEveryDocumentPublishes(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-emptied"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
		{ID: "only-one", Text: "the sole row"},
	}))

	require.NoError(t, loader.DeleteDocsAndUpload(ctx, kbID, []string{"only-one"}, kb.DeleteDocsOptions{}))

	require.NoError(t, loader.ClearCache())
	results, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
		Mode: kb.SearchModeBM25, TopK: 10, QueryText: "the sole row",
	})
	if err == nil {
		require.Empty(t, results, "a deleted row survived the publish")
	} else {
		// An emptied knowledge base has no shards left to read, which reads the same
		// as one that was never built.
		require.ErrorIs(t, err, kb.ErrKBUninitialized)
	}

	// The published manifest now names no shards, so there is nothing to
	// reconstruct a local shard from.
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{
		{ID: "written-after", Text: "the sole row returns"},
	}))
	require.NoError(t, loader.ClearCache())
	found, err := loader.Search(ctx, kbID, nil, &kb.SearchOptions{
		Mode: kb.SearchModeBM25, TopK: 10, QueryText: "the sole row returns",
	})
	require.NoError(t, err)
	ids := make([]string, len(found))
	for i, r := range found {
		ids[i] = r.ID
	}
	require.Contains(t, ids, "written-after")
	require.NotContains(t, ids, "only-one", "a deleted row came back with the rebuild")

	// A delete against an emptied knowledge base has nothing to remove, and
	// must not wedge the client that is trying to clean up after itself.
	require.NoError(t, loader.DeleteDocsAndUpload(
		ctx, kbID, []string{"written-after"}, kb.DeleteDocsOptions{},
	))
	require.NoError(t, loader.ClearCache())
	require.NoError(t, loader.DeleteDocsAndUpload(
		ctx, kbID, []string{"written-after"}, kb.DeleteDocsOptions{},
	))
}
