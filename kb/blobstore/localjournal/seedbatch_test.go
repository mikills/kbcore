package localjournal_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/blobstore/localjournal"
	"github.com/stretchr/testify/require"
)

func openSeedStore(t *testing.T) (*localjournal.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store := localjournal.New(dir, journal.Config{})
	require.NoError(t, store.Open(context.Background()))
	t.Cleanup(func() { _ = store.Close() })
	return store, filepath.Join(dir, "journal.db")
}

func remoteObject(key string) journal.Object {
	return journal.Object{
		Key:           key,
		Version:       "v1",
		RemoteVersion: "v1",
		UpdatedAt:     time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC),
		Size:          7,
	}
}

func TestSeedBatch(t *testing.T) {
	t.Run("inventories_remote_objects", testSeedBatchInventories)
	t.Run("keeps_local_entries", testSeedBatchKeepsLocal)
	t.Run("rejects_invalid_objects", testSeedBatchRejectsInvalid)
	t.Run("empty_does_not_write", testSeedBatchEmptyNoWrite)
}

func testSeedBatchInventories(t *testing.T) {
	store, _ := openSeedStore(t)
	ctx := context.Background()

	require.NoError(t, store.SeedBatch(ctx, []journal.Object{
		remoteObject("a/one"),
		remoteObject("a/two"),
	}))

	for _, key := range []string{"a/one", "a/two"} {
		object, err := store.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, object.Replicated)
		require.Equal(t, "v1", object.RemoteVersion)
		require.Empty(t, object.PayloadID)
	}
}

// Seeding is inventory, never a mutation.
func testSeedBatchKeepsLocal(t *testing.T) {
	store, _ := openSeedStore(t)
	ctx := context.Background()

	written, _, err := store.PutBytes(ctx, "a/local", []byte("local"), "")
	require.NoError(t, err)
	_, _, err = store.PutBytes(ctx, "a/tombstoned", []byte("gone"), "")
	require.NoError(t, err)
	_, err = store.Delete(ctx, "a/tombstoned")
	require.NoError(t, err)

	require.NoError(t, store.SeedBatch(ctx, []journal.Object{
		remoteObject("a/local"),
		remoteObject("a/tombstoned"),
	}))

	local, err := store.Get(ctx, "a/local")
	require.NoError(t, err)
	require.Equal(t, written.Version, local.Version)

	tombstoned, err := store.Get(ctx, "a/tombstoned")
	require.NoError(t, err)
	require.True(t, tombstoned.Tombstone)
}

func testSeedBatchRejectsInvalid(t *testing.T) {
	store, _ := openSeedStore(t)
	ctx := context.Background()

	require.Error(t, store.SeedBatch(ctx, []journal.Object{remoteObject("bad\\key")}))
	require.ErrorIs(t, store.SeedBatch(ctx, []journal.Object{{Key: "a/one", Version: "v1"}}), journal.ErrCorrupt)

	_, err := store.Get(ctx, "a/one")
	require.ErrorIs(t, err, journal.ErrNotFound)
}

// bbolt fsyncs on every commit, and the journal opens without NoSync.
func testSeedBatchEmptyNoWrite(t *testing.T) {
	store, dbPath := openSeedStore(t)
	ctx := context.Background()

	require.NoError(t, store.SeedBatch(ctx, []journal.Object{remoteObject("a/one")}))
	before, err := os.Stat(dbPath)
	require.NoError(t, err)

	for range 5 {
		require.NoError(t, store.SeedBatch(ctx, nil))
		require.NoError(t, store.SeedBatch(ctx, []journal.Object{}))
	}

	after, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime())
}
