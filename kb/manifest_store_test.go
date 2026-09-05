package kb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/blobstore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobManifestStore(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		runManifestStoreTests(t, func(t *testing.T) ManifestStore {
			blobRoot := t.TempDir()
			blobStore := &LocalBlobStore{Root: blobRoot}
			return &BlobManifestStore{Store: blobStore}
		})
	})
	t.Run("get_retry", testBlobManifestStoreGetRetry)
}

// runManifestStoreTests exercises the full ManifestStore contract.
// Each subtest receives a fresh, empty store instance via newStore.
func runManifestStoreTests(t *testing.T, newStore func(t *testing.T) ManifestStore) {
	t.Helper()

	ctx := context.Background()
	kbID := "test-manifest-store"

	sampleManifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         "duckdb_shard_files",
		KBID:           kbID,
		CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
		TotalSizeBytes: 1024,
		Shards: []SnapshotShardMetadata{
			{
				ShardID:    "shard-00000",
				Key:        kbID + ".duckdb.shards/abc/shard-00000.duckdb",
				SizeBytes:  1024,
				VectorRows: 10,
				CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
			},
		},
	}

	tests := []struct {
		name string
		run  func(t *testing.T, store ManifestStore)
	}{
		{
			name: "get_missing",
			run: func(t *testing.T, store ManifestStore) {
				_, err := store.Get(ctx, kbID)
				require.ErrorIs(t, err, ErrManifestNotFound)
			},
		},
		{
			name: "upsert_create_empty_version",
			run: func(t *testing.T, store ManifestStore) {
				version, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				require.NoError(t, err)
				require.NotEmpty(t, version)
			},
		},
		{
			name: "get_after_upsert",
			run: func(t *testing.T, store ManifestStore) {
				version, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				require.NoError(t, err)

				doc, err := store.Get(ctx, kbID)
				require.NoError(t, err)
				require.Equal(t, version, doc.Version)
				require.Equal(t, kbID, doc.Manifest.KBID)
				require.Len(t, doc.Manifest.Shards, len(sampleManifest.Shards))
			},
		},
		{
			name: "upsert_cas_conflict",
			run: func(t *testing.T, store ManifestStore) {
				_, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				require.NoError(t, err)

				_, err = store.UpsertIfMatch(ctx, kbID, sampleManifest, "stale-version")
				require.ErrorIs(t, err, ErrBlobVersionMismatch)
			},
		},
		{
			name: "upsert_cas_success",
			run: func(t *testing.T, store ManifestStore) {
				v1, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				require.NoError(t, err)

				v2, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, v1)
				require.NoError(t, err)
				require.NotEmpty(t, v2)
			},
		},
		{
			name: "head_version_missing",
			run: func(t *testing.T, store ManifestStore) {
				version, err := store.HeadVersion(ctx, kbID)
				require.NoError(t, err)
				require.Empty(t, version)
			},
		},
		{
			name: "head_version_exists",
			run: func(t *testing.T, store ManifestStore) {
				upsertVersion, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				require.NoError(t, err)

				headVersion, err := store.HeadVersion(ctx, kbID)
				require.NoError(t, err)
				require.Equal(t, upsertVersion, headVersion)
			},
		},
		{
			name: "delete_existing",
			run: func(t *testing.T, store ManifestStore) {
				_, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				require.NoError(t, err)

				require.NoError(t, store.Delete(ctx, kbID))

				_, err = store.Get(ctx, kbID)
				require.ErrorIs(t, err, ErrManifestNotFound)
			},
		},
		{
			name: "delete_missing",
			run: func(t *testing.T, store ManifestStore) {
				require.NoError(t, store.Delete(ctx, kbID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			tc.run(t, store)
		})
	}
}

func testBlobManifestStoreGetRetry(t *testing.T) {
	t.Run("succeeds_after_transient_churn", func(t *testing.T) {
		// Simulate a concurrent writer that changes the manifest between the
		// first Head and the post-Download Head for 2 attempts, then stabilizes.
		blobRoot := t.TempDir()
		inner := &LocalBlobStore{Root: blobRoot}
		churner := &churnOnHeadBlobStore{inner: inner, churnAttempts: 2}
		ms := &BlobManifestStore{Store: churner}

		ctx := context.Background()
		kbID := "kb-retry-churn"

		seedManifestDirect(t, inner, kbID, 1)

		doc, err := ms.Get(ctx, kbID)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, kbID, doc.Manifest.KBID)
		assert.Len(t, doc.Manifest.Shards, 1)
		// Should have retried: 2 churned attempts + 1 successful = 3 total Head pairs.
		assert.Equal(t, int32(2), churner.churnsTriggered.Load())
	})

	t.Run("exhausts_retries_returns_error", func(t *testing.T) {
		// Churn on every attempt - should exhaust all 4 retries.
		blobRoot := t.TempDir()
		inner := &LocalBlobStore{Root: blobRoot}
		churner := &churnOnHeadBlobStore{inner: inner, churnAttempts: 10}
		ms := &BlobManifestStore{Store: churner}

		ctx := context.Background()
		kbID := "kb-retry-exhaust"

		seedManifestDirect(t, inner, kbID, 1)

		_, err := ms.Get(ctx, kbID)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBlobVersionMismatch)
	})

	t.Run("respects_context_cancellation_during_backoff", func(t *testing.T) {
		blobRoot := t.TempDir()
		inner := &LocalBlobStore{Root: blobRoot}
		churner := &churnOnHeadBlobStore{inner: inner, churnAttempts: 10}
		ms := &BlobManifestStore{Store: churner}

		kbID := "kb-retry-cancel"
		seedManifestDirect(t, inner, kbID, 1)

		ctx, cancel := context.WithCancel(context.Background())
		// Cancel after first churn triggers backoff sleep.
		churner.onChurn = func() { cancel() }

		_, err := ms.Get(ctx, kbID)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// churnOnHeadBlobStore wraps a BlobStore and mutates the manifest blob between
// the pre-download Head and the post-download Head for the first N retry
// attempts, simulating a concurrent writer that keeps changing the manifest.
type churnOnHeadBlobStore struct {
	inner           *LocalBlobStore
	churnAttempts   int
	churnsTriggered atomic.Int32
	onChurn         func()

	mu         sync.Mutex
	headCount  map[string]int // per-key head call count within an attempt
	churnsDone int
}

func (c *churnOnHeadBlobStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	info, err := c.inner.Head(ctx, key)
	if err != nil {
		return nil, err
	}

	// Only churn on manifest keys.
	if filepath.Ext(key) != ".json" {
		return info, nil
	}

	c.mu.Lock()
	if c.headCount == nil {
		c.headCount = make(map[string]int)
	}
	c.headCount[key]++
	count := c.headCount[key]
	shouldChurn := count%2 == 0 && c.churnsDone < c.churnAttempts
	if shouldChurn {
		c.churnsDone++
	}
	c.mu.Unlock()

	// On the second Head call of an attempt (post-download verification),
	// mutate the file so the version changes.
	if shouldChurn {
		c.churnsTriggered.Add(1)
		// Append a byte to change the hash/version.
		path := filepath.Join(c.inner.Root, key)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, fmt.Errorf("churn read: %w", readErr)
		}
		if writeErr := os.WriteFile(path, append(data, '\n'), 0o644); writeErr != nil {
			return nil, fmt.Errorf("churn write: %w", writeErr)
		}
		// Re-read to get the updated info.
		info, err = c.inner.Head(ctx, key)
		if c.onChurn != nil {
			c.onChurn()
		}
	}

	return info, err
}

func (c *churnOnHeadBlobStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	return c.inner.DownloadBytes(ctx, key)
}

func (c *churnOnHeadBlobStore) Download(ctx context.Context, key string, dest string) error {
	return c.inner.Download(ctx, key, dest)
}

func (c *churnOnHeadBlobStore) UploadBytesIfMatch(
	ctx context.Context,
	key string,
	data []byte,
	expectedVersion string,
) (*BlobObjectInfo, error) {
	return c.inner.UploadBytesIfMatch(ctx, key, data, expectedVersion)
}

func (c *churnOnHeadBlobStore) UploadIfMatch(
	ctx context.Context,
	key string,
	src string,
	expectedVersion string,
) (*BlobObjectInfo, error) {
	return c.inner.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (c *churnOnHeadBlobStore) Delete(ctx context.Context, key string) error {
	return c.inner.Delete(ctx, key)
}

func (c *churnOnHeadBlobStore) Copy(ctx context.Context, srcKey, dstKey string, opts blobstore.CopyOptions) (*BlobObjectInfo, error) {
	return c.inner.Copy(ctx, srcKey, dstKey, opts)
}

func (c *churnOnHeadBlobStore) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	return c.inner.List(ctx, prefix)
}

func isManifestKey(key string) bool {
	return filepath.Ext(key) == ".json"
}

// seedManifestDirect writes a manifest blob directly into the given blob store.
func seedManifestDirect(t *testing.T, bs *LocalBlobStore, kbID string, shardCount int) {
	t.Helper()
	manifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         ShardManifestLayoutDuckDBs,
		FormatKind:     "duckdb_sharded",
		FormatVersion:  1,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: int64(shardCount),
	}
	for i := range shardCount {
		manifest.Shards = append(manifest.Shards, SnapshotShardMetadata{
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

	_, err = bs.UploadIfMatch(context.Background(), ShardManifestKey(kbID), path, "")
	require.NoError(t, err)
}
