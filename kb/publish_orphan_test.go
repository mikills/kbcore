package kb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/require"
)

func writeBlobFile(t *testing.T, root, key string, modTime time.Time) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(key))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(key), 0o644))
	require.NoError(t, os.Chtimes(path, modTime, modTime))
}

func TestPublishReconcile(t *testing.T) {
	t.Run("collects_replaced_shards", testPublishCollectsReplaced)
}

// A publish swaps the shard set without telling the GC queue.
func testPublishCollectsReplaced(t *testing.T) {
	const kbID = "orphan-publish"
	ctx := context.Background()
	blobRoot := t.TempDir()
	now := time.Now().UTC()

	liveKey := ShardBlobPrefix(kbID) + "0123456789abcdef/shard-00000.duckdb"
	replacedKey := ShardBlobPrefix(kbID) + "fedcba9876543210/shard-00000.duckdb"
	writeBlobFile(t, blobRoot, liveKey, now)
	writeBlobFile(t, blobRoot, replacedKey, now.Add(-24*time.Hour))

	mock := &mockArtifactFormat{
		buildArtifactsFn: func(context.Context, string, string, int64) ([]SnapshotShardMetadata, error) {
			return []SnapshotShardMetadata{
				{ShardID: "shard-00000", Key: liveKey, SizeBytes: 1, VectorRows: 1, CreatedAt: now},
			}, nil
		},
	}
	clock := NewFakeClock(now)
	loader := NewKB(&LocalBlobStore{Root: blobRoot}, t.TempDir(),
		WithArtifactFormat(mock), WithClock(clock))

	version := seedManifest(t, ctx, loader.BlobStore, kbID, []SnapshotShardMetadata{
		{ShardID: "shard-00000", Key: replacedKey, SizeBytes: 1, VectorRows: 1, CreatedAt: now},
	})

	srcPath := filepath.Join(t.TempDir(), "source.mock")
	require.NoError(t, os.WriteFile(srcPath, []byte("source"), 0o644))
	_, err := loader.UploadSnapshotShardedIfMatch(ctx, kbID, srcPath, version, DefaultSnapshotShardSize)
	require.NoError(t, err)

	clock.Advance(DefaultShardGCGraceWindow + time.Minute)
	result, err := loader.SweepDelayedShardGC(ctx, time.Time{})
	require.NoError(t, err)
	require.Equal(t, 1, result.Deleted)
	require.Equal(t, 0, result.Pending)

	_, err = loader.BlobStore.Head(ctx, replacedKey)
	require.Error(t, err)
	_, err = loader.BlobStore.Head(ctx, liveKey)
	require.NoError(t, err)
}
