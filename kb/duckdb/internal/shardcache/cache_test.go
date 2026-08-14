package shardcache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

type downloadFunc func(context.Context, string, string) error

func (fn downloadFunc) Download(ctx context.Context, key, destination string) error {
	return fn(ctx, key, destination)
}

func TestEnsureLocalFileProtectsOnlyTheDownloadedShard(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	contents := []byte("duckdb shard")
	shard := kb.SnapshotShardMetadata{ShardID: "one", Key: "remote/shard", Version: "v1", SizeBytes: int64(len(contents))}
	var protectedPath string
	reserved := false
	manager := Manager{
		CacheDir: cacheDir,
		BlobStore: downloadFunc(func(_ context.Context, _ string, destination string) error {
			require.True(t, reserved)
			return os.WriteFile(destination, contents, 0o600)
		}),
		ReserveCache: func(_ context.Context, incoming int64) (func(), error) {
			require.Equal(t, int64(len(contents)), incoming)
			reserved = true
			return func() { reserved = false }, nil
		},
		EvictCacheIfNeeded: func(_ context.Context, value string) error {
			require.False(t, reserved)
			protectedPath = value
			return nil
		},
	}
	localPath, cached, err := manager.EnsureLocalFile(ctx, "kb-a", shard)
	require.NoError(t, err)
	require.False(t, cached)
	require.Equal(t, localPath, protectedPath)
	require.FileExists(t, localPath)
}

func TestEnsureLocalFileRemovesShardWhenDiskBudgetRejectsIt(t *testing.T) {
	contents := []byte("duckdb shard")
	manager := Manager{
		CacheDir: t.TempDir(),
		BlobStore: downloadFunc(func(_ context.Context, _ string, destination string) error {
			return os.WriteFile(destination, contents, 0o600)
		}),
		EvictCacheIfNeeded: func(context.Context, string) error {
			return errors.New("disk reserve exhausted")
		},
	}
	localPath, cached, err := manager.EnsureLocalFile(context.Background(), "kb-a", kb.SnapshotShardMetadata{
		ShardID: "one", Key: "remote/shard", Version: "v1", SizeBytes: int64(len(contents)),
	})
	require.ErrorContains(t, err, "disk reserve exhausted")
	require.Empty(t, localPath)
	require.False(t, cached)
	entries, err := os.ReadDir(filepath.Join(manager.CacheDir, "kb-a", "query-shards"))
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestFileName(t *testing.T) {
	got := FileName(kb.SnapshotShardMetadata{ShardID: "bad/id", Key: "k", Version: "v"})
	require.Contains(t, got, "bad_id")
	require.Contains(t, got, ".duckdb")
}
