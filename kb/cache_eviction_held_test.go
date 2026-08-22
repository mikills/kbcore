package kb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	. "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func writeCachedKB(t *testing.T, cacheDir, kbID string, bytes int, pending bool) {
	t.Helper()
	dir := filepath.Join(cacheDir, kbID)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "shard.db"), make([]byte, bytes), 0o644))
	if pending {
		require.NoError(t, MarkPendingSession(dir))
	}
}

func TestHeldSessionBudget(t *testing.T) {
	ctx := context.Background()

	t.Run("a session larger than the budget does not block writes", func(t *testing.T) {
		cacheDir := t.TempDir()
		writeCachedKB(t, cacheDir, "uploading", 4096, true)
		kb := NewKB(nil, cacheDir, WithMaxCacheBytes(1024))

		require.NoError(t, kb.EvictCacheIfNeeded(ctx, ""))
		require.True(t, HasPendingSession(filepath.Join(cacheDir, "uploading")))
		metrics := kb.CacheEvictionMetricsSnapshot()
		require.Zero(t, metrics.CacheBytesCurrent)
		require.GreaterOrEqual(t, metrics.CacheHeldBytes, int64(4096))
		require.Contains(t, kb.CacheEvictionOpenMetricsText(), "minnow_cache_held_bytes")
	})

	t.Run("cache alongside a session is still evicted to make room", func(t *testing.T) {
		cacheDir := t.TempDir()
		writeCachedKB(t, cacheDir, "uploading", 4096, true)
		writeCachedKB(t, cacheDir, "cold", 4096, false)
		kb := NewKB(nil, cacheDir, WithMaxCacheBytes(1024))

		require.NoError(t, kb.EvictCacheIfNeeded(ctx, ""))
		_, err := os.Stat(filepath.Join(cacheDir, "cold"))
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("a published shard over budget still reports the failure", func(t *testing.T) {
		cacheDir := t.TempDir()
		writeCachedKB(t, cacheDir, "cold", 4096, false)
		kb := NewKB(nil, cacheDir, WithMaxCacheBytes(1024))

		require.ErrorIs(t, kb.EvictCacheIfNeeded(ctx, "cold"), ErrCacheBudgetExceeded)
	})
}
