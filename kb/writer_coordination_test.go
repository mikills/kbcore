package kb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type alwaysConflictLeaseManager struct{}

func (alwaysConflictLeaseManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*kb.WriteLease, error) {
	return nil, kb.ErrWriteLeaseConflict
}

func (alwaysConflictLeaseManager) Renew(
	ctx context.Context,
	lease *kb.WriteLease,
	ttl time.Duration,
) (*kb.WriteLease, error) {
	return nil, kb.ErrWriteLeaseConflict
}

func (alwaysConflictLeaseManager) Release(ctx context.Context, lease *kb.WriteLease) error {
	return nil
}

func (alwaysConflictLeaseManager) Peek(context.Context, string) (*kb.WriteLease, error) {
	return nil, nil
}

func TestWriteLease(t *testing.T) {
	t.Run("redis renew before expiry", func(t *testing.T) {
		ctx := context.Background()
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		mgr, err := kb.NewRedisWriteLeaseManager(client, "test:lease:")
		require.NoError(t, err)

		lease, err := mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
		require.NoError(t, err)
		require.NotEmpty(t, lease.Token)

		_, err = mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
		require.ErrorIs(t, err, kb.ErrWriteLeaseConflict)

		renewed, err := mgr.Renew(ctx, lease, 1200*time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, lease.Token, renewed.Token)
		assert.True(t, renewed.ExpiresAt.After(lease.ExpiresAt))

		require.NoError(t, mgr.Release(ctx, renewed))
		_, err = mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
		require.NoError(t, err)
	})

	t.Run("redis renew after expiry conflicts", func(t *testing.T) {
		ctx := context.Background()
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		mgr, err := kb.NewRedisWriteLeaseManager(client, "test:lease:")
		require.NoError(t, err)

		lease, err := mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
		require.NoError(t, err)

		mr.FastForward(2 * time.Second)

		_, err = mgr.Renew(ctx, lease, time.Second)
		require.ErrorIs(t, err, kb.ErrWriteLeaseConflict)
		_, err = mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
		require.NoError(t, err)
	})

	t.Run("redis release requires matching token", func(t *testing.T) {
		ctx := context.Background()
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		mgr, err := kb.NewRedisWriteLeaseManager(client, "test:lease:")
		require.NoError(t, err)

		lease, err := mgr.Acquire(ctx, "kb-1", time.Second)
		require.NoError(t, err)

		wrong := &kb.WriteLease{KBID: lease.KBID, Token: "not-the-token"}
		require.NoError(t, mgr.Release(ctx, wrong))

		_, err = mgr.Acquire(ctx, "kb-1", time.Second)
		require.ErrorIs(t, err, kb.ErrWriteLeaseConflict)

		require.NoError(t, mgr.Release(ctx, lease))
		_, err = mgr.Acquire(ctx, "kb-1", time.Second)
		require.NoError(t, err)
	})

	t.Run("in-memory acquire renew release cycle", func(t *testing.T) {
		ctx := context.Background()
		mgr := kb.NewInMemoryWriteLeaseManager()

		lease, err := mgr.Acquire(ctx, "kb-lease", 100*time.Millisecond)
		require.NoError(t, err)

		_, err = mgr.Acquire(ctx, "kb-lease", 100*time.Millisecond)
		require.ErrorIs(t, err, kb.ErrWriteLeaseConflict)

		renewed, err := mgr.Renew(ctx, lease, 200*time.Millisecond)
		require.NoError(t, err)
		assert.True(t, renewed.ExpiresAt.After(lease.ExpiresAt))

		require.NoError(t, mgr.Release(ctx, lease))

		_, err = mgr.Acquire(ctx, "kb-lease", 100*time.Millisecond)
		require.NoError(t, err)
	})

	t.Run("injected conflict manager blocks upload", func(t *testing.T) {
		ctx := context.Background()
		harness := kb.NewTestHarness(t, "kb-opt").
			WithOptions(kb.WithWriteLeaseManager(alwaysConflictLeaseManager{})).
			Setup()
		defer harness.Cleanup()

		localPath := filepath.Join(harness.CacheDir(), "local.duckdb")
		require.NoError(t, os.WriteFile(localPath, []byte("v1"), 0o644))

		_, err := harness.KB().UploadSnapshotShardedIfMatch(ctx, "kb-opt", localPath, "", kb.DefaultSnapshotShardSize)
		require.Error(t, err)
		require.True(t, errors.Is(err, kb.ErrWriteLeaseConflict))
	})
}
