package kb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type alwaysConflictLeaseManager struct{}

func (alwaysConflictLeaseManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*WriteLease, error) {
	return nil, ErrWriteLeaseConflict
}

func (alwaysConflictLeaseManager) Renew(ctx context.Context, lease *WriteLease, ttl time.Duration) (*WriteLease, error) {
	return nil, ErrWriteLeaseConflict
}

func (alwaysConflictLeaseManager) Release(ctx context.Context, lease *WriteLease) error {
	return nil
}

func TestWriteLeaseRedis(t *testing.T) {
	type testCase struct {
		name string
		run  func(t *testing.T, ctx context.Context, mr *miniredis.Miniredis, mgr *RedisWriteLeaseManager)
	}

	tests := []testCase{
		{
			name: "renew_before_expiry",
			run: func(t *testing.T, ctx context.Context, _ *miniredis.Miniredis, mgr *RedisWriteLeaseManager) {
				lease, err := mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
				require.NoError(t, err)
				require.NotEmpty(t, lease.Token)

				_, err = mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
				require.ErrorIs(t, err, ErrWriteLeaseConflict)

				renewed, err := mgr.Renew(ctx, lease, 1200*time.Millisecond)
				require.NoError(t, err)
				assert.Equal(t, lease.Token, renewed.Token)
				assert.True(t, renewed.ExpiresAt.After(lease.ExpiresAt))

				require.NoError(t, mgr.Release(ctx, renewed))
				_, err = mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
				require.NoError(t, err)
			},
		},
		{
			name: "renew_after_expiry_conflicts",
			run: func(t *testing.T, ctx context.Context, mr *miniredis.Miniredis, mgr *RedisWriteLeaseManager) {
				lease, err := mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
				require.NoError(t, err)

				mr.FastForward(2 * time.Second)

				_, err = mgr.Renew(ctx, lease, time.Second)
				require.ErrorIs(t, err, ErrWriteLeaseConflict)
				_, err = mgr.Acquire(ctx, "kb-1", 500*time.Millisecond)
				require.NoError(t, err)
			},
		},
		{
			name: "release_requires_matching_token",
			run: func(t *testing.T, ctx context.Context, _ *miniredis.Miniredis, mgr *RedisWriteLeaseManager) {
				lease, err := mgr.Acquire(ctx, "kb-1", time.Second)
				require.NoError(t, err)

				wrong := &WriteLease{KBID: lease.KBID, Token: "not-the-token"}
				require.NoError(t, mgr.Release(ctx, wrong))

				_, err = mgr.Acquire(ctx, "kb-1", time.Second)
				require.ErrorIs(t, err, ErrWriteLeaseConflict)

				require.NoError(t, mgr.Release(ctx, lease))
				_, err = mgr.Acquire(ctx, "kb-1", time.Second)
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mr := miniredis.RunT(t)

			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = client.Close() })

			mgr, err := NewRedisWriteLeaseManager(client, "test:lease:")
			require.NoError(t, err)
			tc.run(t, ctx, mr, mgr)
		})
	}
}

func TestWriteLeaseInMemory(t *testing.T) {
	ctx := context.Background()
	mgr := NewInMemoryWriteLeaseManager()

	lease, err := mgr.Acquire(ctx, "kb-lease", 100*time.Millisecond)
	require.NoError(t, err)

	_, err = mgr.Acquire(ctx, "kb-lease", 100*time.Millisecond)
	require.ErrorIs(t, err, ErrWriteLeaseConflict)

	renewed, err := mgr.Renew(ctx, lease, 200*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, renewed.ExpiresAt.After(lease.ExpiresAt))

	require.NoError(t, mgr.Release(ctx, lease))

	_, err = mgr.Acquire(ctx, "kb-lease", 100*time.Millisecond)
	require.NoError(t, err)
}

func TestWriteLeaseInjectedManager(t *testing.T) {
	ctx := context.Background()
	harness := NewTestHarness(t, "kb-opt").
		WithOptions(WithWriteLeaseManager(alwaysConflictLeaseManager{})).
		Setup()
	defer harness.Cleanup()

	localPath := filepath.Join(harness.CacheDir(), "local.duckdb")
	require.NoError(t, os.WriteFile(localPath, []byte("v1"), 0o644))

	_, err := harness.KB().UploadSnapshotShardedIfMatch(ctx, "kb-opt", localPath, "", defaultSnapshotShardSize)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWriteLeaseConflict))
}
