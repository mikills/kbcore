package lease_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/lease"
	"github.com/mikills/minnow/kb/testutil"
)

func newS3Manager(t *testing.T) *lease.S3Manager {
	t.Helper()
	ctx := context.Background()
	mock, err := testutil.StartMockS3(ctx, "lease-bucket")
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := blobstore.NewS3BlobStore(mock.Client, mock.Bucket, "")
	mgr, err := lease.NewS3Manager(store, "")
	require.NoError(t, err)
	return mgr
}

// Ensure S3Manager satisfies the lease.Manager interface.
var _ lease.Manager = (*lease.S3Manager)(nil)

func TestS3Manager(t *testing.T) {
	t.Run("acquire_and_release", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		l, err := mgr.Acquire(ctx, "kb-a", time.Minute)
		require.NoError(t, err)
		require.NotEmpty(t, l.Token)

		require.NoError(t, mgr.Release(ctx, l))

		// Re-acquire after release succeeds.
		l2, err := mgr.Acquire(ctx, "kb-a", time.Minute)
		require.NoError(t, err)
		require.NotEmpty(t, l2.Token)
	})

	t.Run("conflict_while_held", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		_, err := mgr.Acquire(ctx, "kb-b", time.Minute)
		require.NoError(t, err)

		_, err = mgr.Acquire(ctx, "kb-b", time.Minute)
		require.ErrorIs(t, err, lease.ErrConflict)
	})

	t.Run("expired_lock_evicted_on_acquire", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		fakeClock := &fakeClock{t: time.Now().UTC()}
		mgr.SetClock(fakeClock)

		_, err := mgr.Acquire(ctx, "kb-c", 10*time.Second)
		require.NoError(t, err)

		// Advance clock past TTL.
		fakeClock.t = fakeClock.t.Add(30 * time.Second)

		// Next acquire evicts the expired lock and succeeds.
		l2, err := mgr.Acquire(ctx, "kb-c", time.Minute)
		require.NoError(t, err)
		require.NotEmpty(t, l2.Token)
	})

	t.Run("renew_extends_expiry", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		l, err := mgr.Acquire(ctx, "kb-d", 10*time.Second)
		require.NoError(t, err)

		renewed, err := mgr.Renew(ctx, l, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, l.Token, renewed.Token)
		assert.True(t, renewed.ExpiresAt.After(l.ExpiresAt))
	})

	t.Run("renew_with_wrong_token_conflicts", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		l, err := mgr.Acquire(ctx, "kb-e", time.Minute)
		require.NoError(t, err)

		impostor := &lease.Lease{KBID: l.KBID, Token: "wrong-token", ExpiresAt: l.ExpiresAt}
		_, err = mgr.Renew(ctx, impostor, time.Minute)
		require.ErrorIs(t, err, lease.ErrConflict)
	})

	t.Run("release_with_wrong_token_is_noop", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		l, err := mgr.Acquire(ctx, "kb-f", time.Minute)
		require.NoError(t, err)

		impostor := &lease.Lease{KBID: l.KBID, Token: "wrong-token", ExpiresAt: l.ExpiresAt}
		require.NoError(t, mgr.Release(ctx, impostor))

		// Original lock still held — second acquire conflicts.
		_, err = mgr.Acquire(ctx, "kb-f", time.Minute)
		require.ErrorIs(t, err, lease.ErrConflict)
	})

	t.Run("release_missing_lock_is_noop", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()
		l := &lease.Lease{KBID: "kb-never-acquired", Token: "some-token", ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, mgr.Release(ctx, l))
	})

	t.Run("independent_kbs_dont_interfere", func(t *testing.T) {
		mgr := newS3Manager(t)
		ctx := context.Background()

		l1, err := mgr.Acquire(ctx, "kb-x", time.Minute)
		require.NoError(t, err)
		l2, err := mgr.Acquire(ctx, "kb-y", time.Minute)
		require.NoError(t, err)

		require.NoError(t, mgr.Release(ctx, l1))
		require.NoError(t, mgr.Release(ctx, l2))
	})
}

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }
