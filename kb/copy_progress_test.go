package kb

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/stretchr/testify/require"
)

// copyFaultStore injects copy outages: the failAt-th call fails (1-based,
// 0 disables), or every call fails when failAll is set. Healing is flipping
// the flags.
type copyFaultStore struct {
	BlobStore
	mu      sync.Mutex
	failAt  int
	failAll bool
	calls   int
}

func (s *copyFaultStore) Copy(ctx context.Context, srcKey, dstKey string, opts blobstore.CopyOptions) (*blobstore.ObjectInfo, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	fail := s.failAll || (s.failAt > 0 && n == s.failAt)
	s.mu.Unlock()
	if fail {
		return nil, errors.New("outage: remote copy unavailable")
	}
	return s.BlobStore.Copy(ctx, srcKey, dstKey, opts)
}

func (s *copyFaultStore) heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAll = false
	s.failAt = 0
}

func copyTestSeed(t *testing.T, loader *KB) {
	t.Helper()
	advSeedKB(t, loader, "src", map[string]string{"s0": "alpha-body", "s1": "beta-body"})
	_, err := loader.CreateBackup(context.Background(), "src", "b1")
	require.NoError(t, err)
}

func requireCopyComplete(t *testing.T, loader *KB) {
	t.Helper()
	ctx := context.Background()
	desc, err := loader.GetBackup(ctx, "src", "b1")
	require.NoError(t, err)
	for _, ref := range desc.Shards {
		srcRaw, err := loader.BlobStore.DownloadBytes(ctx, ref.Key)
		require.NoError(t, err)
		dstRaw, err := loader.BlobStore.DownloadBytes(ctx, remapShardKey("src", "dst", ref.Key))
		require.NoError(t, err)
		require.Equal(t, srcRaw, dstRaw)
	}
}

func TestCopyProgress(t *testing.T) {
	t.Run("resume_after_crash", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		copyTestSeed(t, loader)
		faults := &copyFaultStore{BlobStore: loader.BlobStore, failAt: 2}
		loader.BlobStore = faults

		err := loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1")
		require.Error(t, err, "first attempt must stop at the injected fault")

		st, err := loader.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, 2, st.Total)
		require.Equal(t, 1, st.Copied)
		require.Equal(t, 1, st.PendingEntries)

		// Crash: drop the KB instance, resume from the durable journal with
		// a fresh instance over the same store.
		faults.heal()
		loader2 := NewKB(loader.BlobStore, t.TempDir())
		require.NoError(t, loader2.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1"))

		st, err = loader2.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, 2, st.Copied)
		require.Equal(t, 0, st.PendingEntries)
		requireCopyComplete(t, loader2)
	})

	t.Run("outage_resume", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		copyTestSeed(t, loader)
		faults := &copyFaultStore{BlobStore: loader.BlobStore, failAll: true}
		loader.BlobStore = faults

		err := loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1")
		require.Error(t, err, "total outage must fail the attempt")

		st, err := loader.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, 2, st.PendingEntries)
		require.Equal(t, 0, st.Copied)
		require.NotEmpty(t, st.LastError)

		faults.heal()
		require.NoError(t, loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1"))
		st, err = loader.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, 2, st.Copied)
		require.Equal(t, 0, st.PendingEntries)
		requireCopyComplete(t, loader)
	})

	t.Run("status_observable", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		copyTestSeed(t, loader)
		faults := &copyFaultStore{BlobStore: loader.BlobStore, failAt: 2}
		loader.BlobStore = faults

		require.Error(t, loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1"))

		st, err := loader.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, 2, st.Total)
		require.Equal(t, 1, st.PendingEntries)
		require.Greater(t, st.PendingBytes, int64(0))
		require.Greater(t, st.BytesCopied, int64(0))
		require.Equal(t, st.BytesCopied+st.PendingBytes, st.BytesTotal)
		require.NotEmpty(t, st.LastError)

		_, err = loader.CopyProgress(ctx, "dst", "missing")
		require.Error(t, err)

		faults.heal()
		require.NoError(t, loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1"))
	})
}
