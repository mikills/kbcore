package kb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/stretchr/testify/require"
)

// fallbackOnlyStore forces the copyShardServerSide fallback by
// reporting Copy as unimplemented while delegating everything else.
type fallbackOnlyStore struct {
	BlobStore
}

func (s *fallbackOnlyStore) Copy(ctx context.Context, srcKey, dstKey string, opts blobstore.CopyOptions) (*blobstore.ObjectInfo, error) {
	return nil, errors.New("Copy not supported by this store")
}

func TestCopyDurability(t *testing.T) {
	t.Run("nil_clock_fails_closed", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		copyTestSeed(t, loader)
		loader.Clock = nil
		_, err := loader.SweepRetention(ctx, "src", DefaultRetentionPolicy())
		require.ErrorContains(t, err, "clock is not configured")
		require.Error(t, loader.AddRefOwners(ctx, []string{"k"}, "o1"))
		_, err = loader.BranchKB(ctx, "src", "b9", "dst9")
		require.ErrorContains(t, err, "clock is not configured")
		require.Error(t, loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c9"))
	})

	t.Run("copy_resume_collision", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		copyTestSeed(t, loader)
		require.NoError(t, loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1"))
		_, err := loader.CreateBackup(ctx, "src", "b2")
		require.NoError(t, err)
		err = loader.CopyBackupWithProgress(ctx, "src", "b2", "dst", "c1")
		require.ErrorIs(t, err, ErrBackupExists)
	})

	t.Run("copy_fallback_rehash", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		copyTestSeed(t, loader)
		loader.BlobStore = &fallbackOnlyStore{BlobStore: loader.BlobStore}
		require.NoError(t, loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1"))
		st, err := loader.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, 2, st.Copied)
		require.Equal(t, 0, st.PendingEntries)
		requireCopyComplete(t, loader)
	})

	t.Run("copy_concurrent_no_loss", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		loader.Clock = NewFakeClock(time.Unix(0, 0).UTC())
		copyTestSeed(t, loader)
		const racers = 4
		var wg sync.WaitGroup
		errs := make([]error, racers)
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = loader.CopyBackupWithProgress(ctx, "src", "b1", "dst", "c1")
			}(i)
		}
		wg.Wait()
		// Purely concurrent phase must converge with no state loss:
		// every winner-staged shard is adopted and merged, no serial
		// heal before asserting.
		for _, err := range errs {
			require.NoError(t, err)
		}
		st, err := loader.CopyProgress(ctx, "dst", "c1")
		require.NoError(t, err)
		require.Equal(t, st.Total, st.Copied)
		require.Equal(t, 0, st.PendingEntries)
		requireCopyComplete(t, loader)
	})

	t.Run("reftable_concurrent_no_loss", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		const owners = 8
		var wg sync.WaitGroup
		errs := make([]error, owners)
		for i := 0; i < owners; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = loader.AddRefOwners(ctx, []string{"shard-k"}, string(rune('a'+i)))
			}(i)
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
		got := loader.refOwnersOf(ctx, "shard-k")
		require.Len(t, got, owners)
	})

	t.Run("delete_corrupt_sweeps_owners", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)
		// Legacy fan-out marker under another owner with the same branch ID.
		writeLegacyBranchMarker(t, loader, "oth", "b1", "dst2", manifest.Shards[0].Key)
		// Corrupt the lookup marker.
		raw, err := loader.BlobStore.DownloadBytes(ctx, BranchRecordKey("src", "b1"))
		require.NoError(t, err)
		raw = append(raw, byte('}'))
		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, BranchRecordKey("src", "b1"), raw, "")
		require.NoError(t, err)

		require.NoError(t, loader.DeleteBranch(ctx, "src", "b1"))
		_, err = loader.GetBranch(ctx, "src", "b1")
		require.ErrorIs(t, err, ErrBackupNotFound)
		_, err = loader.GetBranch(ctx, "oth", "b1")
		require.ErrorIs(t, err, ErrBackupNotFound)
		require.Empty(t, loader.refOwnersOf(ctx, manifest.Shards[0].Key))
	})
}
