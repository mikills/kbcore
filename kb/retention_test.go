package kb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type retentionListFailStore struct {
	BlobStore
}

func (retentionListFailStore) List(context.Context, string) ([]BlobObjectInfo, error) {
	return nil, errors.New("list boom")
}

func retentionSeed(t *testing.T, loader *KB, kbID string, clock *FakeClock) {
	t.Helper()
	advSeedKB(t, loader, kbID, map[string]string{"s": "x"})
}

func TestRetentionSchedule(t *testing.T) {
	t.Run("keeps_newest_on_failure", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
		loader, _ := newOrphanKB(t)
		loader.Clock = clock
		retentionSeed(t, loader, "kb", clock)
		_, err := loader.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		clock.Advance(time.Hour)
		_, err = loader.CreateBackup(ctx, "kb", "b2")
		require.NoError(t, err)

		working := loader.BlobStore
		loader.BlobStore = retentionListFailStore{BlobStore: working}
		deleted, err := loader.SweepRetention(ctx, "kb", RetentionPolicy{KeepLastN: 0})
		require.Error(t, err, "partial list failure must abort the sweep")
		require.Empty(t, deleted, "delete==0 on any list/verify failure")

		loader.BlobStore = working
		got, err := loader.GetBackup(ctx, "kb", "b2")
		require.NoError(t, err, "newest valid recovery point must survive the failed sweep")
		require.NoError(t, ValidateBackupDescriptor(got))
	})

	t.Run("prunes_expired", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
		loader, _ := newOrphanKB(t)
		loader.Clock = clock
		retentionSeed(t, loader, "kb", clock)
		_, err := loader.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		clock.Advance(2 * time.Hour)
		_, err = loader.CreateBackup(ctx, "kb", "b2")
		require.NoError(t, err)

		deleted, err := loader.SweepRetention(ctx, "kb", RetentionPolicy{KeepLastN: 1, MaxAge: time.Hour})
		require.NoError(t, err)
		require.Equal(t, []string{"b1"}, deleted)

		ids, err := loader.ListBackupIDs(ctx, "kb")
		require.NoError(t, err)
		require.Equal(t, []string{"b2"}, ids)
	})

	t.Run("corrupt_skipped", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
		loader, _ := newOrphanKB(t)
		loader.Clock = clock
		retentionSeed(t, loader, "kb", clock)
		_, err := loader.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		clock.Advance(time.Hour)
		_, err = loader.CreateBackup(ctx, "kb", "b2")
		require.NoError(t, err)

		raw, err := loader.BlobStore.DownloadBytes(ctx, BackupDescriptorKey("kb", "b1"))
		require.NoError(t, err)
		raw = append(raw, byte('}'))
		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, BackupDescriptorKey("kb", "b1"), raw, "")
		require.NoError(t, err)

		deleted, err := loader.SweepRetention(ctx, "kb", RetentionPolicy{KeepLastN: 1})
		require.NoError(t, err, "corrupt markers are skipped, not fatal")
		require.Empty(t, deleted, "corrupt descriptors are never deleted by retention")

		ids, err := loader.ListBackupIDs(ctx, "kb")
		require.NoError(t, err)
		require.Contains(t, ids, "b1", "corrupt descriptor must be left for the operator")
		require.Contains(t, ids, "b2")
	})

	t.Run("empty_noop", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		deleted, err := loader.SweepRetention(ctx, "kb", RetentionPolicy{KeepLastN: 1})
		require.NoError(t, err)
		require.Empty(t, deleted)

		s := NewScheduler(NewInMemoryWriteLeaseManager(), time.Minute, nil, nil)
		require.NoError(t, loader.RegisterDefaultJobs(s))
		require.Contains(t, s.JobIDs(), RetentionJobID)
		outcome, err := s.RunOnce(ctx, RetentionJobID)
		require.NoError(t, err)
		require.Equal(t, SchedulerOutcomeSuccess, outcome)
	})
}
