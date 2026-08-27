package kb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testLiveKey      = "target.duckdb.shards/0123456789abcdef/shard-00000.duckdb"
	testOrphanKey    = "target.duckdb.shards/fedcba9876543210/shard-00000.duckdb"
	testCompactedKey = "target.duckdb.compacted/compact-1756300000000000000/part-00000"
)

func writeShardBlob(t *testing.T, root, key string, modTime time.Time) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(key))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(key), 0o644))
	require.NoError(t, os.Chtimes(path, modTime, modTime))
}

func newOrphanKB(t *testing.T) (*KB, string) {
	t.Helper()
	root := t.TempDir()
	return NewKB(&LocalBlobStore{Root: root}, t.TempDir()), root
}

func seedShardManifest(t *testing.T, loader *KB, kbID string, shards []SnapshotShardMetadata) {
	t.Helper()
	_, err := loader.ManifestStore.UpsertIfMatch(context.Background(), kbID, SnapshotShardManifest{
		SchemaVersion: 1,
		Layout:        ShardManifestLayoutDuckDBs,
		KBID:          kbID,
		Shards:        shards,
	}, "")
	require.NoError(t, err)
}

func TestOrphanSelection(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-DefaultOrphanedShardGracePeriod)
	stale := now.Add(-24 * time.Hour)
	active := []SnapshotShardMetadata{{Key: testLiveKey}}

	t.Run("stale_unreferenced", func(t *testing.T) {
		objects := []BlobObjectInfo{
			{Key: testLiveKey, UpdatedAt: stale},
			{Key: testOrphanKey, UpdatedAt: stale, Size: 42},
			{Key: "target.duckdb.shards/aaaaaaaaaaaaaaaa/shard-00000.duckdb", UpdatedAt: now.Add(-time.Minute)},
			{Key: "target.duckdb.shards/bbbbbbbbbbbbbbbb/shard-00000.duckdb"},
			{Key: ""},
		}

		orphaned := orphanedShardBlobs(objects, active, cutoff, ShardBlobPrefix("target"))

		require.Len(t, orphaned, 1)
		require.Equal(t, testOrphanKey, orphaned[0].Key)
		require.Equal(t, int64(42), orphaned[0].SizeBytes)
	})

	// A kbID can itself contain a shard prefix, so a listing returns keys
	// owned by another knowledge base.
	t.Run("foreign_and_malformed", func(t *testing.T) {
		objects := []BlobObjectInfo{
			{Key: "target.duckdb.shards/tenant2.duckdb.shards/0123456789abcdef/shard-00000.duckdb", UpdatedAt: stale},
			{Key: "target.duckdb.shards/notahash/shard-00000.duckdb", UpdatedAt: stale},
			{Key: "target.duckdb.shards/0123456789abcdef/manifest.json", UpdatedAt: stale},
			{Key: "target.duckdb.shards/0123456789abcdef/nested/shard-00000.duckdb", UpdatedAt: stale},
		}

		require.Empty(t, orphanedShardBlobs(objects, nil, cutoff, ShardBlobPrefix("target")))
	})

	t.Run("compacted_parts", func(t *testing.T) {
		objects := []BlobObjectInfo{
			{Key: testCompactedKey, UpdatedAt: stale},
			{Key: "target.duckdb.compacted/compact-1/bogus-00000", UpdatedAt: stale},
		}

		orphaned := orphanedShardBlobs(objects, nil, cutoff, "target.duckdb.compacted/")

		require.Len(t, orphaned, 1)
		require.Equal(t, testCompactedKey, orphaned[0].Key)
	})
}

func TestShardReconcile(t *testing.T) {
	ctx := context.Background()

	t.Run("queues_both_namespaces", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		stale := now.Add(-24 * time.Hour)

		writeShardBlob(t, root, testLiveKey, stale)
		writeShardBlob(t, root, testOrphanKey, stale)
		writeShardBlob(t, root, testCompactedKey, stale)
		writeShardBlob(t, root, "target-other.duckdb.shards/fedcba9876543210/shard-00000.duckdb", stale)

		active := []SnapshotShardMetadata{{Key: testLiveKey}}
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", active, now))

		queued := map[string]bool{}
		for _, entry := range loader.shardGC {
			queued[entry.Shard.Key] = true
		}
		require.Equal(t, map[string]bool{testOrphanKey: true, testCompactedKey: true}, queued)
	})

	// A busy KB republishes constantly, and a moving deadline is never reached.
	t.Run("keeps_deadline", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))

		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		require.Equal(t, 1, loader.shardGCPendingCount())
		deadline := loader.shardGC[0].NotBefore

		for round := 1; round <= 3; round++ {
			later := now.Add(time.Duration(round) * shardReconcileInterval)
			require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, later))
			require.Equal(t, 1, loader.shardGCPendingCount())
			require.Equal(t, deadline, loader.shardGC[0].NotBefore)
		}
	})

	t.Run("throttled", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))

		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		require.Equal(t, 1, loader.shardGCPendingCount())

		loader.shardGC = nil
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now.Add(time.Minute)))
		require.Equal(t, 0, loader.shardGCPendingCount())

		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now.Add(shardReconcileInterval)))
		require.Equal(t, 1, loader.shardGCPendingCount())
	})

	t.Run("releases_throttle_on_error", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))

		working := loader.BlobStore
		loader.BlobStore = listAlwaysFails{BlobStore: working}
		require.Error(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		require.Equal(t, 0, loader.shardGCPendingCount())
		require.Equal(t, uint64(1), loader.shardMetrics.Snapshot()["target"].ReconcileFailuresTotal)

		loader.BlobStore = working
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now.Add(time.Second)))
		require.Equal(t, 1, loader.shardGCPendingCount())
	})

	// A generation ages past the grace period long after its publish finished.
	t.Run("scheduled_without_publishing", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testLiveKey, now.Add(-24*time.Hour))
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		seedShardManifest(t, loader, "target", []SnapshotShardMetadata{{Key: testLiveKey}})

		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 1, result.Deleted)

		_, err = loader.BlobStore.Head(ctx, testOrphanKey)
		require.Error(t, err)
		_, err = loader.BlobStore.Head(ctx, testLiveKey)
		require.NoError(t, err)
	})

	// Deleting a KB drops its manifest first, so stragglers have none at all.
	t.Run("scheduled_collects_manifestless_kb", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		writeShardBlob(t, root, testCompactedKey, now.Add(-24*time.Hour))

		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 2, result.Deleted)
	})

	t.Run("scheduled_ignores_per_kb_throttle", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		seedShardManifest(t, loader, "target", nil)
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		loader.shardGC = nil

		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now.Add(time.Minute)))
		require.Equal(t, 1, loader.shardGCPendingCount())
	})

	// Keys sort the same every scan, so restarting at the head starves the tail.
	t.Run("scheduled_resumes_after_deadline", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		stale := now.Add(-24 * time.Hour)

		for _, kbID := range []string{"aaa", "bbb", "ccc"} {
			writeShardBlob(t, root, kbID+".duckdb.shards/fedcba9876543210/shard-00000.duckdb", stale)
		}

		stopped, cancel := context.WithCancel(ctx)
		working := loader.BlobStore
		loader.BlobStore = cancelAfterList{BlobStore: working, cancel: cancel}
		require.Error(t, loader.ReconcileShardBlobsForAllKBs(stopped, now))
		require.Equal(t, "aaa", loader.shardScanCursor())
		require.Equal(t, 0, loader.shardGCPendingCount())

		loader.BlobStore = working
		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now.Add(shardReconcileInterval)))
		require.Equal(t, 3, loader.shardGCPendingCount())
		require.Empty(t, loader.shardScanCursor())
	})

	t.Run("scheduled_releases_scan_on_error", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		seedShardManifest(t, loader, "target", nil)

		loader.ManifestStore = manifestGetFails{ManifestStore: loader.ManifestStore}
		require.Error(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))
		require.Equal(t, 0, loader.shardGCPendingCount())
		require.Equal(t, uint64(1), loader.shardMetrics.Snapshot()["target"].ReconcileFailuresTotal)

		loader.ManifestStore = newOrphanManifestStore(t, loader)
		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now.Add(time.Second)))
		require.Equal(t, 1, loader.shardGCPendingCount())
	})

	t.Run("scheduled_records_scan_failure", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))

		working := loader.BlobStore
		loader.BlobStore = listAlwaysFails{BlobStore: working}
		require.Error(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))
		require.Equal(t, uint64(1), loader.shardMetrics.Snapshot()[scanMetricsKey].ReconcileFailuresTotal)

		loader.BlobStore = working
		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now.Add(time.Second)))
		require.Equal(t, 1, loader.shardGCPendingCount())
	})

	t.Run("scheduled_throttled", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		seedShardManifest(t, loader, "target", nil)

		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))
		require.Equal(t, 1, loader.shardGCPendingCount())

		loader.shardGC = nil
		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now.Add(time.Minute)))
		require.Equal(t, 0, loader.shardGCPendingCount())
	})
}

func TestShardGCSweep(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes_orphan", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testLiveKey, now.Add(-24*time.Hour))
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))

		active := []SnapshotShardMetadata{{Key: testLiveKey}}
		seedShardManifest(t, loader, "target", active)
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", active, now))

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 1, result.Deleted)
		require.Equal(t, 0, result.Pending)

		_, err = loader.BlobStore.Head(ctx, testOrphanKey)
		require.Error(t, err)
		_, err = loader.BlobStore.Head(ctx, testLiveKey)
		require.NoError(t, err)
	})

	t.Run("spares_referenced", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testLiveKey, now.Add(-24*time.Hour))
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		require.Equal(t, 1, loader.shardGCPendingCount())

		seedShardManifest(t, loader, "target", []SnapshotShardMetadata{{Key: testLiveKey}})

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 0, result.Deleted)
		require.Equal(t, 1, result.Retried)

		_, err = loader.BlobStore.Head(ctx, testLiveKey)
		require.NoError(t, err)
	})

	// Content-addressed keys make a republish byte-identical to what it replaced.
	t.Run("spares_restaged", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		require.Equal(t, 1, loader.shardGCPendingCount())

		writeShardBlob(t, root, testOrphanKey, now)

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 0, result.Deleted)
		require.Equal(t, 1, result.Retried)

		_, err = loader.BlobStore.Head(ctx, testOrphanKey)
		require.NoError(t, err)
	})

	t.Run("spares_republished_after_cached_read", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testLiveKey, now.Add(-24*time.Hour))
		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		active := []SnapshotShardMetadata{{Key: testLiveKey}}
		seedShardManifest(t, loader, "target", active)
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", active, now))

		loader.ManifestStore = &republishOnSecondGet{
			ManifestStore: loader.ManifestStore,
			next: SnapshotShardManifest{
				SchemaVersion: 1,
				Layout:        ShardManifestLayoutDuckDBs,
				KBID:          "target",
				Shards:        []SnapshotShardMetadata{{Key: testLiveKey}, {Key: testOrphanKey}},
			},
		}

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 0, result.Deleted)
		require.Equal(t, 1, result.Retried)

		_, err = loader.BlobStore.Head(ctx, testOrphanKey)
		require.NoError(t, err)
	})

	// Write time is the only signal that a publish may still be staging.
	t.Run("spares_unknown_write_time", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testOrphanKey, now.Add(-24*time.Hour))
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target", nil, now))
		loader.BlobStore = headWithoutTime{BlobStore: loader.BlobStore}

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 0, result.Deleted)
		require.Equal(t, 1, result.Retried)
	})

	// A shard observed being replaced already has its manifest flip behind it,
	// so it must not also wait out the orphan grace period.
	t.Run("deletes_replaced_without_orphan_grace", func(t *testing.T) {
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()

		writeShardBlob(t, root, testOrphanKey, now)
		seedShardManifest(t, loader, "target", nil)
		loader.EnqueueReplacedShardsForGC("target", []SnapshotShardMetadata{{Key: testOrphanKey}}, now)

		result, err := loader.SweepDelayedShardGC(ctx, now.Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 1, result.Deleted)
	})
}

func TestManifestKey(t *testing.T) {
	t.Run("suffix_matches_builder", func(t *testing.T) {
		require.Equal(t, "target"+shardManifestSuffix, ShardManifestKey("target"))
	})

	t.Run("parses_kb_id", func(t *testing.T) {
		kbID, ok := KBIDFromManifestKey("target" + shardManifestSuffix)
		require.True(t, ok)
		require.Equal(t, "target", kbID)
	})

	t.Run("rejects_non_manifest_keys", func(t *testing.T) {
		for _, key := range []string{
			testLiveKey,
			"nested/target" + shardManifestSuffix,
			shardManifestSuffix,
			"target.snapshot.json",
		} {
			_, ok := KBIDFromManifestKey(key)
			require.False(t, ok, key)
		}
	})
}

var errListUnavailable = errors.New("list unavailable")

var errManifestUnavailable = errors.New("manifest unavailable")

type cancelAfterList struct {
	BlobStore
	cancel context.CancelFunc
}

func (c cancelAfterList) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	objects, err := c.BlobStore.List(ctx, prefix)
	c.cancel()
	return objects, err
}

type manifestGetFails struct{ ManifestStore }

func (manifestGetFails) Get(context.Context, string) (*ManifestDocument, error) {
	return nil, errManifestUnavailable
}

func newOrphanManifestStore(t *testing.T, loader *KB) ManifestStore {
	t.Helper()
	return &BlobManifestStore{Store: loader.BlobStore}
}

type listAlwaysFails struct{ BlobStore }

func (listAlwaysFails) List(context.Context, string) ([]BlobObjectInfo, error) {
	return nil, errListUnavailable
}

type headWithoutTime struct{ BlobStore }

func (h headWithoutTime) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	info, err := h.BlobStore.Head(ctx, key)
	if err != nil {
		return nil, err
	}
	info.UpdatedAt = time.Time{}
	return info, nil
}

// Stands in for a publish landing between the sweep's cached read and its
// delete.
type republishOnSecondGet struct {
	ManifestStore
	next  SnapshotShardManifest
	calls int
}

func (m *republishOnSecondGet) Get(ctx context.Context, kbID string) (*ManifestDocument, error) {
	m.calls++
	if m.calls > 1 {
		return &ManifestDocument{Manifest: m.next}, nil
	}
	return m.ManifestStore.Get(ctx, kbID)
}
