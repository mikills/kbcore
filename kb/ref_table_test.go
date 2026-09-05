package kb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dropLiveShards rewrites the KB manifest to reference nothing, so the given
// shards look replaced to GC while pins decide their fate.
func dropLiveShards(t *testing.T, loader *KB, kbID string) {
	t.Helper()
	ctx := context.Background()
	doc, err := loader.ManifestStore.Get(ctx, kbID)
	require.NoError(t, err)
	next := doc.Manifest
	next.Shards = nil
	_, err = loader.ManifestStore.UpsertIfMatch(ctx, kbID, next, doc.Version)
	require.NoError(t, err)
}

// writeLegacyBranchMarker persists a pre-ref-table fan-out marker directly
// under one owner prefix, with no ref-table entry.
func writeLegacyBranchMarker(t *testing.T, loader *KB, ownerKBID, branchID, targetKBID, shardKey string) {
	t.Helper()
	ctx := context.Background()
	rec := &BranchRecord{
		RecordVersion:         BranchRecordVersion,
		BranchID:              branchID,
		SourceKBID:            ownerKBID,
		TargetKBID:            targetKBID,
		SourceManifestVersion: "v1",
		CreatedAt:             time.Now().UTC(),
		Shards:                []BackupShardRef{{Key: shardKey, SizeBytes: 1, SHA256: "ab", Version: "v"}},
	}
	sum, err := branchRecordChecksum(rec)
	require.NoError(t, err)
	rec.RecordSHA256 = sum
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	_, err = loader.BlobStore.UploadBytesIfMatch(ctx, BranchRecordKey(ownerKBID, branchID), data, "")
	require.NoError(t, err)
}

func TestRefTable(t *testing.T) {
	t.Run("shared_pin", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "alpha"})
		dropped := manifest.Shards[0]

		_, err := loader.BranchKB(ctx, "src", "b1", "d1")
		require.NoError(t, err)
		_, err = loader.BranchKB(ctx, "src", "b2", "d2")
		require.NoError(t, err)
		require.Len(t, loader.refOwnersOf(ctx, dropped.Key), 2)

		dropLiveShards(t, loader, "src")
		now := time.Now().UTC()
		loader.EnqueueReplacedShardsForGC("src", []SnapshotShardMetadata{{Key: dropped.Key}}, now.Add(-time.Hour))

		require.NoError(t, loader.DeleteBranch(ctx, "src", "b1"))
		res, err := loader.SweepDelayedShardGC(ctx, now)
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "shard shared with b2 must survive b1's release")
		_, err = loader.BlobStore.Head(ctx, dropped.Key)
		require.NoError(t, err)
	})

	t.Run("release_on_all_gone", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "alpha"})
		dropped := manifest.Shards[0]

		_, err := loader.BranchKB(ctx, "src", "b1", "d1")
		require.NoError(t, err)
		_, err = loader.BranchKB(ctx, "src", "b2", "d2")
		require.NoError(t, err)

		dropLiveShards(t, loader, "src")
		now := time.Now().UTC()
		loader.EnqueueReplacedShardsForGC("src", []SnapshotShardMetadata{{Key: dropped.Key}}, now.Add(-time.Hour))

		require.NoError(t, loader.DeleteBranch(ctx, "src", "b1"))
		require.NoError(t, loader.DeleteBranch(ctx, "src", "b2"))
		require.Empty(t, loader.refOwnersOf(ctx, dropped.Key))

		res, err := loader.SweepDelayedShardGC(ctx, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted, "shard with no remaining owners must be collectible")
	})

	t.Run("migration_reads_old_markers", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "alpha"})
		pinned := manifest.Shards[0].Key

		// Legacy fan-out marker only: no ref-table entry exists.
		writeLegacyBranchMarker(t, loader, "src", "legacy1", "dst", pinned)
		pins, err := loader.refTablePinnedKeys(ctx)
		require.NoError(t, err)
		require.NotContains(t, pins, pinned)

		dropLiveShards(t, loader, "src")
		now := time.Now().UTC()
		loader.EnqueueReplacedShardsForGC("src", []SnapshotShardMetadata{{Key: pinned}}, now.Add(-time.Hour))
		res, err := loader.SweepDelayedShardGC(ctx, now)
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "legacy fan-out marker must still pin the shard")

		require.NoError(t, loader.DeleteBranch(ctx, "src", "legacy1"))
		res, err = loader.SweepDelayedShardGC(ctx, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
	})
}
