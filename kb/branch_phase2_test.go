package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/stretchr/testify/require"
)

func branchSeed(t *testing.T, loader *KB, kbID string, bodies map[string]string) SnapshotShardManifest {
	t.Helper()
	metas := make([]SnapshotShardMetadata, 0, len(bodies))
	i := 0
	for shardID, body := range bodies {
		key := fmt.Sprintf("%s.duckdb.shards/abc123def4567890/shard-%05d.duckdb", kbID, i)
		info, err := loader.BlobStore.UploadBytesIfMatch(context.Background(), key, []byte(body), "")
		require.NoError(t, err)
		sum := sha256.Sum256([]byte(body))
		metas = append(metas, SnapshotShardMetadata{
			ShardID:   shardID,
			Key:       key,
			Version:   info.Version,
			SizeBytes: int64(len(body)),
			SHA256:    hex.EncodeToString(sum[:]),
		})
		i++
	}
	manifest := SnapshotShardManifest{
		SchemaVersion: 1,
		Layout:        ShardManifestLayoutDuckDBs,
		FormatKind:    "duckdb_sharded",
		FormatVersion: 2,
		KBID:          kbID,
		CreatedAt:     time.Now().UTC(),
		Shards:        metas,
	}
	_, err := loader.ManifestStore.UpsertIfMatch(context.Background(), kbID, manifest, "")
	require.NoError(t, err)
	return manifest
}

func TestBranch(t *testing.T) {
	t.Run("zero_copy_shares_keys", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		src := branchSeed(t, loader, "src", map[string]string{"s0": "alpha", "s1": "beta"})

		before, err := loader.BlobStore.List(ctx, "src.duckdb.shards/")
		require.NoError(t, err)

		rec, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)
		require.Equal(t, "src", rec.SourceKBID)
		require.Equal(t, "dst", rec.TargetKBID)
		require.NotEmpty(t, rec.RecordSHA256)

		dstDoc, err := loader.ManifestStore.Get(ctx, "dst")
		require.NoError(t, err)
		require.Equal(t, "dst", dstDoc.Manifest.KBID)
		require.Len(t, dstDoc.Manifest.Shards, len(src.Shards))
		for i, shard := range dstDoc.Manifest.Shards {
			require.Equal(t, src.Shards[i].Key, shard.Key, "branch must reference source keys verbatim")
		}

		after, err := loader.BlobStore.List(ctx, "src.duckdb.shards/")
		require.NoError(t, err)
		require.Len(t, after, len(before), "branch must not copy shard bytes")

		got, err := loader.GetBranch(ctx, "src", "b1")
		require.NoError(t, err)
		require.Equal(t, rec.RecordSHA256, got.RecordSHA256)
	})

	t.Run("branch_of_branch", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "a", map[string]string{"s0": "alpha"})

		first, err := loader.BranchKB(ctx, "a", "b1", "b")
		require.NoError(t, err)
		require.Empty(t, first.ParentBranchID)

		second, err := loader.BranchKBFrom(ctx, "b", "b2", "c", "b1")
		require.NoError(t, err)
		require.Equal(t, "b1", second.ParentBranchID)
		require.Equal(t, "b", second.SourceKBID)
		require.Equal(t, "c", second.TargetKBID)

		// b's manifest references a's keys, so c's pin fans out to a.
		pinned, err := loader.GetBranch(ctx, "a", "b2")
		require.NoError(t, err)
		require.Equal(t, "c", pinned.TargetKBID)

		cDoc, err := loader.ManifestStore.Get(ctx, "c")
		require.NoError(t, err)
		aDoc, err := loader.ManifestStore.Get(ctx, "a")
		require.NoError(t, err)
		require.Equal(t, aDoc.Manifest.Shards[0].Key, cDoc.Manifest.Shards[0].Key)
	})

	t.Run("delete_blocked", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)

		blocked, err := loader.HasBackupsOrBranches(ctx, "src")
		require.NoError(t, err)
		require.True(t, blocked)

		err = loader.DeleteKnowledgeBase(ctx, "src")
		require.ErrorIs(t, err, ErrDeleteBlockedByBackups)

		require.NoError(t, loader.DeleteBranch(ctx, "src", "b1"))
		require.NoError(t, loader.DeleteKnowledgeBase(ctx, "src"))
	})

	t.Run("concurrent_branch", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		const racers = 4
		errs := make([]error, racers)
		var wg sync.WaitGroup
		for i := range racers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = loader.BranchKB(ctx, "src", "b1", "dst")
			}(i)
		}
		wg.Wait()
		wins := 0
		for _, err := range errs {
			if err == nil {
				wins++
				continue
			}
			require.ErrorIs(t, err, ErrBackupExists)
		}
		require.Equal(t, 1, wins, "exactly one concurrent branch must win")
	})

	t.Run("delete_race", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		var wg sync.WaitGroup
		var branchErr, deleteErr error
		wg.Add(2)
		go func() { defer wg.Done(); _, branchErr = loader.BranchKB(ctx, "src", "b1", "dst") }()
		go func() { defer wg.Done(); deleteErr = loader.DeleteKnowledgeBase(ctx, "src") }()
		wg.Wait()
		if deleteErr != nil {
			require.ErrorIs(t, deleteErr, ErrDeleteBlockedByBackups)
		}
		if branchErr != nil {
			require.True(t, isErrIn(branchErr, ErrManifestNotFound, ErrBackupExists),
				"branch loser must fail fenced, got %v", branchErr)
		}
		// Either the branch won (delete fenced) or the delete won (branch
		// found no source); both leave a consistent state.
		if branchErr == nil {
			_, err := loader.ManifestStore.Get(ctx, "dst")
			require.NoError(t, err)
		}
	})

	t.Run("corrupt", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)

		raw, err := loader.BlobStore.DownloadBytes(ctx, BranchRecordKey("src", "b1"))
		require.NoError(t, err)
		raw = append(raw, byte('}'))
		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, BranchRecordKey("src", "b1"), raw, "")
		require.NoError(t, err)
		_, err = loader.GetBranch(ctx, "src", "b1")
		require.ErrorIs(t, err, ErrBackupCorrupt)

		// A corrupt marker must not wedge GC: the live manifest still
		// protects its shards and unrelated orphans still sweep.
		stale := time.Now().UTC().Add(-24 * time.Hour)
		writeShardBlob(t, loader.BlobStore.(*LocalBlobStore).Root,
			"src.duckdb.shards/fedcba9876543210/shard-00000.duckdb", stale)
		live := []SnapshotShardMetadata{{Key: "src.duckdb.shards/abc123def4567890/shard-00000.duckdb"}}
		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "src", live, time.Now().UTC()))
		res, err := loader.SweepDelayedShardGC(ctx, time.Now().UTC().Add(DefaultShardGCGraceWindow+time.Second))
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
	})

	t.Run("gc_spares_then_releases", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "alpha", "s1": "beta"})
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)

		dropped := manifest.Shards[0]
		live := manifest
		live.Shards = manifest.Shards[1:]
		doc, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		_, err = loader.ManifestStore.UpsertIfMatch(ctx, "src", live, doc.Version)
		require.NoError(t, err)

		now := time.Now().UTC()
		loader.EnqueueReplacedShardsForGC("src", []SnapshotShardMetadata{dropped}, now.Add(-time.Hour))
		res, err := loader.SweepDelayedShardGC(ctx, now)
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "branch-pinned shard must survive GC")

		require.NoError(t, loader.DeleteBranch(ctx, "src", "b1"))
		res, err = loader.SweepDelayedShardGC(ctx, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
	})

	t.Run("tombstone_target", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		require.NoError(t, loader.TombstoneKnowledgeBase(ctx, "dst", "test"))
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.ErrorIs(t, err, ErrKBTombstoned)
		_, err = loader.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
		require.Error(t, err)
	})
}

func TestReachability(t *testing.T) {
	t.Run("manifestless_pins_spared", func(t *testing.T) {
		ctx := context.Background()
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		stale := now.Add(-24 * time.Hour)
		branchSeed(t, loader, "src", map[string]string{"s0": "alpha"})

		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)
		doc, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		pinnedKey := doc.Manifest.Shards[0].Key
		writeShardBlob(t, root, pinnedKey, stale)

		// Bypass the delete guard the way a crashed deleter would: the
		// manifest is gone but the branch marker still pins the bytes.
		require.NoError(t, loader.ManifestStore.Delete(ctx, "src"))

		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))
		require.Equal(t, 0, loader.shardGCPendingCount(), "pinned bytes must not queue as orphans")

		loader.EnqueueReplacedShardsForGC("src", []SnapshotShardMetadata{{Key: pinnedKey}}, now.Add(-time.Hour))
		res, err := loader.SweepDelayedShardGC(ctx, now)
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "missing manifest must not read as empty while pins exist")
		_, err = loader.BlobStore.Head(ctx, pinnedKey)
		require.NoError(t, err)
	})

	t.Run("tombstone_skips_reconcile", func(t *testing.T) {
		ctx := context.Background()
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		writeShardBlob(t, root, "src.duckdb.shards/fedcba9876543210/shard-00000.duckdb", now.Add(-24*time.Hour))
		require.NoError(t, loader.TombstoneKnowledgeBase(ctx, "src", "test"))

		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now))
		require.Equal(t, 0, loader.shardGCPendingCount())

		require.NoError(t, loader.ClearTombstone(ctx, "src"))
		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, now.Add(shardReconcileInterval)))
		require.Equal(t, 1, loader.shardGCPendingCount())
	})

	t.Run("reads_pin", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
		loader, _ := newOrphanKB(t)
		loader.Clock = clock
		manifest := branchSeed(t, loader, "kb", map[string]string{"s0": "x"})
		held := manifest.Shards[0].Key

		loader.PinShardForRead("kb", held)
		loader.EnqueueReplacedShardsForGC("kb", []SnapshotShardMetadata{{Key: held}}, clock.Now())
		// Compaction published a manifest that drops the held shard while
		// the read is still open.
		doc, err := loader.ManifestStore.Get(ctx, "kb")
		require.NoError(t, err)
		dropped := doc.Manifest
		dropped.Shards = nil
		_, err = loader.ManifestStore.UpsertIfMatch(ctx, "kb", dropped, doc.Version)
		require.NoError(t, err)
		clock.Advance(DefaultShardGCGraceWindow + time.Second)
		res, err := loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "pinned shard is exempt from every GC timer")

		loader.UnpinShardForRead("kb", held)
		clock.Advance(DefaultShardGCRetryDelay + time.Second)
		res, err = loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
	})

	t.Run("journal_pendings_pin", func(t *testing.T) {
		ctx := context.Background()
		loader, root := newOrphanKB(t)
		now := time.Now().UTC()
		stale := now.Add(-24 * time.Hour)
		liveKey := "kb.duckdb.shards/0123456789abcdef/shard-00000.duckdb"
		pendingKey := "kb.duckdb.shards/aaaaaaaaaaaaaaaa/shard-00000.duckdb"
		orphanKey := "kb.duckdb.shards/fedcba9876543210/shard-00000.duckdb"
		writeShardBlob(t, root, liveKey, stale)
		writeShardBlob(t, root, pendingKey, stale)
		writeShardBlob(t, root, orphanKey, stale)
		loader.BlobStore = &pendingKeysStore{
			BlobStore: loader.BlobStore,
			pending:   []string{pendingKey},
		}

		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "kb", []SnapshotShardMetadata{{Key: liveKey}}, now))
		queued := map[string]bool{}
		for _, entry := range loader.shardGC {
			queued[entry.Shard.Key] = true
		}
		require.Equal(t, map[string]bool{orphanKey: true}, queued)
	})

	t.Run("crash_recovery", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC))
		loader, root := newOrphanKB(t)
		loader.Clock = clock
		branchSeed(t, loader, "target", map[string]string{"s0": "live"})
		doc, err := loader.ManifestStore.Get(ctx, "target")
		require.NoError(t, err)
		liveKey := doc.Manifest.Shards[0].Key
		writeShardBlob(t, root, liveKey, clock.Now().Add(-24*time.Hour))
		writeShardBlob(t, root, testOrphanKey, clock.Now().Add(-24*time.Hour))

		require.NoError(t, loader.EnqueueOrphanedShardBlobs(ctx, "target",
			[]SnapshotShardMetadata{{Key: liveKey}}, clock.Now()))
		require.Equal(t, 1, loader.shardGCPendingCount())

		// Crash: the in-memory queue is lost. The loss window is bounded by
		// the hourly reconcile, which re-derives orphans from storage.
		loader.shardGC = nil
		require.Equal(t, 0, loader.shardGCPendingCount())

		clock.Advance(shardReconcileInterval)
		require.NoError(t, loader.ReconcileShardBlobsForAllKBs(ctx, clock.Now()))
		require.Equal(t, 1, loader.shardGCPendingCount(), "reconcile must heal the lost queue")

		clock.Advance(DefaultShardGCGraceWindow + time.Second)
		res, err := loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
		_, err = loader.BlobStore.Head(ctx, testOrphanKey)
		require.Error(t, err)
		_, err = loader.BlobStore.Head(ctx, liveKey)
		require.NoError(t, err)
	})

	t.Run("concurrent_compaction", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
		loader, root := newOrphanKB(t)
		loader.Clock = clock
		branchSeed(t, loader, "target", map[string]string{"s0": "v1"})
		doc, err := loader.ManifestStore.Get(ctx, "target")
		require.NoError(t, err)
		oldKey := doc.Manifest.Shards[0].Key
		writeShardBlob(t, root, oldKey, clock.Now().Add(-24*time.Hour))

		// Compaction replaced oldKey, but a concurrent WAP publish rebased
		// and re-added it between the sweep's cached read and its confirm.
		loader.EnqueueReplacedShardsForGC("target", []SnapshotShardMetadata{{Key: oldKey}}, clock.Now())
		loader.ManifestStore = &republishOnSecondGet{
			ManifestStore: loader.ManifestStore,
			next: SnapshotShardManifest{
				SchemaVersion: 1, Layout: ShardManifestLayoutDuckDBs, KBID: "target",
				Shards: []SnapshotShardMetadata{{Key: oldKey}},
			},
		}
		clock.Advance(DefaultShardGCGraceWindow + time.Second)
		res, err := loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "republished shard must survive its own GC entry")
		require.Equal(t, 1, res.Retried)
	})
}

func TestCopy(t *testing.T) {
	t.Run("local_createonly", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		_, err := loader.BlobStore.UploadBytesIfMatch(ctx, "kb/a", []byte("bytes"), "")
		require.NoError(t, err)

		info, err := loader.BlobStore.Copy(ctx, "kb/a", "kb/b", blobstore.CopyOptions{CreateOnly: true})
		require.NoError(t, err)
		require.Equal(t, "kb/b", info.Key)
		raw, err := loader.BlobStore.DownloadBytes(ctx, "kb/b")
		require.NoError(t, err)
		require.Equal(t, []byte("bytes"), raw)

		_, err = loader.BlobStore.Copy(ctx, "kb/a", "kb/b", blobstore.CopyOptions{CreateOnly: true})
		require.ErrorIs(t, err, blobstore.ErrVersionMismatch)
	})

	t.Run("ifmatch", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		_, err := loader.BlobStore.UploadBytesIfMatch(ctx, "kb/a", []byte("v1"), "")
		require.NoError(t, err)
		head, err := loader.BlobStore.Head(ctx, "kb/a")
		require.NoError(t, err)

		_, err = loader.BlobStore.Copy(ctx, "kb/a", "kb/b", blobstore.CopyOptions{ExpectedVersion: "stale"})
		require.ErrorIs(t, err, blobstore.ErrVersionMismatch)

		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, "kb/b", []byte("old"), "")
		require.NoError(t, err)
		bHead, err := loader.BlobStore.Head(ctx, "kb/b")
		require.NoError(t, err)
		_, err = loader.BlobStore.Copy(ctx, "kb/a", "kb/b", blobstore.CopyOptions{ExpectedVersion: bHead.Version})
		require.NoError(t, err)
		raw, err := loader.BlobStore.DownloadBytes(ctx, "kb/b")
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), raw)

		// Unconditional copy overwrites without a precondition.
		_, err = loader.BlobStore.Copy(ctx, "kb/a", "kb/b", blobstore.CopyOptions{})
		require.NoError(t, err)
		_ = head
	})

	t.Run("missing_src", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		_, err := loader.BlobStore.Copy(ctx, "kb/nope", "kb/b", blobstore.CopyOptions{CreateOnly: true})
		require.Error(t, err)
		require.ErrorIs(t, err, blobstore.ErrNotFound)
	})

	t.Run("clone_round_trip", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "alpha-body", "s1": "beta-body"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		require.NoError(t, loader.CloneKBFromBackup(ctx, "src", "b1", "dst"))

		srcDoc, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		dstDoc, err := loader.ManifestStore.Get(ctx, "dst")
		require.NoError(t, err)
		require.Len(t, dstDoc.Manifest.Shards, len(srcDoc.Manifest.Shards))
		for i, srcShard := range srcDoc.Manifest.Shards {
			dstShard := dstDoc.Manifest.Shards[i]
			require.NotEqual(t, srcShard.Key, dstShard.Key)
			srcRaw, err := loader.BlobStore.DownloadBytes(ctx, srcShard.Key)
			require.NoError(t, err)
			dstRaw, err := loader.BlobStore.DownloadBytes(ctx, dstShard.Key)
			require.NoError(t, err)
			require.Equal(t, srcRaw, dstRaw)
		}
	})

	t.Run("restore_zero_copy", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		src := branchSeed(t, loader, "src", map[string]string{"s0": "alpha", "s1": "beta"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)

		before, err := loader.BlobStore.List(ctx, "src.duckdb.shards/")
		require.NoError(t, err)
		rec, err := loader.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
		require.NoError(t, err)
		require.Equal(t, "b1", rec.ParentBranchID)

		dstDoc, err := loader.ManifestStore.Get(ctx, "dst")
		require.NoError(t, err)
		require.Len(t, dstDoc.Manifest.Shards, len(src.Shards))
		for i, shard := range dstDoc.Manifest.Shards {
			require.Equal(t, src.Shards[i].Key, shard.Key, "zero-copy restore must share source keys")
		}
		after, err := loader.BlobStore.List(ctx, "src.duckdb.shards/")
		require.NoError(t, err)
		require.Len(t, after, len(before), "zero-copy restore must not copy shard bytes")
		dstObjects, err := loader.BlobStore.List(ctx, "dst")
		require.NoError(t, err)
		for _, obj := range dstObjects {
			require.NotContains(t, obj.Key, ".duckdb.shards/", "no shard bytes under the target prefix")
		}
	})
}

func isErrIn(err error, targets ...error) bool {
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// pendingKeysStore stands in for a tiered store with unreplicated journal
// bytes: UnreplicatedKeys pins them in the GC live set.
type pendingKeysStore struct {
	BlobStore
	pending []string
}

func (p *pendingKeysStore) UnreplicatedKeys(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for _, key := range p.pending {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	return out, nil
}
