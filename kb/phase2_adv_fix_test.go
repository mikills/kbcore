package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhase2AdvFix(t *testing.T) {
	t.Run("tombstone_rollback", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		loader.ManifestStore = &failDeleteManifest{ManifestStore: loader.ManifestStore}
		err := loader.DeleteKnowledgeBase(ctx, "src")
		require.ErrorContains(t, err, "delete manifest")
		tombstoned, terr := loader.IsTombstoned(ctx, "src")
		require.NoError(t, terr)
		require.False(t, tombstoned, "manifest-delete failure must roll back the tombstone")
	})

	t.Run("delete_spares_pins", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "alpha"})
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)
		pinnedKey := manifest.Shards[0].Key
		extraKey := "src.duckdb.shards/ffffffffffffffff/shard-00099.duckdb"
		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, extraKey, []byte("extra"), "")
		require.NoError(t, err)
		mixed := &ManifestDocument{Manifest: SnapshotShardManifest{
			KBID:   "src",
			Shards: []SnapshotShardMetadata{{Key: pinnedKey}, {Key: extraKey}},
		}}
		require.Empty(t, loader.deleteManifestShards(ctx, "src", mixed))
		_, err = loader.BlobStore.Head(ctx, pinnedKey)
		require.NoError(t, err, "branch-pinned bytes must survive the delete path")
		_, err = loader.BlobStore.Head(ctx, extraKey)
		require.Error(t, err, "unpinned bytes must still be deleted")
	})

	t.Run("delete_race_bytes", func(t *testing.T) {
		for i := 0; i < 8; i++ {
			ctx := context.Background()
			loader, _ := newOrphanKB(t)
			src := branchSeed(t, loader, "src", map[string]string{"s0": "x"})
			want := map[string]string{}
			for _, shard := range src.Shards {
				raw, err := loader.BlobStore.DownloadBytes(ctx, shard.Key)
				require.NoError(t, err)
				want[shard.Key] = string(raw)
			}
			done := make(chan struct{})
			var branchErr, deleteErr error
			go func() { defer close(done); _, branchErr = loader.BranchKB(ctx, "src", "b1", "dst") }()
			deleteErr = loader.DeleteKnowledgeBase(ctx, "src")
			<-done
			if deleteErr != nil {
				require.ErrorIs(t, deleteErr, ErrDeleteBlockedByBackups)
			}
			if branchErr != nil {
				require.True(t, isErrIn(branchErr, ErrManifestNotFound, ErrBackupExists),
					"branch loser must fail fenced, got %v", branchErr)
			}
			// Shard-byte integrity: no surviving manifest or branch marker
			// may reference missing or altered bytes.
			for _, kbID := range []string{"src", "dst"} {
				doc, err := loader.ManifestStore.Get(ctx, kbID)
				if err != nil {
					continue
				}
				for _, shard := range doc.Manifest.Shards {
					raw, err := loader.BlobStore.DownloadBytes(ctx, shard.Key)
					require.NoError(t, err, "manifest %s references missing bytes", kbID)
					if body, ok := want[shard.Key]; ok {
						require.Equal(t, body, string(raw))
					}
				}
			}
			if rec, err := loader.GetBranch(ctx, "src", "b1"); err == nil {
				for _, ref := range rec.Shards {
					raw, err := loader.BlobStore.DownloadBytes(ctx, ref.Key)
					require.NoError(t, err, "branch marker references missing bytes")
					if body, ok := want[ref.Key]; ok {
						require.Equal(t, body, string(raw))
					}
				}
			}
		}
	})

	t.Run("branch_src_missing", func(t *testing.T) {
		ctx := context.Background()
		loader, root := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(manifest.Shards[0].Key))))
		_, err := loader.BranchKB(ctx, "src", "b1", "dst")
		require.ErrorIs(t, err, ErrBackupCorrupt)
		_, err = loader.GetBranch(ctx, "src", "b1")
		require.ErrorIs(t, err, ErrBackupNotFound)
		_, err = loader.ManifestStore.Get(ctx, "dst")
		require.ErrorIs(t, err, ErrManifestNotFound)
	})

	t.Run("restore_src_missing", func(t *testing.T) {
		ctx := context.Background()
		loader, root := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(manifest.Shards[0].Key))))
		_, err = loader.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
		require.ErrorIs(t, err, ErrBackupCorrupt)
	})

	t.Run("branch_delete_fanout", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
		loader, _ := newOrphanKB(t)
		loader.Clock = clock
		keyA := "src.duckdb.shards/aaaaaaaaaaaaaaaa/shard-00000.duckdb"
		keyB := "oth.duckdb.shards/bbbbbbbbbbbbbbbb/shard-00001.duckdb"
		metas := make([]SnapshotShardMetadata, 0, 2)
		for i, item := range []struct{ id, key, body string }{
			{"s0", keyA, "alpha"}, {"s1", keyB, "beta"},
		} {
			info, err := loader.BlobStore.UploadBytesIfMatch(ctx, item.key, []byte(item.body), "")
			require.NoError(t, err)
			sum := sha256.Sum256([]byte(item.body))
			metas = append(metas, SnapshotShardMetadata{
				ShardID:   item.id,
				Key:       item.key,
				Version:   info.Version,
				SizeBytes: int64(len(item.body)),
				SHA256:    hex.EncodeToString(sum[:]),
			})
			_ = i
		}
		_, err := loader.ManifestStore.UpsertIfMatch(ctx, "src", SnapshotShardManifest{
			SchemaVersion: 1,
			Layout:        ShardManifestLayoutDuckDBs,
			FormatKind:    "duckdb_sharded",
			FormatVersion: 2,
			KBID:          "src",
			CreatedAt:     clock.Now(),
			Shards:        metas,
		}, "")
		require.NoError(t, err)
		_, err = loader.BranchKB(ctx, "src", "b1", "dst")
		require.NoError(t, err)
		_, err = loader.GetBranch(ctx, "src", "b1")
		require.NoError(t, err)
		_, err = loader.GetBranch(ctx, "oth", "b1")
		require.NoError(t, err, "cross-prefix branch must fan markers out to every owner")

		require.NoError(t, loader.DeleteBranch(ctx, "src", "b1"))
		_, err = loader.GetBranch(ctx, "src", "b1")
		require.ErrorIs(t, err, ErrBackupNotFound)
		_, err = loader.GetBranch(ctx, "oth", "b1")
		require.ErrorIs(t, err, ErrBackupNotFound, "one DeleteBranch must release every owner marker")
		ids, err := loader.ListBranchIDs(ctx, "oth")
		require.NoError(t, err)
		require.Empty(t, ids)

		// With all markers gone the shared bytes are collectible again.
		doc, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		dropped := doc.Manifest
		dropped.Shards = nil
		_, err = loader.ManifestStore.UpsertIfMatch(ctx, "src", dropped, doc.Version)
		require.NoError(t, err)
		loader.EnqueueReplacedShardsForGC("src",
			[]SnapshotShardMetadata{{Key: keyA}}, clock.Now().Add(-time.Hour))
		res, err := loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
	})

	t.Run("tombstone_drop", func(t *testing.T) {
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
		loader, _ := newOrphanKB(t)
		loader.Clock = clock
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "x"})
		key := manifest.Shards[0].Key
		require.NoError(t, loader.TombstoneKnowledgeBase(ctx, "src", "test"))
		// Compaction dropped the shard while the tombstone was present.
		doc, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		dropped := doc.Manifest
		dropped.Shards = nil
		_, err = loader.ManifestStore.UpsertIfMatch(ctx, "src", dropped, doc.Version)
		require.NoError(t, err)
		loader.EnqueueReplacedShardsForGC("src",
			[]SnapshotShardMetadata{{Key: key}}, clock.Now().Add(-time.Hour))
		res, err := loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted)
		require.Equal(t, 0, res.Retried, "tombstoned entries must not retry forever")
		require.Equal(t, 0, res.Pending, "tombstoned entries must be dropped")
		res, err = loader.SweepDelayedShardGC(ctx, clock.Now())
		require.NoError(t, err)
		require.Equal(t, 0, res.Pending)
		_, err = loader.BlobStore.Head(ctx, key)
		require.NoError(t, err, "tombstoned bytes must be left alone")
	})
}

type failDeleteManifest struct {
	ManifestStore
}

func (m *failDeleteManifest) Delete(_ context.Context, kbID string) error {
	return errors.New("manifest delete unavailable")
}
