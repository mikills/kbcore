package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func advSeedKB(t *testing.T, loader *KB, kbID string, bodies map[string]string) SnapshotShardManifest {
	t.Helper()
	ctx := context.Background()
	metas := make([]SnapshotShardMetadata, 0, len(bodies))
	i := 0
	for shardID, body := range bodies {
		key := fmt.Sprintf("%s.duckdb.shards/abc123def4567890/shard-%05d.duckdb", kbID, i)
		info, err := loader.BlobStore.UploadBytesIfMatch(ctx, key, []byte(body), "")
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
	_, err := loader.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, "")
	require.NoError(t, err)
	return manifest
}

func TestBackupAdv(t *testing.T) {
	t.Run("exists_wraps_mismatch", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		advSeedKB(t, loader, "kb", map[string]string{"s": "x"})
		_, err := loader.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		_, err = loader.CreateBackup(ctx, "kb", "b1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
		require.ErrorIs(t, err, ErrBlobVersionMismatch)

		_, err = loader.CreateSnapshot(ctx, "kb", "s1")
		require.NoError(t, err)
		_, err = loader.CreateSnapshot(ctx, "kb", "s1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
		require.ErrorIs(t, err, ErrBlobVersionMismatch)

		require.NoError(t, loader.CloneKBFromBackup(ctx, "kb", "b1", "dst"))
		err = loader.CloneKBFromBackup(ctx, "kb", "b1", "dst")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
		require.ErrorIs(t, err, ErrBlobVersionMismatch)
	})

	t.Run("concurrent_create", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		advSeedKB(t, loader, "kb", map[string]string{"s": "x"})
		const racers = 8
		errs := make([]error, racers)
		var wg sync.WaitGroup
		for i := range racers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = loader.CreateBackup(ctx, "kb", "b1")
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
		require.Equal(t, 1, wins, "exactly one concurrent create must win")
		got, err := loader.GetBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		require.NoError(t, ValidateBackupDescriptor(got))
	})

	t.Run("kbid_reject", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		advSeedKB(t, loader, "kb", map[string]string{"s": "x"})
		for _, bad := range []string{"a/b", "..", "/abs", "x.backups/y", "x.snapshots/y", "k.duckdb.shards/z", ".", "a\\b"} {
			_, err := loader.CreateBackup(ctx, bad, "b1")
			require.Error(t, err, "kb %q", bad)
			_, err = loader.CreateSnapshotFrom(ctx, bad, "s1", "")
			require.Error(t, err, "kb %q", bad)
			require.Error(t, loader.CloneKBFromBackup(ctx, bad, "b1", "dst"), "src %q", bad)
			require.Error(t, loader.CloneKBFromBackup(ctx, "kb", "b1", bad), "dst %q", bad)
		}
	})

	t.Run("shardkey_reject", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		for _, bad := range []string{"../evil", "/abs", "a/../b", "a\\b"} {
			_, err := loader.ManifestStore.UpsertIfMatch(ctx, "kb", SnapshotShardManifest{
				SchemaVersion: 1, FormatKind: "duckdb_sharded", FormatVersion: 2,
				KBID: "kb", CreatedAt: time.Now().UTC(),
				Shards: []SnapshotShardMetadata{{ShardID: "s", Key: bad, Version: "v", SizeBytes: 1, SHA256: "ab"}},
			}, "")
			require.NoError(t, err)
			_, err = loader.CreateBackup(ctx, "kb", "b1")
			require.Error(t, err)
			require.ErrorIs(t, err, ErrBackupCorrupt, "key %q", bad)
		}
	})

	t.Run("legacy_unify", func(t *testing.T) {
		desc := &BackupDescriptor{
			DescriptorVersion: 1, BackupID: "b1", SourceKBID: "kb",
			SourceManifestVersion: "v", CreatedAt: time.Now().UTC(),
			FormatKind: "duckdb_sharded", FormatVersion: 1, MinReader: "x",
			Shards: []BackupShardRef{{Key: "k", SizeBytes: 1, SHA256: "s", Version: "v"}},
		}
		err := ValidateBackupDescriptor(desc)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupLegacyFormat)
		require.NotErrorIs(t, err, ErrBackupCorrupt)
	})

	t.Run("remap_delim", func(t *testing.T) {
		require.Equal(t, "dst.duckdb.shards/x", remapShardKey("kb", "dst", "kb.duckdb.shards/x"))
		got := remapShardKey("kb", "dst", "kb2.duckdb.shards/x")
		require.NotEqual(t, "dst2.duckdb.shards/x", got)
		require.Contains(t, got, "dst.duckdb.shards/restored/")
	})

	t.Run("rollback_cleans_stage", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		advSeedKB(t, loader, "src", map[string]string{"s0": "alpha-body", "s1": "beta-body"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		doc, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, doc.Manifest.Shards[0].Key, []byte("tampered"), "")
		require.NoError(t, err)

		err = loader.CloneKBFromBackup(ctx, "src", "b1", "dst")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupCorrupt)
		head, err := loader.ManifestStore.HeadVersion(ctx, "dst")
		require.NoError(t, err)
		require.Empty(t, head)
		leftovers, err := loader.BlobStore.List(ctx, "dst")
		require.NoError(t, err)
		require.Empty(t, leftovers, "failed clone must not leave staged shards")
	})

	t.Run("clone_occupancy", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		advSeedKB(t, loader, "src", map[string]string{"s": "x"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		_, err = loader.BlobStore.UploadBytesIfMatch(ctx, "dst.duckdb.shards/leftover", []byte("junk"), "")
		require.NoError(t, err)
		err = loader.CloneKBFromBackup(ctx, "src", "b1", "dst")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
		head, err := loader.ManifestStore.HeadVersion(ctx, "dst")
		require.NoError(t, err)
		require.Empty(t, head)
	})

	t.Run("concurrent_clone", func(t *testing.T) {
		ctx := context.Background()
		loader, _ := newOrphanKB(t)
		advSeedKB(t, loader, "src", map[string]string{"s0": "alpha", "s1": "beta"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		const racers = 4
		errs := make([]error, racers)
		var wg sync.WaitGroup
		for i := range racers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = loader.CloneKBFromBackup(ctx, "src", "b1", "dst")
			}(i)
		}
		wg.Wait()
		wins := 0
		for _, err := range errs {
			if err == nil {
				wins++
				continue
			}
			require.ErrorIs(t, err, ErrBackupExists, "loser must fail fenced, got %v", err)
		}
		require.Equal(t, 1, wins, "exactly one concurrent clone must win")
		doc, err := loader.ManifestStore.Get(ctx, "dst")
		require.NoError(t, err)
		require.Len(t, doc.Manifest.Shards, 2)
	})

	t.Run("guard_nil_store", func(t *testing.T) {
		_, err := (&KB{}).HasBackupsOrBranches(context.Background(), "kb")
		require.Error(t, err, "nil store must fail closed, not report no-backups")
	})

	t.Run("guard_bad_kbid", func(t *testing.T) {
		loader, _ := newOrphanKB(t)
		_, err := loader.HasBackupsOrBranches(context.Background(), "a/b")
		require.Error(t, err)
	})
}

func TestBackupGCPins(t *testing.T) {
	t.Run("pinned_until_markers_gone", func(t *testing.T) {
		ctx := context.Background()
		loader, root := newOrphanKB(t)
		manifest := advSeedKB(t, loader, "kb", map[string]string{"s0": "alpha", "s1": "beta"})
		_, err := loader.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)

		dropped := manifest.Shards[0]
		live := manifest
		live.Shards = manifest.Shards[1:]
		doc, err := loader.ManifestStore.Get(ctx, "kb")
		require.NoError(t, err)
		_, err = loader.ManifestStore.UpsertIfMatch(ctx, "kb", live, doc.Version)
		require.NoError(t, err)

		past := time.Now().UTC().Add(-time.Hour)
		loader.EnqueueReplacedShardsForGC("kb", []SnapshotShardMetadata{dropped}, past)
		now := time.Now().UTC()
		res, err := loader.SweepDelayedShardGC(ctx, now)
		require.NoError(t, err)
		require.Equal(t, 0, res.Deleted, "backup-pinned shard must survive GC")
		_, statErr := os.Stat(filepath.Join(root, filepath.FromSlash(dropped.Key)))
		require.NoError(t, statErr, "pinned shard bytes must remain")

		require.NoError(t, loader.DeleteBackup(ctx, "kb", "b1"))
		res, err = loader.SweepDelayedShardGC(ctx, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, 1, res.Deleted)
		_, statErr = os.Stat(filepath.Join(root, filepath.FromSlash(dropped.Key)))
		require.True(t, os.IsNotExist(statErr))
	})
}
