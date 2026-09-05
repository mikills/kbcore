package kb_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func backupTestKB(t *testing.T) *KB {
	t.Helper()
	return NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir())
}

func seedShardedKB(t *testing.T, k *KB, kbID string, bodies map[string]string) SnapshotShardManifest {
	t.Helper()
	ctx := context.Background()
	metas := make([]SnapshotShardMetadata, 0, len(bodies))
	i := 0
	for shardID, body := range bodies {
		key := fmt.Sprintf("%s.duckdb.shards/abc123def4567890/shard-%05d.duckdb", kbID, i)
		info, err := k.BlobStore.UploadBytesIfMatch(ctx, key, []byte(body), "")
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
	_, err := k.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, "")
	require.NoError(t, err)
	return manifest
}

func TestBackup(t *testing.T) {
	t.Run("round_trip", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "alpha-body", "shard-00001": "beta-body"})

		desc, err := k.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		require.Equal(t, 1, desc.DescriptorVersion)
		require.Equal(t, "kb", desc.SourceKBID)
		require.NotEmpty(t, desc.SourceManifestVersion)
		require.NotEmpty(t, desc.DescriptorSHA256)
		require.Len(t, desc.Shards, 2)

		got, err := k.GetBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		require.NoError(t, ValidateBackupDescriptor(got))
		require.Equal(t, desc.DescriptorSHA256, got.DescriptorSHA256)

		ids, err := k.ListBackupIDs(ctx, "kb")
		require.NoError(t, err)
		require.Equal(t, []string{"b1"}, ids)
	})

	t.Run("create_only_dup", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		_, err = k.CreateBackup(ctx, "kb", "b1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
	})

	t.Run("rejects_legacy_v1", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		_, err := k.ManifestStore.UpsertIfMatch(ctx, "kb", SnapshotShardManifest{
			SchemaVersion: 1, FormatKind: "duckdb_sharded", FormatVersion: 1,
			KBID: "kb", CreatedAt: time.Now().UTC(),
			Shards: []SnapshotShardMetadata{{ShardID: "s", Key: "k", Version: "v", SizeBytes: 1, SHA256: "ab"}},
		}, "")
		require.NoError(t, err)
		_, err = k.CreateBackup(ctx, "kb", "b1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupLegacyFormat)
	})

	t.Run("rejects_missing_sha", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		doc, err := k.ManifestStore.Get(ctx, "kb")
		require.NoError(t, err)
		doc.Manifest.Shards[0].SHA256 = ""
		_, err = k.ManifestStore.UpsertIfMatch(ctx, "kb", doc.Manifest, doc.Version)
		require.NoError(t, err)
		_, err = k.CreateBackup(ctx, "kb", "b1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupCorrupt)
		require.Contains(t, err.Error(), "sha256")
	})

	t.Run("tamper_detected", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)

		raw, err := k.BlobStore.DownloadBytes(ctx, BackupDescriptorKey("kb", "b1"))
		require.NoError(t, err)
		var doc map[string]any
		require.NoError(t, json.Unmarshal(raw, &doc))
		shards := doc["shards"].([]any)
		shards[0].(map[string]any)["size_bytes"] = float64(99999)
		mutated, err := json.Marshal(doc)
		require.NoError(t, err)
		_, err = k.BlobStore.UploadBytesIfMatch(ctx, BackupDescriptorKey("kb", "b1"), mutated, "")
		require.NoError(t, err)

		_, err = k.GetBackup(ctx, "kb", "b1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupCorrupt)
	})

	t.Run("unknown_descriptor_version", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		raw, err := json.Marshal(map[string]any{"descriptor_version": 99, "backup_id": "bad"})
		require.NoError(t, err)
		_, err = k.BlobStore.UploadBytesIfMatch(ctx, BackupDescriptorKey("kb", "bad"), raw, "")
		require.NoError(t, err)
		_, err = k.GetBackup(ctx, "kb", "bad")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupCorrupt)
		require.Contains(t, err.Error(), "descriptor_version")
	})

	t.Run("bad_ids", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateBackup(ctx, "kb", "")
		require.Error(t, err)
		_, err = k.CreateBackup(ctx, "kb", "../evil")
		require.Error(t, err)
		_, err = k.CreateBackup(ctx, "", "b1")
		require.Error(t, err)
	})

	t.Run("missing_kb", func(t *testing.T) {
		_, err := backupTestKB(t).CreateBackup(context.Background(), "nope", "b1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrManifestNotFound)
	})
}

func TestSnapshot(t *testing.T) {
	t.Run("zero_copy", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		src := seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "alpha", "shard-00001": "beta"})

		before, err := k.BlobStore.List(ctx, "kb.duckdb.shards/")
		require.NoError(t, err)

		rec, err := k.CreateSnapshot(ctx, "kb", "s1")
		require.NoError(t, err)
		require.Equal(t, "kb", rec.SourceKBID)
		require.NotEmpty(t, rec.SourceManifestVersion)
		require.NotEmpty(t, rec.RecordSHA256)
		require.Len(t, rec.Manifest.Shards, len(src.Shards))
		for i, shard := range rec.Manifest.Shards {
			require.Equal(t, src.Shards[i].Key, shard.Key, "snapshot must reference the same shard keys")
		}

		after, err := k.BlobStore.List(ctx, "kb.duckdb.shards/")
		require.NoError(t, err)
		require.Len(t, after, len(before), "snapshot must not copy shard bytes")

		got, err := k.GetSnapshot(ctx, "kb", "s1")
		require.NoError(t, err)
		require.Equal(t, rec.RecordSHA256, got.RecordSHA256)

		ids, err := k.ListSnapshotIDs(ctx, "kb")
		require.NoError(t, err)
		require.Equal(t, []string{"s1"}, ids)
	})

	t.Run("dup", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateSnapshot(ctx, "kb", "s1")
		require.NoError(t, err)
		_, err = k.CreateSnapshot(ctx, "kb", "s1")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
	})

	t.Run("parent_link", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		rec, err := k.CreateSnapshotFrom(ctx, "kb", "s1", "b1")
		require.NoError(t, err)
		require.Equal(t, "b1", rec.ParentBackupID)
	})
}

func TestRestore(t *testing.T) {
	t.Run("round_trip", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "src", map[string]string{"shard-00000": "alpha-body", "shard-00001": "beta-body"})
		_, err := k.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)

		require.NoError(t, k.CloneKBFromBackup(ctx, "src", "b1", "dst"))

		srcDoc, err := k.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		dstDoc, err := k.ManifestStore.Get(ctx, "dst")
		require.NoError(t, err)
		require.Equal(t, "dst", dstDoc.Manifest.KBID)
		require.Equal(t, len(srcDoc.Manifest.Shards), len(dstDoc.Manifest.Shards))
		for i, srcShard := range srcDoc.Manifest.Shards {
			dstShard := dstDoc.Manifest.Shards[i]
			require.True(t, strings.HasPrefix(dstShard.Key, "dst"), "clone must live under the target prefix")
			require.NotEqual(t, srcShard.Key, dstShard.Key)
			require.Equal(t, srcShard.SHA256, dstShard.SHA256)
			require.Equal(t, srcShard.SizeBytes, dstShard.SizeBytes)
			srcRaw, err := k.BlobStore.DownloadBytes(ctx, srcShard.Key)
			require.NoError(t, err)
			dstRaw, err := k.BlobStore.DownloadBytes(ctx, dstShard.Key)
			require.NoError(t, err)
			require.Equal(t, srcRaw, dstRaw, "clone must byte-copy shard content")
		}
	})

	t.Run("refuses_existing_target", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "src", map[string]string{"shard-00000": "x"})
		_, err := k.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		require.NoError(t, k.CloneKBFromBackup(ctx, "src", "b1", "dst"))
		err = k.CloneKBFromBackup(ctx, "src", "b1", "dst")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupExists)
	})

	t.Run("rollback_on_corrupt", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "src", map[string]string{"shard-00000": "alpha-body", "shard-00001": "beta-body"})
		_, err := k.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)

		srcDoc, err := k.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		_, err = k.BlobStore.UploadBytesIfMatch(ctx, srcDoc.Manifest.Shards[0].Key, []byte("tampered-content"), "")
		require.NoError(t, err)

		err = k.CloneKBFromBackup(ctx, "src", "b1", "dst")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBackupCorrupt)

		head, headErr := k.ManifestStore.HeadVersion(ctx, "dst")
		require.NoError(t, headErr)
		require.Empty(t, head, "failed restore must roll back the target manifest")
	})

	t.Run("never_mutates_source", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "src", map[string]string{"shard-00000": "x"})
		before, err := k.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		_, err = k.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		require.NoError(t, k.CloneKBFromBackup(ctx, "src", "b1", "dst"))

		after, err := k.ManifestStore.Get(ctx, "src")
		require.NoError(t, err)
		require.Equal(t, before.Version, after.Version)
		require.Equal(t, before.Manifest.Shards, after.Manifest.Shards)
	})

	t.Run("delete_guard", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateBackup(ctx, "kb", "b1")
		require.NoError(t, err)

		err = k.DeleteKnowledgeBase(ctx, "kb")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDeleteBlockedByBackups)

		require.NoError(t, k.DeleteBackup(ctx, "kb", "b1"))
		require.NoError(t, k.DeleteKnowledgeBase(ctx, "kb"))
	})

	t.Run("delete_guard_snapshot", func(t *testing.T) {
		ctx := context.Background()
		k := backupTestKB(t)
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		_, err := k.CreateSnapshot(ctx, "kb", "s1")
		require.NoError(t, err)

		err = k.DeleteKnowledgeBase(ctx, "kb")
		require.ErrorIs(t, err, ErrDeleteBlockedByBackups)

		require.NoError(t, k.DeleteSnapshot(ctx, "kb", "s1"))
		require.NoError(t, k.DeleteKnowledgeBase(ctx, "kb"))
	})
}

func TestRetention(t *testing.T) {
	seedRetention := func(t *testing.T) ([]BackupDescriptor, *KB) {
		t.Helper()
		ctx := context.Background()
		clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		k := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithClock(clock))
		seedShardedKB(t, k, "kb", map[string]string{"shard-00000": "x"})
		var descs []BackupDescriptor
		for _, id := range []string{"b1", "b2", "b3"} {
			clock.Advance(time.Hour)
			_, err := k.CreateBackup(ctx, "kb", id)
			require.NoError(t, err)
			got, err := k.GetBackup(ctx, "kb", id)
			require.NoError(t, err)
			descs = append(descs, *got)
		}
		return descs, k
	}

	t.Run("never_drops_newest", func(t *testing.T) {
		descs, _ := seedRetention(t)
		drop := SelectForRetention(descs, 0)
		require.Len(t, drop, 2, "keep<=0 must still keep the newest valid descriptor")
		for _, d := range drop {
			require.NotEqual(t, "b3", d.BackupID)
		}
	})

	t.Run("keeps_n", func(t *testing.T) {
		descs, _ := seedRetention(t)
		drop := SelectForRetention(descs, 2)
		require.Len(t, drop, 1)
		require.Equal(t, "b1", drop[0].BackupID)
	})

	t.Run("drops_invalid", func(t *testing.T) {
		descs, _ := seedRetention(t)
		bad := descs[2]
		bad.DescriptorSHA256 = "00"
		require.Error(t, ValidateBackupDescriptor(&bad))
		drop := SelectForRetention([]BackupDescriptor{descs[0], bad}, 1)
		require.Len(t, drop, 1)
		require.Equal(t, bad.BackupID, drop[0].BackupID, "invalid descriptors are always selected, newest-valid is kept")
	})

	t.Run("empty", func(t *testing.T) {
		require.Empty(t, SelectForRetention(nil, 2))
	})

	t.Run("rejects_dup_keys", func(t *testing.T) {
		descs, _ := seedRetention(t)
		dup := descs[0]
		dup.BackupID = "dup"
		dup.Shards = append(append([]BackupShardRef(nil), dup.Shards...), dup.Shards[0])
		require.ErrorIs(t, ValidateBackupDescriptor(&dup), ErrBackupCorrupt)
	})

	t.Run("rejects_zero_size", func(t *testing.T) {
		descs, _ := seedRetention(t)
		zero := descs[0]
		zero.BackupID = "zero"
		zero.Shards[0].SizeBytes = 0
		require.ErrorIs(t, ValidateBackupDescriptor(&zero), ErrBackupCorrupt)
	})

	t.Run("rejects_missing_sha", func(t *testing.T) {
		descs, _ := seedRetention(t)
		missing := descs[0]
		missing.BackupID = "nosha"
		missing.Shards[0].SHA256 = ""
		require.ErrorIs(t, ValidateBackupDescriptor(&missing), ErrBackupCorrupt)
	})

	t.Run("rejects_unknown_version", func(t *testing.T) {
		descs, _ := seedRetention(t)
		unknown := descs[0]
		unknown.BackupID = "future"
		unknown.DescriptorVersion = 99
		err := ValidateBackupDescriptor(&unknown)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrBackupCorrupt))
		require.Contains(t, err.Error(), "descriptor_version")
	})
}
