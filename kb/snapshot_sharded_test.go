package kb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSharded(t *testing.T) {
	t.Run("publish_queryable_shards", testSnapshotShardedPublishQueryableShards)
	t.Run("manifest_cas", testSnapshotShardedManifestCAS)
	t.Run("write_lease_conflict", testSnapshotShardedWriteLeaseConflict)
}

func testSnapshotShardedPublishQueryableShards(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		docs          int
		targetBytes   int64
		wantMinShards int
	}{
		{name: "single_shard", docs: 4, targetBytes: 1 << 20, wantMinShards: 1},
		{name: "multiple_shards", docs: 24, targetBytes: 1024, wantMinShards: 2},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			kbID := "kb-sharded-publish-" + tc.name
			harness := NewTestHarness(t, kbID).Setup()
			defer harness.Cleanup()

			store := &LocalBlobStore{Root: harness.BlobRoot()}
			loader := NewKB(store, harness.CacheDir())

			sourcePath := filepath.Join(harness.CacheDir(), "source.duckdb")
			require.NoError(t, writeSnapshotSourceDB(ctx, loader, sourcePath, tc.docs))

			info, err := loader.UploadSnapshotShardedIfMatch(ctx, kbID, sourcePath, "", tc.targetBytes)
			require.NoError(t, err)
			require.NotEmpty(t, info.Version)

			reconstructed := filepath.Join(harness.CacheDir(), "reconstructed.duckdb")
			manifest, err := loader.DownloadSnapshotFromShards(ctx, kbID, reconstructed)
			require.NoError(t, err)

			require.Equal(t, kbID, manifest.KBID)
			require.Equal(t, shardManifestLayoutDuckDBs, manifest.Layout)
			require.GreaterOrEqual(t, len(manifest.Shards), tc.wantMinShards)
			assert.Greater(t, manifest.TotalSizeBytes, int64(0))

			reconstructedDB, err := loader.openConfiguredDB(ctx, reconstructed)
			require.NoError(t, err)
			var gotRows int
			require.NoError(t, reconstructedDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&gotRows))
			require.Equal(t, tc.docs, gotRows)
			require.NoError(t, reconstructedDB.Close())

			for i, shard := range manifest.Shards {
				require.NotEmpty(t, shard.ShardID)
				require.NotEmpty(t, shard.Key)
				require.NotEmpty(t, shard.Version)
				require.NotEmpty(t, shard.SHA256)
				require.Greater(t, shard.SizeBytes, int64(0))
				require.Greater(t, shard.VectorRows, int64(0))

				localShard := filepath.Join(harness.CacheDir(), fmt.Sprintf("check-shard-%02d.duckdb", i))
				require.NoError(t, loader.BlobStore.Download(ctx, shard.Key, localShard))
				shardDB, err := loader.openConfiguredDB(ctx, localShard)
				require.NoError(t, err)

				var shardRows int
				require.NoError(t, shardDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&shardRows))
				assert.Greater(t, shardRows, 0)
				require.NoError(t, shardDB.Close())
			}
		})
	}
}

func testSnapshotShardedManifestCAS(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-sharded-cas"

	harness := NewTestHarness(t, kbID).Setup()
	defer harness.Cleanup()

	store := &LocalBlobStore{Root: harness.BlobRoot()}
	loader := NewKB(store, harness.CacheDir())

	src1 := filepath.Join(harness.CacheDir(), "v1.duckdb")
	require.NoError(t, writeSnapshotSourceDB(ctx, loader, src1, 3))
	info1, err := loader.UploadSnapshotShardedIfMatch(ctx, kbID, src1, "", 1024)
	require.NoError(t, err)

	src2 := filepath.Join(harness.CacheDir(), "v2.duckdb")
	require.NoError(t, writeSnapshotSourceDB(ctx, loader, src2, 6))

	_, err = loader.UploadSnapshotShardedIfMatch(ctx, kbID, src2, "stale-version", 1024)
	require.ErrorIs(t, err, ErrBlobVersionMismatch)

	_, err = loader.UploadSnapshotShardedIfMatch(ctx, kbID, src2, info1.Version, 1024)
	require.NoError(t, err)
}

func testSnapshotShardedWriteLeaseConflict(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-sharded-lease"

	harness := NewTestHarness(t, kbID).Setup()
	defer harness.Cleanup()

	loader := NewKB(&LocalBlobStore{Root: harness.BlobRoot()}, harness.CacheDir())
	src := filepath.Join(harness.CacheDir(), "lease-src.duckdb")
	require.NoError(t, writeSnapshotSourceDB(ctx, loader, src, 4))

	leaseMgr := loader.WriteLeaseManager
	require.NotNil(t, leaseMgr)
	held, err := leaseMgr.Acquire(ctx, kbID, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = leaseMgr.Release(context.Background(), held)
	})

	_, err = loader.UploadSnapshotShardedIfMatch(ctx, kbID, src, "", 1024)
	require.ErrorIs(t, err, ErrWriteLeaseConflict)
}

func writeSnapshotSourceDB(ctx context.Context, loader *KB, path string, docs int) error {
	db, err := loader.openConfiguredDB(ctx, path)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[3]
		)
	`); err != nil {
		return err
	}

	for i := 0; i < docs; i++ {
		vec := formatVectorForSQL([]float32{float32(i) * 0.1, float32(i+1) * 0.1, float32(i+2) * 0.1})
		query := fmt.Sprintf(`INSERT INTO docs (id, content, embedding) VALUES (?, ?, %s::FLOAT[3])`, vec)
		if _, err := db.ExecContext(ctx, query, fmt.Sprintf("doc-%03d", i), fmt.Sprintf("content-%03d", i)); err != nil {
			return err
		}
	}

	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return err
	}
	if err := checkpointAndCloseDB(ctx, db, "close source snapshot db"); err != nil {
		return err
	}
	return nil
}
