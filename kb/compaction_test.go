package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompaction(t *testing.T) {
	t.Run("threshold_decision", testCompactionThresholdDecision)
	t.Run("select_candidates_size_tiered", testCompactionSelectCandidatesSizeTiered)
	t.Run("select_candidates_tombstone_pressure_fallback", testCompactionSelectCandidatesTombstonePressureFallback)
	t.Run("publish", testCompactionPublish)
	t.Run("cas_conflict", testCompactionCASConflict)
	t.Run("write_lease_conflict", testCompactionWriteLeaseConflict)
	t.Run("gc_delay_grace_window", testCompactionGCDelayGraceWindow)
	t.Run("gc_retry_on_delete_failure", testCompactionGCRetryOnDeleteFailure)
}

func testCompactionThresholdDecision(t *testing.T) {
	tests := []struct {
		name     string
		policy   ShardingPolicy
		manifest *SnapshotShardManifest
		want     bool
	}{
		{
			name: "disabled",
			policy: ShardingPolicy{
				CompactionEnabled: false,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1"}, {ShardID: "s2"}}},
			want:     false,
		},
		{
			name: "below_debt_thresholds",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  8,
				CompactionTombstoneRatio: 0.20,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1", TombstoneRatio: 0.10}, {ShardID: "s2", TombstoneRatio: 0.19}}},
			want:     false,
		},
		{
			name: "debt_from_shard_count",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  3,
				CompactionTombstoneRatio: 0.90,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1"}, {ShardID: "s2"}, {ShardID: "s3"}}},
			want:     true,
		},
		{
			name: "debt_from_tombstone_pressure",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  8,
				CompactionTombstoneRatio: 0.20,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1", TombstoneRatio: 0.25}, {ShardID: "s2", TombstoneRatio: 0.05}}},
			want:     true,
		},
		{
			name: "single_shard_never_compacts",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  1,
				CompactionTombstoneRatio: 0.01,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1", TombstoneRatio: 0.8}}},
			want:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldCompact(tc.policy, tc.manifest))
		})
	}
}

func testCompactionSelectCandidatesSizeTiered(t *testing.T) {
	policy := ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	}

	manifest := &SnapshotShardManifest{Shards: []SnapshotShardMetadata{
		{ShardID: "s-small", SizeBytes: 20, TombstoneRatio: 0.30},
		{ShardID: "s-a", SizeBytes: 90, TombstoneRatio: 0.10},
		{ShardID: "s-b", SizeBytes: 95, TombstoneRatio: 0.40},
		{ShardID: "s-c", SizeBytes: 110, TombstoneRatio: 0.20},
		{ShardID: "s-large", SizeBytes: 220, TombstoneRatio: 0.90},
	}}

	candidates := selectCompactionCandidates(policy, manifest)
	require.Len(t, candidates, 3)
	assert.Equal(t, []string{"s-b", "s-c", "s-a"}, []string{candidates[0].ShardID, candidates[1].ShardID, candidates[2].ShardID})
}

func testCompactionSelectCandidatesTombstonePressureFallback(t *testing.T) {
	policy := ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  8,
		CompactionTombstoneRatio: 0.30,
		TargetShardBytes:         100,
	}

	manifest := &SnapshotShardManifest{Shards: []SnapshotShardMetadata{
		{ShardID: "s-1", SizeBytes: 40, TombstoneRatio: 0.35},
		{ShardID: "s-2", SizeBytes: 120, TombstoneRatio: 0.32},
		{ShardID: "s-3", SizeBytes: 260, TombstoneRatio: 0.10},
	}}

	candidates := selectCompactionCandidates(policy, manifest)
	require.Len(t, candidates, 2)
	assert.Equal(t, []string{"s-1", "s-2"}, []string{candidates[0].ShardID, candidates[1].ShardID})
}

func testCompactionPublish(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-compaction-publish"
	harness := NewTestHarness(t, kbID).WithOptions(WithShardingPolicy(ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	})).Setup()
	defer harness.Cleanup()

	seedManifestWithShards(t, ctx, harness.KB(), kbID, []seedShardSpec{
		{id: "s-a", vectorRows: 30, tombstone: 0.10},
		{id: "s-b", vectorRows: 35, tombstone: 0.35},
		{id: "s-c", vectorRows: 40, tombstone: 0.22},
	})

	result, err := harness.KB().CompactShardsIfNeeded(ctx, kbID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Performed)
	require.Len(t, result.ReplacedShards, 3)
	require.Len(t, result.ReplacementShards, 1)
	assert.NotEmpty(t, result.ManifestVersionOld)
	assert.NotEmpty(t, result.ManifestVersionNew)

	reconstructed := filepath.Join(harness.CacheDir(), "compacted.duckdb")
	updatedManifest, err := harness.KB().DownloadSnapshotFromShards(ctx, kbID, reconstructed)
	require.NoError(t, err)
	require.Len(t, updatedManifest.Shards, 1)
	assert.Equal(t, result.ReplacementShards[0].ShardID, updatedManifest.Shards[0].ShardID)
	assert.Equal(t, int64(30+35+40), updatedManifest.Shards[0].VectorRows)
	db, err := testOpenConfiguredDB(harness.KB(), ctx, reconstructed)
	require.NoError(t, err)
	defer db.Close()
	var mergedRows int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&mergedRows))
	assert.Equal(t, int64(30+35+40), mergedRows)
}

func testCompactionCASConflict(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-compaction-cas"
	harness := NewTestHarness(t, kbID).WithOptions(WithShardingPolicy(ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	})).Setup()
	defer harness.Cleanup()

	baseKB := harness.KB()
	seedManifestWithShards(t, ctx, baseKB, kbID, []seedShardSpec{
		{id: "s-a", vectorRows: 10, tombstone: 0.10},
		{id: "s-b", vectorRows: 20, tombstone: 0.30},
		{id: "s-c", vectorRows: 30, tombstone: 0.25},
	})

	conflictStore := &manifestCASConflictStore{BlobStore: baseKB.BlobStore, manifestKey: shardManifestKey(kbID)}
	casKB := NewKB(conflictStore, harness.CacheDir(), WithShardingPolicy(baseKB.ShardingPolicy))

	result, err := casKB.CompactShardsIfNeeded(ctx, kbID)
	require.ErrorIs(t, err, ErrBlobVersionMismatch)
	assert.Nil(t, result)

	manifestPath := filepath.Join(harness.CacheDir(), "manifest-after-cas.json")
	manifest, err := baseKB.DownloadSnapshotFromShards(ctx, kbID, manifestPath)
	require.NoError(t, err)
	assert.Len(t, manifest.Shards, 3)

	for _, shard := range manifest.Shards {
		assert.NotContains(t, shard.Key, ".duckdb.compacted/")
	}
}

func testCompactionWriteLeaseConflict(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-compaction-lease"
	harness := NewTestHarness(t, kbID).WithOptions(WithShardingPolicy(ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	})).Setup()
	defer harness.Cleanup()

	seedManifestWithShards(t, ctx, harness.KB(), kbID, []seedShardSpec{
		{id: "s-a", vectorRows: 10, tombstone: 0.10},
		{id: "s-b", vectorRows: 20, tombstone: 0.30},
		{id: "s-c", vectorRows: 30, tombstone: 0.25},
	})

	leaseMgr := harness.KB().WriteLeaseManager
	require.NotNil(t, leaseMgr)
	held, err := leaseMgr.Acquire(ctx, kbID, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = leaseMgr.Release(context.Background(), held)
	})

	result, err := harness.KB().CompactShardsIfNeeded(ctx, kbID)
	require.ErrorIs(t, err, ErrWriteLeaseConflict)
	assert.Nil(t, result)
}

func testCompactionGCDelayGraceWindow(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-shard-gc-delay"
	harness := NewTestHarness(t, kbID).WithOptions(WithShardingPolicy(ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	})).Setup()
	defer harness.Cleanup()

	seedManifestWithShards(t, ctx, harness.KB(), kbID, []seedShardSpec{
		{id: "s-a", vectorRows: 30, tombstone: 0.10},
		{id: "s-b", vectorRows: 35, tombstone: 0.35},
		{id: "s-c", vectorRows: 40, tombstone: 0.22},
	})

	result, err := harness.KB().CompactShardsIfNeeded(ctx, kbID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Performed)
	require.NotEmpty(t, result.ReplacedShards)

	for _, shard := range result.ReplacedShards {
		_, err := os.Stat(filepath.Join(harness.BlobRoot(), shard.Key))
		require.NoError(t, err)
	}

	sweepBefore, err := harness.KB().SweepDelayedShardGC(ctx, time.Now().UTC())
	require.NoError(t, err)
	assert.Zero(t, sweepBefore.Deleted)
	assert.Equal(t, len(result.ReplacedShards), sweepBefore.Pending)

	for _, shard := range result.ReplacedShards {
		_, err := os.Stat(filepath.Join(harness.BlobRoot(), shard.Key))
		require.NoError(t, err)
	}

	sweepAfter, err := harness.KB().SweepDelayedShardGC(ctx, time.Now().UTC().Add(defaultShardGCGraceWindow+time.Second))
	require.NoError(t, err)
	assert.Equal(t, len(result.ReplacedShards), sweepAfter.Deleted)
	assert.Zero(t, sweepAfter.Pending)

	for _, shard := range result.ReplacedShards {
		_, err := os.Stat(filepath.Join(harness.BlobRoot(), shard.Key))
		require.Error(t, err)
		assert.True(t, errors.Is(err, os.ErrNotExist))
	}
}

func testCompactionGCRetryOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-shard-gc-retry"
	harness := NewTestHarness(t, kbID).WithOptions(WithShardingPolicy(ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	})).Setup()
	defer harness.Cleanup()

	baseStore := &LocalBlobStore{Root: harness.BlobRoot()}
	flakyStore := newFlakyDeleteStore(baseStore)
	kbWithFlakyDelete := NewKB(flakyStore, harness.CacheDir(), WithShardingPolicy(harness.KB().ShardingPolicy))

	seedManifestWithShards(t, ctx, kbWithFlakyDelete, kbID, []seedShardSpec{
		{id: "s-a", vectorRows: 30, tombstone: 0.10},
		{id: "s-b", vectorRows: 35, tombstone: 0.35},
		{id: "s-c", vectorRows: 40, tombstone: 0.22},
	})

	result, err := kbWithFlakyDelete.CompactShardsIfNeeded(ctx, kbID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Performed)
	require.NotEmpty(t, result.ReplacedShards)

	flakyStore.failKey = result.ReplacedShards[0].Key

	sweep1, err := kbWithFlakyDelete.SweepDelayedShardGC(ctx, time.Now().UTC().Add(defaultShardGCGraceWindow+time.Second))
	require.Error(t, err)
	assert.Equal(t, 1, sweep1.Retried)
	assert.Equal(t, len(result.ReplacedShards)-1, sweep1.Deleted)
	assert.Equal(t, 1, sweep1.Pending)

	_, err = os.Stat(filepath.Join(harness.BlobRoot(), flakyStore.failKey))
	require.NoError(t, err)

	sweep2, err := kbWithFlakyDelete.SweepDelayedShardGC(ctx, time.Now().UTC().Add(defaultShardGCGraceWindow+defaultShardGCRetryDelay+2*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 1, sweep2.Deleted)
	assert.Zero(t, sweep2.Pending)

	_, err = os.Stat(filepath.Join(harness.BlobRoot(), flakyStore.failKey))
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))

	// Idempotent: a subsequent sweep remains a no-op.
	sweep3, err := kbWithFlakyDelete.SweepDelayedShardGC(ctx, time.Now().UTC().Add(defaultShardGCGraceWindow+2*defaultShardGCRetryDelay+3*time.Second))
	require.NoError(t, err)
	assert.Zero(t, sweep3.Deleted)
	assert.Zero(t, sweep3.Pending)
}

type seedShardSpec struct {
	id         string
	vectorRows int64
	tombstone  float64
}

func seedManifestWithShards(t *testing.T, ctx context.Context, kb *KB, kbID string, shards []seedShardSpec) {
	t.Helper()

	now := time.Now().UTC()
	manifest := SnapshotShardManifest{
		SchemaVersion: 1, Layout: shardManifestLayoutDuckDBs,
		KBID:      kbID,
		CreatedAt: now,
	}
	total := int64(0)
	docSeq := 0
	for i, shard := range shards {
		shardPath := filepath.Join(t.TempDir(), shard.id+".duckdb")
		rows := int(shard.vectorRows)
		if rows <= 0 {
			rows = 1
		}
		require.NoError(t, writeTestShardDB(ctx, kb, shardPath, shard.id, docSeq, rows))
		docSeq += rows

		key := kbID + ".duckdb.shards/seed/shard-" + shard.id + ".duckdb"
		info, err := kb.BlobStore.UploadIfMatch(ctx, key, shardPath, "")
		require.NoError(t, err)
		sha, err := fileContentSHA256(shardPath)
		require.NoError(t, err)
		shardInfo, err := os.Stat(shardPath)
		require.NoError(t, err)

		meta := SnapshotShardMetadata{
			ShardID:        shard.id,
			Key:            key,
			Version:        info.Version,
			SizeBytes:      shardInfo.Size(),
			VectorRows:     int64(rows),
			CreatedAt:      now.Add(time.Duration(i) * time.Second),
			SealedAt:       now.Add(time.Duration(i) * time.Second),
			TombstoneRatio: shard.tombstone,
			GraphAvailable: false,
			SHA256:         sha,
		}
		total += meta.SizeBytes
		manifest.Shards = append(manifest.Shards, meta)
	}
	manifest.TotalSizeBytes = total

	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, data, 0o644))
	_, err = kb.BlobStore.UploadIfMatch(ctx, shardManifestKey(kbID), manifestPath, "")
	require.NoError(t, err)

}

func writeTestShardDB(ctx context.Context, kb *KB, path, shardID string, startDoc, rows int) error {
	db, err := testOpenConfiguredDB(kb, ctx, path)
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

	for i := 0; i < rows; i++ {
		docNum := startDoc + i
		vec := formatVectorForSQL([]float32{
			float32(docNum%7) * 0.1,
			float32((docNum+3)%11) * 0.1,
			float32((docNum+5)%13) * 0.1,
		})
		insertSQL := fmt.Sprintf(`INSERT INTO docs (id, content, embedding) VALUES (?, ?, %s::FLOAT[3])`, vec)
		if _, err := db.ExecContext(ctx, insertSQL, fmt.Sprintf("%s-doc-%03d", shardID, docNum), fmt.Sprintf("doc %d", docNum)); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return err
	}
	return checkpointAndCloseDB(ctx, db, "close seeded shard db")
}

type manifestCASConflictStore struct {
	BlobStore
	manifestKey string
}

func (m *manifestCASConflictStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error) {
	if key == m.manifestKey && expectedVersion != "" {
		return nil, ErrBlobVersionMismatch
	}
	return m.BlobStore.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (m *manifestCASConflictStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	return m.BlobStore.Head(ctx, key)
}

func (m *manifestCASConflictStore) Download(ctx context.Context, key string, dest string) error {
	return m.BlobStore.Download(ctx, key, dest)
}

type flakyDeleteStore struct {
	BlobStore
	failKey string
	failed  bool
}

func newFlakyDeleteStore(inner BlobStore) *flakyDeleteStore {
	return &flakyDeleteStore{BlobStore: inner}
}

func (s *flakyDeleteStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == s.failKey && !s.failed {
		s.failed = true
		return fmt.Errorf("injected delete failure for %s", key)
	}
	local, ok := s.BlobStore.(*LocalBlobStore)
	if !ok {
		return fmt.Errorf("unsupported delete store")
	}
	return os.Remove(filepath.Join(local.Root, key))
}
