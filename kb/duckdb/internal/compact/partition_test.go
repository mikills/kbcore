package compact

import (
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/compactionplan"
)

func planShardIDs(groups [][]kb.SnapshotShardMetadata) [][]string {
	out := make([][]string, len(groups))
	for i, group := range groups {
		ids := make([]string, len(group))
		for j, shard := range group {
			ids[j] = shard.ShardID
		}
		out[i] = ids
	}
	return out
}

func toPlanShards(shards []kb.SnapshotShardMetadata) []compactionplan.Shard {
	out := make([]compactionplan.Shard, len(shards))
	for i, shard := range shards {
		out[i] = compactionplan.Shard{
			ShardID:        shard.ShardID,
			SizeBytes:      shard.SizeBytes,
			TombstoneRatio: shard.TombstoneRatio,
		}
	}
	return out
}

func TestPartition(t *testing.T) {
	// Delegates to the single source of truth: for a spread of inputs and
	// caps, the shard-ID grouping must equal
	// compactionplan.PartitionForReconstruct exactly.
	t.Run("delegates to compaction plan", func(t *testing.T) {
		shards := []kb.SnapshotShardMetadata{
			{ShardID: "a", SizeBytes: 20},
			{ShardID: "b", SizeBytes: 20, TombstoneRatio: 0.5},
			{ShardID: "c", SizeBytes: 30},
			{ShardID: "d", SizeBytes: 100},
			{ShardID: "e", SizeBytes: 5, TombstoneRatio: 0.9},
		}
		for _, maxBytes := range []int64{-1, 0, 1, 10, 25, 40, 50, 200, 1 << 20} {
			got := planShardIDs(PartitionForReconstruct(shards, maxBytes))
			wantGroups := compactionplan.PartitionForReconstruct(toPlanShards(shards), maxBytes)
			want := make([][]string, len(wantGroups))
			for i, group := range wantGroups {
				ids := make([]string, len(group))
				for j, shard := range group {
					ids[j] = shard.ShardID
				}
				want[i] = ids
			}
			require.Equal(t, want, got, "maxBytes=%d", maxBytes)
		}
	})

	t.Run("empty", func(t *testing.T) {
		require.Nil(t, PartitionForReconstruct(nil, 64<<20))
		require.Nil(t, PartitionForReconstruct([]kb.SnapshotShardMetadata{}, 64<<20))
	})

	t.Run("non-positive cap is single sorted group", func(t *testing.T) {
		shards := []kb.SnapshotShardMetadata{
			{ShardID: "c", SizeBytes: 30},
			{ShardID: "a", SizeBytes: 20},
			{ShardID: "b", SizeBytes: 20, TombstoneRatio: 0.5},
		}
		for _, maxBytes := range []int64{0, -10} {
			groups := PartitionForReconstruct(shards, maxBytes)
			require.Len(t, groups, 1, "maxBytes=%d", maxBytes)
			// SortAndLimit order: tombstone ratio desc, size asc, id asc.
			require.Equal(t, []string{"b", "a", "c"}, planShardIDs(groups)[0])
		}
	})

	t.Run("greedy packing", func(t *testing.T) {
		shards := []kb.SnapshotShardMetadata{
			{ShardID: "a", SizeBytes: 20},
			{ShardID: "b", SizeBytes: 20},
			{ShardID: "c", SizeBytes: 20},
		}
		groups := PartitionForReconstruct(shards, 50)
		require.Len(t, groups, 2)
		require.Len(t, groups[0], 2)
		require.Len(t, groups[1], 1)
	})

	t.Run("oversize singleton", func(t *testing.T) {
		shards := []kb.SnapshotShardMetadata{{ShardID: "big", SizeBytes: 100}}
		groups := PartitionForReconstruct(shards, 50)
		require.Len(t, groups, 1)
		require.Len(t, groups[0], 1)
		require.Equal(t, "big", groups[0][0].ShardID)
	})

	t.Run("preserves metadata", func(t *testing.T) {
		shards := []kb.SnapshotShardMetadata{
			{ShardID: "b", SizeBytes: 10, Key: "k-b", VectorRows: 3},
			{ShardID: "a", SizeBytes: 10, Key: "k-a", VectorRows: 7},
		}
		groups := PartitionForReconstruct(shards, 1<<20)
		require.Len(t, groups, 1)
		require.Len(t, groups[0], 2)
		for _, shard := range groups[0] {
			require.NotEmpty(t, shard.Key, "delegation must carry the full metadata, not just sizing fields")
			require.NotZero(t, shard.VectorRows)
		}
	})
}
