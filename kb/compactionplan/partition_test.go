package compactionplan

import (
	"fmt"
	"testing"

	"github.com/mikills/minnow/kb/sharding"
	"github.com/stretchr/testify/require"
)

func groupLiveBytes(groups [][]Shard) []int64 {
	out := make([]int64, len(groups))
	for i, g := range groups {
		var total int64
		for _, s := range g {
			total += LiveBytes(s)
		}
		out[i] = total
	}
	return out
}

func TestPartition(t *testing.T) {
	t.Run("bounds", func(t *testing.T) {
		// 500 sealed 32MiB shards must partition so no group's live bytes exceed
		// the 64MiB cap, mirroring TestCompactionHasACeiling.
		policy := sharding.DefaultPolicy()
		const sealed = int64(32) << 20
		var shards []Shard
		for i := range 500 {
			shards = append(shards, Shard{ShardID: fmt.Sprintf("s%04d", i), SizeBytes: sealed})
		}
		groups := PartitionForReconstruct(shards, policy.MaxShardBytes)
		require.NotEmpty(t, groups)
		total := 0
		var largest int64
		for _, g := range groups {
			var live int64
			for _, s := range g {
				live += LiveBytes(s)
			}
			require.LessOrEqual(t, live, policy.MaxShardBytes, "group outgrew the cap")
			largest = max(largest, live)
			total += len(g)
		}
		require.Equal(t, len(shards), total, "every shard must appear exactly once")
		require.Greater(t, len(groups), 1, "500x32MiB must split into multiple groups")
		// Tight packing: 32MiB shards under a 64MiB cap pack two per group.
		require.Equal(t, 250, len(groups), "expected tight 2-per-group packing")
		require.Equal(t, policy.MaxShardBytes, largest)
	})

	t.Run("tight packing", func(t *testing.T) {
		shards := []Shard{
			{ShardID: "a", SizeBytes: 20},
			{ShardID: "b", SizeBytes: 20},
			{ShardID: "c", SizeBytes: 20},
		}
		groups := PartitionForReconstruct(shards, 50)
		require.Len(t, groups, 2)
		require.Len(t, groups[0], 2)
		require.Len(t, groups[1], 1)
		for _, live := range groupLiveBytes(groups) {
			require.LessOrEqual(t, live, int64(50))
		}
	})

	t.Run("tombstone live sizing", func(t *testing.T) {
		// Two shards at the cap that are mostly dead rows fit easily once the
		// tombstones are dropped, so live-byte sizing must keep them together.
		policy := sharding.DefaultPolicy()
		full := policy.MaxShardBytes
		shards := []Shard{
			{ShardID: "a", SizeBytes: full, TombstoneRatio: 0.9},
			{ShardID: "b", SizeBytes: full, TombstoneRatio: 0.9},
		}
		groups := PartitionForReconstruct(shards, policy.MaxShardBytes)
		require.Len(t, groups, 1, "live-byte sizing should pack mostly-dead shards together")
		require.Len(t, groups[0], 2)
	})

	t.Run("single identity", func(t *testing.T) {
		shards := []Shard{{ShardID: "only", SizeBytes: 10}}
		groups := PartitionForReconstruct(shards, 64<<20)
		require.Len(t, groups, 1)
		require.Len(t, groups[0], 1)
		require.Equal(t, "only", groups[0][0].ShardID)
	})

	t.Run("empty", func(t *testing.T) {
		require.Empty(t, PartitionForReconstruct(nil, 64<<20))
		require.Empty(t, PartitionForReconstruct([]Shard{}, 64<<20))
	})

	t.Run("oversize singleton", func(t *testing.T) {
		// A shard that alone exceeds the cap cannot be split here; it must still
		// form its own group so the rebuild makes progress.
		shards := []Shard{{ShardID: "big", SizeBytes: 100}}
		groups := PartitionForReconstruct(shards, 50)
		require.Len(t, groups, 1)
		require.Len(t, groups[0], 1)
	})
}
