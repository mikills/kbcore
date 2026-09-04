package compactionplan

import (
	"fmt"
	"testing"

	"github.com/mikills/minnow/kb/sharding"
	"github.com/stretchr/testify/require"
)

func TestPlan(t *testing.T) {
	t.Run("selects largest compactable shards", func(t *testing.T) {
		policy := sharding.Policy{
			CompactionEnabled:        true,
			CompactionMinShardCount:  3,
			CompactionTombstoneRatio: 0.20,
			TargetShardBytes:         100,
		}
		shards := []Shard{
			{ShardID: "s-small", SizeBytes: 20, TombstoneRatio: 0.30},
			{ShardID: "s-a", SizeBytes: 90, TombstoneRatio: 0.10},
			{ShardID: "s-b", SizeBytes: 95, TombstoneRatio: 0.40},
			{ShardID: "s-c", SizeBytes: 110, TombstoneRatio: 0.20},
		}

		got := Select(policy, shards)

		require.Len(t, got, 3)
		require.Equal(t, []string{"s-b", "s-c", "s-a"}, []string{got[0].ShardID, got[1].ShardID, got[2].ShardID})
	})

	t.Run("tombstone pressure bypasses shard minimum", func(t *testing.T) {
		policy := sharding.Policy{
			CompactionEnabled:        true,
			CompactionMinShardCount:  8,
			CompactionTombstoneRatio: 0.30,
			TargetShardBytes:         100,
		}

		got := Select(policy, []Shard{
			{ShardID: "s-1", SizeBytes: 40, TombstoneRatio: 0.35},
			{ShardID: "s-2", SizeBytes: 120, TombstoneRatio: 0.32},
			{ShardID: "s-3", SizeBytes: 260, TombstoneRatio: 0.10},
		})

		require.Len(t, got, 2)
		require.Equal(t, []string{"s-1", "s-2"}, []string{got[0].ShardID, got[1].ShardID})
	})

	t.Run("should compact on tombstone pressure", func(t *testing.T) {
		policy := sharding.Policy{CompactionEnabled: true, CompactionMinShardCount: 8, CompactionTombstoneRatio: 0.20}

		require.False(t, ShouldCompact(policy, []Shard{{ShardID: "s1"}}))
		require.True(t, ShouldCompact(policy, []Shard{{ShardID: "s1", TombstoneRatio: 0.25}, {ShardID: "s2"}}))
	})
}

func TestCompactionHasACeiling(t *testing.T) {
	// Merging never re-splits, so the densest tier used to compound: 4000
	// sealed shards produced a single 63 GiB one, which no host can index.
	policy := sharding.DefaultPolicy()
	const sealed = int64(32) << 20

	var shards []Shard
	next, largest := 0, int64(0)
	for range 500 {
		shards = append(shards, Shard{ShardID: fmt.Sprintf("s%d", next), SizeBytes: sealed})
		next++
		for {
			picked := Select(policy, shards)
			if len(picked) < 2 {
				break
			}
			merged, chosen := int64(0), map[string]bool{}
			for _, p := range picked {
				chosen[p.ShardID] = true
				merged += p.SizeBytes
			}
			keep := make([]Shard, 0, len(shards))
			for _, s := range shards {
				if !chosen[s.ShardID] {
					keep = append(keep, s)
				}
			}
			shards = append(keep, Shard{ShardID: fmt.Sprintf("s%d", next), SizeBytes: merged})
			next++
			largest = max(largest, merged)
		}
	}
	require.LessOrEqual(t, largest, policy.MaxShardBytes, "compaction outgrew the cap")
	require.Greater(t, largest, sealed, "nothing merged, so the cap proves nothing")
}

func TestTombstonedShardsAtTheCapStillCompact(t *testing.T) {
	// Sizing the merge by the file refused exactly the shards worth merging: a
	// pair at the cap that is mostly dead rows fits easily once they are gone.
	policy := sharding.DefaultPolicy()
	full := policy.MaxShardBytes
	shards := []Shard{
		{ShardID: "a", SizeBytes: full, TombstoneRatio: 0.9},
		{ShardID: "b", SizeBytes: full, TombstoneRatio: 0.9},
	}
	picked := Select(policy, shards)
	require.Len(t, picked, 2, "a shard at the cap could never shed its tombstones")

	merged := int64(0)
	for _, p := range picked {
		merged += LiveBytes(p)
	}
	require.LessOrEqual(t, merged, policy.MaxShardBytes)
}
