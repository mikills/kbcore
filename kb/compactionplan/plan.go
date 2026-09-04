package compactionplan

import (
	"math"
	"sort"

	"github.com/mikills/minnow/kb/sharding"
)

const MaxCandidates = 1 << 2

type Shard struct {
	ShardID        string
	SizeBytes      int64
	TombstoneRatio float64
}

func SelectWithReason(policy sharding.Policy, shards []Shard) ([]Shard, string) {
	policy = sharding.NormalizePolicy(policy)
	if !ShouldCompact(policy, shards) {
		return nil, "no_compaction_debt"
	}
	tiers := make(map[int][]Shard)
	for _, shard := range shards {
		if shard.SizeBytes <= 0 || LiveBytes(shard) >= policy.MaxShardBytes {
			continue
		}
		tier := SizeTier(shard.SizeBytes, policy.TargetShardBytes)
		tiers[tier] = append(tiers[tier], shard)
	}
	if tier, ok := densestTier(tiers); ok && len(tiers[tier]) >= 2 {
		if picked := Fit(SortAndLimit(tiers[tier], MaxCandidates), policy.MaxShardBytes); len(picked) >= 2 {
			return picked, "size_tier"
		}
	}
	pressure := make([]Shard, 0, len(shards))
	for _, shard := range shards {
		if LiveBytes(shard) < policy.MaxShardBytes && shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			pressure = append(pressure, shard)
		}
	}
	picked := Fit(SortAndLimit(pressure, MaxCandidates), policy.MaxShardBytes)
	if len(picked) < 2 {
		return nil, "insufficient_candidates"
	}
	return picked, "tombstone_pressure"
}

// Fit takes candidates in order while the shard they would merge into stays
// under maxBytes. Without it the densest tier merges forever, and the index
// build over the result outgrows every host.
func Fit(candidates []Shard, maxBytes int64) []Shard {
	merged := int64(0)
	for i, candidate := range candidates {
		if merged+LiveBytes(candidate) > maxBytes {
			return candidates[:i]
		}
		merged += LiveBytes(candidate)
	}
	return candidates
}

// LiveBytes is what a shard contributes to a merge. Tombstoned rows are dropped
// on the way in, so sizing by the file would refuse to compact the shards that
// most need it, and would strand one at the cap with no way to shrink.
func LiveBytes(shard Shard) int64 {
	ratio := min(max(shard.TombstoneRatio, 0), 1)
	return int64(float64(shard.SizeBytes) * (1 - ratio))
}

func Select(policy sharding.Policy, shards []Shard) []Shard {
	candidates, _ := SelectWithReason(policy, shards)
	return candidates
}

func ShouldCompact(policy sharding.Policy, shards []Shard) bool {
	if !policy.CompactionEnabled || len(shards) < 2 {
		return false
	}
	if len(shards) >= policy.CompactionMinShardCount {
		return true
	}
	for _, shard := range shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			return true
		}
	}
	return false
}

func SizeTier(sizeBytes, targetBytes int64) int {
	if sizeBytes <= 0 || targetBytes <= 0 {
		return 0
	}
	ratio := float64(sizeBytes) / float64(targetBytes)
	return int(math.Round(math.Log2(ratio)))
}

func densestTier(tiers map[int][]Shard) (int, bool) {
	bestTier := 0
	bestCount := 0
	found := false
	for tier, shards := range tiers {
		count := len(shards)
		if !found || count > bestCount || count == bestCount && absInt(tier) < absInt(bestTier) ||
			count == bestCount && absInt(tier) == absInt(bestTier) && tier < bestTier {
			bestTier = tier
			bestCount = count
			found = true
		}
	}
	return bestTier, found
}

func SortAndLimit(candidates []Shard, limit int) []Shard {
	sorted := append([]Shard(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		if left.TombstoneRatio != right.TombstoneRatio {
			return left.TombstoneRatio > right.TombstoneRatio
		}
		if left.SizeBytes != right.SizeBytes {
			return left.SizeBytes < right.SizeBytes
		}
		return left.ShardID < right.ShardID
	})
	if limit > 0 && len(sorted) > limit {
		sorted = sorted[:limit]
	}
	return sorted
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
