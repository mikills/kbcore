package compact

import (
	"math"
	"sort"
	"time"

	kb "github.com/mikills/minnow/kb"
)

const maxCompactionCandidates = 1 << 2

func BuildManifest(
	kbID string,
	current *kb.SnapshotShardManifest,
	replaced []kb.SnapshotShardMetadata,
	replacement kb.SnapshotShardMetadata,
) kb.SnapshotShardManifest {
	replacedByID := make(map[string]struct{}, len(replaced))
	for _, shard := range replaced {
		replacedByID[shard.ShardID] = struct{}{}
	}

	nextShards := make([]kb.SnapshotShardMetadata, 0, len(current.Shards)-len(replaced)+1)
	inserted := false
	for _, shard := range current.Shards {
		if _, drop := replacedByID[shard.ShardID]; drop {
			if !inserted {
				nextShards = append(nextShards, replacement)
				inserted = true
			}
			continue
		}
		nextShards = append(nextShards, shard)
	}
	if !inserted {
		nextShards = append(nextShards, replacement)
	}

	total := int64(0)
	for _, shard := range nextShards {
		total += shard.SizeBytes
	}

	return kb.SnapshotShardManifest{
		SchemaVersion:  current.SchemaVersion,
		Layout:         current.Layout,
		FormatKind:     current.FormatKind,
		FormatVersion:  current.FormatVersion,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: total,
		Shards:         nextShards,
	}
}

func SelectCandidatesWithReason(
	policy kb.ShardingPolicy,
	manifest *kb.SnapshotShardManifest,
) ([]kb.SnapshotShardMetadata, string) {
	policy = kb.NormalizeShardingPolicy(policy)
	if !shouldCompact(policy, manifest) {
		return nil, "no_compaction_debt"
	}

	tiers := make(map[int][]kb.SnapshotShardMetadata)
	for _, shard := range manifest.Shards {
		if shard.SizeBytes <= 0 || liveShardBytes(shard) >= policy.MaxShardBytes {
			continue
		}
		tier := shardSizeTier(shard.SizeBytes, policy.TargetShardBytes)
		tiers[tier] = append(tiers[tier], shard)
	}

	if tier, ok := densestTier(tiers); ok && len(tiers[tier]) >= 2 {
		picked := fitWithinMaxShard(sortAndLimitCompactionCandidates(tiers[tier], maxCompactionCandidates), policy.MaxShardBytes)
		if len(picked) >= 2 {
			return picked, "size_tier"
		}
	}

	pressure := make([]kb.SnapshotShardMetadata, 0, len(manifest.Shards))
	for _, shard := range manifest.Shards {
		if liveShardBytes(shard) < policy.MaxShardBytes && shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			pressure = append(pressure, shard)
		}
	}
	picked := fitWithinMaxShard(sortAndLimitCompactionCandidates(pressure, maxCompactionCandidates), policy.MaxShardBytes)
	if len(picked) < 2 {
		return nil, "insufficient_candidates"
	}

	return picked, "tombstone_pressure"
}

// fitWithinMaxShard stops before the merged shard outgrows what a host can
// index. Compaction never re-splits, so without it the densest tier merges
// forever.
func fitWithinMaxShard(candidates []kb.SnapshotShardMetadata, maxBytes int64) []kb.SnapshotShardMetadata {
	merged := int64(0)
	for i, candidate := range candidates {
		if merged+liveShardBytes(candidate) > maxBytes {
			return candidates[:i]
		}
		merged += liveShardBytes(candidate)
	}
	return candidates
}

// liveShardBytes is what a shard contributes to a merge. Tombstoned rows are
// dropped on the way in, so sizing by the file would strand a shard at the cap
// with no way to shrink.
func liveShardBytes(shard kb.SnapshotShardMetadata) int64 {
	ratio := min(max(shard.TombstoneRatio, 0), 1)
	return int64(float64(shard.SizeBytes) * (1 - ratio))
}

func shouldCompact(policy kb.ShardingPolicy, manifest *kb.SnapshotShardManifest) bool {
	if !policy.CompactionEnabled || manifest == nil || len(manifest.Shards) < 2 {
		return false
	}
	if len(manifest.Shards) >= policy.CompactionMinShardCount {
		return true
	}
	for _, shard := range manifest.Shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			return true
		}
	}
	return false
}

func shardSizeTier(sizeBytes, targetBytes int64) int {
	if sizeBytes <= 0 || targetBytes <= 0 {
		return 0
	}
	ratio := float64(sizeBytes) / float64(targetBytes)
	return int(math.Round(math.Log2(ratio)))
}

func densestTier(tiers map[int][]kb.SnapshotShardMetadata) (int, bool) {
	bestTier := 0
	bestCount := 0
	found := false
	for tier, shards := range tiers {
		count := len(shards)
		if !found || count > bestCount || (count == bestCount && absInt(tier) < absInt(bestTier)) ||
			(count == bestCount && absInt(tier) == absInt(bestTier) && tier < bestTier) {
			bestTier = tier
			bestCount = count
			found = true
		}
	}
	return bestTier, found
}

func sortAndLimitCompactionCandidates(candidates []kb.SnapshotShardMetadata, limit int) []kb.SnapshotShardMetadata {
	sorted := append([]kb.SnapshotShardMetadata(nil), candidates...)
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
