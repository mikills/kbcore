package compact

import (
	"math"
	"sort"
	"time"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/compactionplan"
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

// PartitionForReconstruct groups shards so each group's live bytes fit within
// maxBytes, bounding a sequential grouped rebuild to one resident group at a
// time. This is a thin delegate over compactionplan.PartitionForReconstruct,
// the single source of truth for grouping semantics: ordering is pinned to
// SortAndLimit order (tombstone ratio desc, size asc, shard id asc), not
// manifest order, so packing is deterministic no matter how the manifest
// lists shards. Sizing uses live bytes (see liveShardBytes), which is a
// live-bytes bound, not a bound on copied bytes: the merge copies all docs
// rows unfiltered and never consults doc_tombstones, so a group whose shards
// carry nonzero tombstone ratios can copy more than the cap. That is latent
// today: sealed shards carry TombstoneRatio 0 (snapshot_shards.go,
// compaction.go), inherited from Fit's live-bytes sizing.
//
// A single shard over the cap still forms its own group: a shard cannot be
// split here, so the caller merges it alone. Live bytes, not file bytes, are
// capped; HNSW/FTS overhead can push the file past the cap.
func PartitionForReconstruct(shards []kb.SnapshotShardMetadata, maxBytes int64) [][]kb.SnapshotShardMetadata {
	if len(shards) == 0 {
		return nil
	}
	planShards := make([]compactionplan.Shard, len(shards))
	for i, shard := range shards {
		planShards[i] = compactionplan.Shard{
			ShardID:        shard.ShardID,
			SizeBytes:      shard.SizeBytes,
			TombstoneRatio: shard.TombstoneRatio,
		}
	}
	groups := compactionplan.PartitionForReconstruct(planShards, maxBytes)
	// Map back to the original metadata. ShardIDs are unique IDs in practice
	// (compaction_test.go:176-182 asserts no dups); FIFO best-effort otherwise.
	pending := make(map[string][]kb.SnapshotShardMetadata, len(shards))
	for _, shard := range shards {
		pending[shard.ShardID] = append(pending[shard.ShardID], shard)
	}
	out := make([][]kb.SnapshotShardMetadata, len(groups))
	for i, group := range groups {
		mapped := make([]kb.SnapshotShardMetadata, len(group))
		for j, planned := range group {
			queue := pending[planned.ShardID]
			mapped[j] = queue[0]
			pending[planned.ShardID] = queue[1:]
		}
		out[i] = mapped
	}
	return out
}

// liveShardBytes is what a shard contributes to group sizing: the file size
// discounted by the tombstone ratio. It is a sizing estimate, not a merge
// filter: the merge copies all docs rows unfiltered (reconstruct
// MergeShardIntoDB never reads doc_tombstones), so live bytes bound the
// group only when tombstone ratios are 0, which holds for sealed shards
// today. Sizing by the raw file would strand a mostly-tombstoned shard at
// the cap with no way to shrink, so live bytes are used instead.
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
