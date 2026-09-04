package sharding

type Policy struct {
	ShardTriggerBytes           int64   `json:"shard_trigger_bytes"`
	ShardTriggerVectorRows      int     `json:"shard_trigger_vector_rows"`
	TargetShardBytes            int64   `json:"target_shard_bytes"`
	MaxShardBytes               int64   `json:"max_shard_bytes"`
	MaxVectorRowsPerShard       int     `json:"max_vector_rows_per_shard"`
	QueryShardFanout            int     `json:"query_shard_fanout"`
	QueryShardFanoutAdaptiveMax int     `json:"query_shard_fanout_adaptive_max"`
	QueryShardParallelism       int     `json:"query_shard_parallelism"`
	QueryShardLocalTopKMult     int     `json:"query_shard_local_topk_multiplier"`
	SmallKBMaxShards            int     `json:"small_kb_max_shards"`
	CompactionEnabled           bool    `json:"compaction_enabled"`
	CompactionEnabledSet        bool    `json:"-"`
	CompactionMinShardCount     int     `json:"compaction_min_shard_count"`
	CompactionTombstoneRatio    float64 `json:"compaction_tombstone_ratio"`
}

const (
	defaultQueryShardFanout      = 1 << 2
	defaultQueryShardParallelism = 1 << 2
)

func DefaultPolicy() Policy {
	return Policy{
		ShardTriggerBytes:           67108864,
		ShardTriggerVectorRows:      150000,
		TargetShardBytes:            33554432,
		MaxShardBytes:               67108864,
		MaxVectorRowsPerShard:       75000,
		QueryShardFanout:            defaultQueryShardFanout,
		QueryShardFanoutAdaptiveMax: 6,
		QueryShardParallelism:       defaultQueryShardParallelism,
		QueryShardLocalTopKMult:     2,
		SmallKBMaxShards:            2,
		CompactionEnabled:           true,
		CompactionMinShardCount:     8,
		CompactionTombstoneRatio:    0.20,
	}
}

func NormalizePolicy(policy Policy) Policy {
	defaults := DefaultPolicy()
	applyPositiveOverrides(&defaults, policy)
	if policy.CompactionTombstoneRatio > 0 && policy.CompactionTombstoneRatio <= 1 {
		defaults.CompactionTombstoneRatio = policy.CompactionTombstoneRatio
	}
	if policy.CompactionEnabledSet || policy.CompactionEnabled {
		defaults.CompactionEnabled = policy.CompactionEnabled
	}
	defaults.CompactionEnabledSet = policy.CompactionEnabledSet
	// A shard nobody capped grows without limit: compaction merges the densest
	// tier and never re-splits, so each round multiplies by the fan-in.
	if defaults.MaxShardBytes < defaults.TargetShardBytes {
		defaults.MaxShardBytes = defaults.TargetShardBytes
	}
	return defaults
}

func applyPositiveOverrides(defaults *Policy, policy Policy) {
	applyPositiveInt64(&defaults.ShardTriggerBytes, policy.ShardTriggerBytes)
	applyPositiveInt(&defaults.ShardTriggerVectorRows, policy.ShardTriggerVectorRows)
	applyPositiveInt64(&defaults.TargetShardBytes, policy.TargetShardBytes)
	applyPositiveInt64(&defaults.MaxShardBytes, policy.MaxShardBytes)
	applyPositiveInt(&defaults.MaxVectorRowsPerShard, policy.MaxVectorRowsPerShard)
	applyPositiveInt(&defaults.QueryShardFanout, policy.QueryShardFanout)
	applyPositiveInt(&defaults.QueryShardFanoutAdaptiveMax, policy.QueryShardFanoutAdaptiveMax)
	applyPositiveInt(&defaults.QueryShardParallelism, policy.QueryShardParallelism)
	applyPositiveInt(&defaults.QueryShardLocalTopKMult, policy.QueryShardLocalTopKMult)
	applyPositiveInt(&defaults.SmallKBMaxShards, policy.SmallKBMaxShards)
	applyPositiveInt(&defaults.CompactionMinShardCount, policy.CompactionMinShardCount)
}

func applyPositiveInt(target *int, value int) {
	if value > 0 {
		*target = value
	}
}

func applyPositiveInt64(target *int64, value int64) {
	if value > 0 {
		*target = value
	}
}
