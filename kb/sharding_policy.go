package kb

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// ShardingPolicy defines thresholded shard/query/compaction controls.
type ShardingPolicy struct {
	ShardTriggerBytes           int64   `json:"shard_trigger_bytes"`
	ShardTriggerVectorRows      int     `json:"shard_trigger_vector_rows"`
	TargetShardBytes            int64   `json:"target_shard_bytes"`
	MaxVectorRowsPerShard       int     `json:"max_vector_rows_per_shard"`
	QueryShardFanout            int     `json:"query_shard_fanout"`
	QueryShardFanoutAdaptiveMax int     `json:"query_shard_fanout_adaptive_max"`
	QueryShardParallelism       int     `json:"query_shard_parallelism"`
	SmallKBMaxShards            int     `json:"small_kb_max_shards"`
	CompactionEnabled           bool    `json:"compaction_enabled"`
	CompactionMinShardCount     int     `json:"compaction_min_shard_count"`
	CompactionTombstoneRatio    float64 `json:"compaction_tombstone_ratio"`
}

// DefaultShardingPolicy returns multi-tenant sharding defaults.
func DefaultShardingPolicy() ShardingPolicy {
	return ShardingPolicy{
		ShardTriggerBytes:           67108864,
		ShardTriggerVectorRows:      150000,
		TargetShardBytes:            33554432,
		MaxVectorRowsPerShard:       75000,
		QueryShardFanout:            4,
		QueryShardFanoutAdaptiveMax: 6,
		QueryShardParallelism:       4,
		SmallKBMaxShards:            2,
		CompactionEnabled:           true,
		CompactionMinShardCount:     8,
		CompactionTombstoneRatio:    0.20,
	}
}

func normalizeShardingPolicy(policy ShardingPolicy) ShardingPolicy {
	defaults := DefaultShardingPolicy()

	if policy.ShardTriggerBytes > 0 {
		defaults.ShardTriggerBytes = policy.ShardTriggerBytes
	}
	if policy.ShardTriggerVectorRows > 0 {
		defaults.ShardTriggerVectorRows = policy.ShardTriggerVectorRows
	}
	if policy.TargetShardBytes > 0 {
		defaults.TargetShardBytes = policy.TargetShardBytes
	}
	if policy.MaxVectorRowsPerShard > 0 {
		defaults.MaxVectorRowsPerShard = policy.MaxVectorRowsPerShard
	}
	if policy.QueryShardFanout > 0 {
		defaults.QueryShardFanout = policy.QueryShardFanout
	}
	if policy.QueryShardFanoutAdaptiveMax > 0 {
		defaults.QueryShardFanoutAdaptiveMax = policy.QueryShardFanoutAdaptiveMax
	}
	if policy.QueryShardParallelism > 0 {
		defaults.QueryShardParallelism = policy.QueryShardParallelism
	}
	if policy.SmallKBMaxShards > 0 {
		defaults.SmallKBMaxShards = policy.SmallKBMaxShards
	}
	if policy.CompactionMinShardCount > 0 {
		defaults.CompactionMinShardCount = policy.CompactionMinShardCount
	}
	if policy.CompactionTombstoneRatio > 0 && policy.CompactionTombstoneRatio <= 1 {
		defaults.CompactionTombstoneRatio = policy.CompactionTombstoneRatio
	}

	return defaults
}

// ShardMetricsObserver receives shard-level metrics events from the DuckDB backend.
// KB satisfies this interface via its record* methods.
type ShardMetricsObserver interface {
	RecordShardCount(kbID string, count int)
	RecordShardExecutionFailure(kbID string)
	RecordShardFanout(kbID string, fanout int, capped bool)
	RecordShardExecution(kbID string, count int)
	RecordShardCacheAccess(kbID string, hit bool)
}

// RecordShardCount implements ShardMetricsObserver.
func (l *KB) RecordShardCount(kbID string, count int) { l.recordShardCount(kbID, count) }

// RecordShardExecutionFailure implements ShardMetricsObserver.
func (l *KB) RecordShardExecutionFailure(kbID string) { l.recordShardExecutionFailure(kbID) }

// RecordShardFanout implements ShardMetricsObserver.
func (l *KB) RecordShardFanout(kbID string, fanout int, capped bool) {
	l.recordShardFanout(kbID, fanout, capped)
}

// RecordShardExecution implements ShardMetricsObserver.
func (l *KB) RecordShardExecution(kbID string, count int) { l.recordShardExecution(kbID, count) }

// RecordShardCacheAccess implements ShardMetricsObserver.
func (l *KB) RecordShardCacheAccess(kbID string, hit bool) { l.recordShardCacheAccess(kbID, hit) }

type shardMetrics struct {
	ShardCount              int
	FanoutUsedTotal         uint64
	FanoutCappedTotal       uint64
	ShardExecTotal          uint64
	ShardExecFailuresTotal  uint64
	CompactionRunsTotal     uint64
	CompactionFailuresTotal uint64
	CompactionDurationTotal time.Duration
	ManifestCASConflicts    uint64
	ShardCacheHitsTotal     uint64
	ShardCacheMissesTotal   uint64
}

func (l *KB) recordShardCount(kbID string, shardCount int) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	if shardCount < 0 {
		shardCount = 0
	}
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	m.ShardCount = shardCount
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) recordShardFanout(kbID string, fanout int, capped bool) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	if fanout < 0 {
		fanout = 0
	}
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	m.FanoutUsedTotal += uint64(fanout)
	if capped {
		m.FanoutCappedTotal++
	}
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) recordShardExecution(kbID string, shardCount int) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	if shardCount < 0 {
		shardCount = 0
	}
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	m.ShardExecTotal += uint64(shardCount)
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) recordShardExecutionFailure(kbID string) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	m.ShardExecFailuresTotal++
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) recordCompactionResult(kbID string, duration time.Duration, result *CompactionPublishResult, err error) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	if err != nil {
		m.CompactionFailuresTotal++
	} else if result != nil && result.Performed {
		m.CompactionRunsTotal++
		if duration > 0 {
			m.CompactionDurationTotal += duration
		}
	}
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) recordManifestCASConflict(kbID string) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	slog.Default().Warn("manifest CAS conflict", "kb_id", kbID, "reason", "manifest_cas_conflict")
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	m.ManifestCASConflicts++
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) recordShardCacheAccess(kbID string, hit bool) {
	if l == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	l.mu.Lock()
	m := l.shardMetricsByKB[kbID]
	if hit {
		m.ShardCacheHitsTotal++
	} else {
		m.ShardCacheMissesTotal++
	}
	l.shardMetricsByKB[kbID] = m
	l.mu.Unlock()
}

func (l *KB) shardMetricsSnapshot() map[string]shardMetrics {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]shardMetrics, len(l.shardMetricsByKB))
	for kbID, m := range l.shardMetricsByKB {
		out[kbID] = m
	}
	return out
}

func (l *KB) ShardingOpenMetricsText() string {
	snap := l.shardMetricsSnapshot()
	if len(snap) == 0 {
		return ""
	}

	kbIDs := make([]string, 0, len(snap))
	for kbID := range snap {
		kbIDs = append(kbIDs, kbID)
	}
	sort.Strings(kbIDs)

	lines := []string{
		"# TYPE kbcore_shard_count gauge",
		"# TYPE kbcore_query_shard_fanout_used_total counter",
		"# TYPE kbcore_query_shard_fanout_capped_total counter",
		"# TYPE kbcore_query_shard_exec_total counter",
		"# TYPE kbcore_query_shard_exec_failures_total counter",
		"# TYPE kbcore_compaction_runs_total counter",
		"# TYPE kbcore_compaction_failures_total counter",
		"# TYPE kbcore_compaction_duration_seconds_sum counter",
		"# TYPE kbcore_manifest_cas_conflicts_total counter",
		"# TYPE kbcore_shard_cache_hits_total counter",
		"# TYPE kbcore_shard_cache_misses_total counter",
	}

	for _, kbID := range kbIDs {
		m := snap[kbID]
		labels := fmt.Sprintf("{kb_id=\"%s\"}", escapeMetricLabelValue(kbID))
		lines = append(lines, fmt.Sprintf("kbcore_shard_count%s %d", labels, m.ShardCount))
		lines = append(lines, fmt.Sprintf("kbcore_query_shard_fanout_used_total%s %d", labels, m.FanoutUsedTotal))
		lines = append(lines, fmt.Sprintf("kbcore_query_shard_fanout_capped_total%s %d", labels, m.FanoutCappedTotal))
		lines = append(lines, fmt.Sprintf("kbcore_query_shard_exec_total%s %d", labels, m.ShardExecTotal))
		lines = append(lines, fmt.Sprintf("kbcore_query_shard_exec_failures_total%s %d", labels, m.ShardExecFailuresTotal))
		lines = append(lines, fmt.Sprintf("kbcore_compaction_runs_total%s %d", labels, m.CompactionRunsTotal))
		lines = append(lines, fmt.Sprintf("kbcore_compaction_failures_total%s %d", labels, m.CompactionFailuresTotal))
		lines = append(lines, fmt.Sprintf("kbcore_compaction_duration_seconds_sum%s %.6f", labels, m.CompactionDurationTotal.Seconds()))
		lines = append(lines, fmt.Sprintf("kbcore_manifest_cas_conflicts_total%s %d", labels, m.ManifestCASConflicts))
		lines = append(lines, fmt.Sprintf("kbcore_shard_cache_hits_total%s %d", labels, m.ShardCacheHitsTotal))
		lines = append(lines, fmt.Sprintf("kbcore_shard_cache_misses_total%s %d", labels, m.ShardCacheMissesTotal))
	}

	return strings.Join(lines, "\n") + "\n"
}

func escapeMetricLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
