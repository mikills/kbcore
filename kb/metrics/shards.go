package metrics

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

type ShardPublishResult interface{ IsPerformed() bool }

type ShardStats struct {
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

type ShardRegistry struct {
	mu   sync.Mutex
	byKB map[string]ShardStats
}

func NewShardRegistry() *ShardRegistry { return &ShardRegistry{byKB: make(map[string]ShardStats)} }

func (r *ShardRegistry) RecordCount(kbID string, shardCount int) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	if shardCount < 0 {
		shardCount = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	m.ShardCount = shardCount
	r.byKB[kbID] = m
}

func (r *ShardRegistry) RecordFanout(kbID string, fanout int, capped bool) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	if fanout < 0 {
		fanout = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	m.FanoutUsedTotal += uint64(fanout)
	if capped {
		m.FanoutCappedTotal++
	}
	r.byKB[kbID] = m
}

func (r *ShardRegistry) RecordExecution(kbID string, shardCount int) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	if shardCount < 0 {
		shardCount = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	m.ShardExecTotal += uint64(shardCount)
	r.byKB[kbID] = m
}

func (r *ShardRegistry) RecordExecutionFailure(kbID string) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	m.ShardExecFailuresTotal++
	r.byKB[kbID] = m
}

func (r *ShardRegistry) RecordCompaction(kbID string, duration time.Duration, performed bool, err error) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	if err != nil {
		m.CompactionFailuresTotal++
	} else if performed {
		m.CompactionRunsTotal++
		if duration > 0 {
			m.CompactionDurationTotal += duration
		}
	}
	r.byKB[kbID] = m
}

func (r *ShardRegistry) RecordManifestCASConflict(kbID string) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	m.ManifestCASConflicts++
	r.byKB[kbID] = m
}

func (r *ShardRegistry) RecordCacheAccess(kbID string, hit bool) {
	if r == nil || strings.TrimSpace(kbID) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kbID = boundedMetricsKey(r.byKB, kbID)
	m := r.byKB[kbID]
	if hit {
		m.ShardCacheHitsTotal++
	} else {
		m.ShardCacheMissesTotal++
	}
	r.byKB[kbID] = m
}

func (r *ShardRegistry) Snapshot() map[string]ShardStats {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]ShardStats, len(r.byKB))
	maps.Copy(out, r.byKB)
	return out
}

func (r *ShardRegistry) OpenMetricsText() string {
	snap := r.Snapshot()
	if len(snap) == 0 {
		return ""
	}
	kbIDs := make([]string, 0, len(snap))
	for kbID := range snap {
		kbIDs = append(kbIDs, kbID)
	}
	sort.Strings(kbIDs)
	lines := []string{
		"# TYPE minnow_shard_count gauge",
		"# TYPE minnow_query_shard_fanout_used_total counter",
		"# TYPE minnow_query_shard_fanout_capped_total counter",
		"# TYPE minnow_query_shard_exec_total counter",
		"# TYPE minnow_query_shard_exec_failures_total counter",
		"# TYPE minnow_compaction_runs_total counter",
		"# TYPE minnow_compaction_failures_total counter",
		"# TYPE minnow_compaction_duration_seconds_sum counter",
		"# TYPE minnow_manifest_cas_conflicts_total counter",
		"# TYPE minnow_shard_cache_hits_total counter",
		"# TYPE minnow_shard_cache_misses_total counter",
	}
	for _, kbID := range kbIDs {
		m := snap[kbID]
		labels := fmt.Sprintf("{kb_id=\"%s\"}", EscapeMetricLabelValue(kbID))
		lines = append(lines, fmt.Sprintf("minnow_shard_count%s %d", labels, m.ShardCount))
		lines = append(lines, fmt.Sprintf("minnow_query_shard_fanout_used_total%s %d", labels, m.FanoutUsedTotal))
		lines = append(lines, fmt.Sprintf("minnow_query_shard_fanout_capped_total%s %d", labels, m.FanoutCappedTotal))
		lines = append(lines, fmt.Sprintf("minnow_query_shard_exec_total%s %d", labels, m.ShardExecTotal))
		lines = append(
			lines,
			fmt.Sprintf("minnow_query_shard_exec_failures_total%s %d", labels, m.ShardExecFailuresTotal),
		)
		lines = append(lines, fmt.Sprintf("minnow_compaction_runs_total%s %d", labels, m.CompactionRunsTotal))
		lines = append(lines, fmt.Sprintf("minnow_compaction_failures_total%s %d", labels, m.CompactionFailuresTotal))
		lines = append(
			lines,
			fmt.Sprintf("minnow_compaction_duration_seconds_sum%s %.6f", labels, m.CompactionDurationTotal.Seconds()),
		)
		lines = append(lines, fmt.Sprintf("minnow_manifest_cas_conflicts_total%s %d", labels, m.ManifestCASConflicts))
		lines = append(lines, fmt.Sprintf("minnow_shard_cache_hits_total%s %d", labels, m.ShardCacheHitsTotal))
		lines = append(lines, fmt.Sprintf("minnow_shard_cache_misses_total%s %d", labels, m.ShardCacheMissesTotal))
	}
	return strings.Join(lines, "\n") + "\n"
}

func EscapeMetricLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
