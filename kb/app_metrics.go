package kb

import (
	"runtime"
	"strings"
	"sync"
	"time"
)

type AppMetrics interface {
	RecordRequest(method, path string, status int, latencyMS int64)
	RecordEmbed(kbID string, latencyMS int64, err error)
	RecordQuery(kbID string, latencyMS int64, resultCount int, topDistance float32, err error)
	RecordIngest(kbID string, latencyMS int64, docCount int, chunkCount int, err error)
	Snapshot() MetricsSnapshot
}

type RouteStats struct {
	Count        int64 `json:"count"`
	ErrorCount   int64 `json:"error_count"`
	LatencySumMS int64 `json:"latency_sum_ms"`
	LatencyMinMS int64 `json:"latency_min_ms"`
	LatencyMaxMS int64 `json:"latency_max_ms"`
}

type EmbedStats struct {
	Count        int64 `json:"count"`
	ErrorCount   int64 `json:"error_count"`
	LatencySumMS int64 `json:"latency_sum_ms"`
	LatencyMaxMS int64 `json:"latency_max_ms"`
}

type QueryStats struct {
	Count          int64   `json:"count"`
	ErrorCount     int64   `json:"error_count"`
	LatencySumMS   int64   `json:"latency_sum_ms"`
	LatencyMaxMS   int64   `json:"latency_max_ms"`
	TotalResults   int64   `json:"total_results"`
	TopDistanceSum float64 `json:"top_distance_sum"`
}

type IngestStats struct {
	Count        int64 `json:"count"`
	ErrorCount   int64 `json:"error_count"`
	LatencySumMS int64 `json:"latency_sum_ms"`
	LatencyMaxMS int64 `json:"latency_max_ms"`
	TotalDocs    int64 `json:"total_docs"`
	TotalChunks  int64 `json:"total_chunks"`
}

type RecentRequest struct {
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	Status    int       `json:"status"`
	LatencyMS int64     `json:"latency_ms"`
	Timestamp time.Time `json:"timestamp"`
}

type RuntimeStats struct {
	HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
	Goroutines     int    `json:"goroutines"`
	NumGC          uint32 `json:"num_gc"`
	GCPauseNS      uint64 `json:"gc_pause_ns"`
}

type MetricsSnapshot struct {
	RouteStats     map[string]RouteStats  `json:"route_stats"`
	EmbedStats     map[string]EmbedStats  `json:"embed_stats"`
	QueryStats     map[string]QueryStats  `json:"query_stats"`
	IngestStats    map[string]IngestStats `json:"ingest_stats"`
	RecentRequests []RecentRequest        `json:"recent_requests"`
	Runtime        RuntimeStats           `json:"runtime"`
	UptimeSeconds  int64                  `json:"uptime_seconds"`
	StartTime      time.Time              `json:"start_time"`
}

// noop implementation: used when metrics are disabled.
type NoopAppMetrics struct{}

func (NoopAppMetrics) RecordRequest(method, path string, status int, latencyMS int64) {}

func (NoopAppMetrics) RecordEmbed(kbID string, latencyMS int64, err error) {}

func (NoopAppMetrics) RecordQuery(kbID string, latencyMS int64, resultCount int, topDistance float32, err error) {
}

func (NoopAppMetrics) RecordIngest(kbID string, latencyMS int64, docCount int, chunkCount int, err error) {
}

func (NoopAppMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{}
}

const appMetricsRecentCapacity = 200

// in-memory implementation: records metrics into local maps and a ring buffer of recent requests.
type InMemAppMetrics struct {
	mu sync.Mutex

	routeStats  map[string]RouteStats
	embedStats  map[string]EmbedStats
	queryStats  map[string]QueryStats
	ingestStats map[string]IngestStats

	recent      []RecentRequest
	recentNext  int
	recentCount int

	startTime time.Time
}

func NewInMemAppMetrics() *InMemAppMetrics {
	return &InMemAppMetrics{
		routeStats:  make(map[string]RouteStats),
		embedStats:  make(map[string]EmbedStats),
		queryStats:  make(map[string]QueryStats),
		ingestStats: make(map[string]IngestStats),
		recent:      make([]RecentRequest, appMetricsRecentCapacity),
		startTime:   time.Now().UTC(),
	}
}

func (m *InMemAppMetrics) RecordRequest(method, path string, status int, latencyMS int64) {
	if m == nil {
		return
	}

	method = strings.TrimSpace(strings.ToUpper(method))
	path = strings.TrimSpace(path)
	if method == "" {
		method = "UNKNOWN"
	}
	if path == "" {
		path = "/"
	}
	if latencyMS < 0 {
		latencyMS = 0
	}

	key := method + " " + path

	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.routeStats[key]
	v.Count++
	if status >= 400 {
		v.ErrorCount++
	}
	v.LatencySumMS += latencyMS
	if v.Count == 1 || latencyMS < v.LatencyMinMS {
		v.LatencyMinMS = latencyMS
	}
	if latencyMS > v.LatencyMaxMS {
		v.LatencyMaxMS = latencyMS
	}
	m.routeStats[key] = v

	m.appendRecentLocked(RecentRequest{
		Method:    method,
		Path:      path,
		Status:    status,
		LatencyMS: latencyMS,
		Timestamp: time.Now().UTC(),
	})
}

func (m *InMemAppMetrics) RecordEmbed(kbID string, latencyMS int64, err error) {
	if m == nil {
		return
	}
	kbID = normalizeMetricsKBID(kbID)
	if latencyMS < 0 {
		latencyMS = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.embedStats[kbID]
	v.Count++
	if err != nil {
		v.ErrorCount++
	}
	v.LatencySumMS += latencyMS
	if latencyMS > v.LatencyMaxMS {
		v.LatencyMaxMS = latencyMS
	}
	m.embedStats[kbID] = v
}

func (m *InMemAppMetrics) RecordQuery(kbID string, latencyMS int64, resultCount int, topDistance float32, err error) {
	if m == nil {
		return
	}
	kbID = normalizeMetricsKBID(kbID)
	if latencyMS < 0 {
		latencyMS = 0
	}
	if resultCount < 0 {
		resultCount = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.queryStats[kbID]
	v.Count++
	if err != nil {
		v.ErrorCount++
	}
	v.LatencySumMS += latencyMS
	if latencyMS > v.LatencyMaxMS {
		v.LatencyMaxMS = latencyMS
	}
	v.TotalResults += int64(resultCount)
	v.TopDistanceSum += float64(topDistance)
	m.queryStats[kbID] = v
}

func (m *InMemAppMetrics) RecordIngest(kbID string, latencyMS int64, docCount int, chunkCount int, err error) {
	if m == nil {
		return
	}
	kbID = normalizeMetricsKBID(kbID)
	if latencyMS < 0 {
		latencyMS = 0
	}
	if docCount < 0 {
		docCount = 0
	}
	if chunkCount < 0 {
		chunkCount = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.ingestStats[kbID]
	v.Count++
	if err != nil {
		v.ErrorCount++
	}
	v.LatencySumMS += latencyMS
	if latencyMS > v.LatencyMaxMS {
		v.LatencyMaxMS = latencyMS
	}
	v.TotalDocs += int64(docCount)
	v.TotalChunks += int64(chunkCount)
	m.ingestStats[kbID] = v
}

func (m *InMemAppMetrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}

	m.mu.Lock()
	out := MetricsSnapshot{
		RouteStats:     copyMap(m.routeStats),
		EmbedStats:     copyMap(m.embedStats),
		QueryStats:     copyMap(m.queryStats),
		IngestStats:    copyMap(m.ingestStats),
		RecentRequests: m.recentSnapshotLocked(),
		StartTime:      m.startTime,
		UptimeSeconds:  int64(time.Since(m.startTime).Seconds()),
	}
	m.mu.Unlock()

	// read mem stats outside the lock: runtime.ReadMemStats stops the world
	// and holding m.mu during that pause would block all record calls.
	var rt runtime.MemStats
	runtime.ReadMemStats(&rt)
	out.Runtime = RuntimeStats{
		HeapAllocBytes: rt.HeapAlloc,
		Goroutines:     runtime.NumGoroutine(),
		NumGC:          rt.NumGC,
		GCPauseNS:      rt.PauseTotalNs,
	}

	return out
}

func (m *InMemAppMetrics) appendRecentLocked(entry RecentRequest) {
	m.recent[m.recentNext] = entry
	m.recentNext = (m.recentNext + 1) % len(m.recent)
	if m.recentCount < len(m.recent) {
		m.recentCount++
	}
}

func (m *InMemAppMetrics) recentSnapshotLocked() []RecentRequest {
	if m.recentCount == 0 {
		return []RecentRequest{}
	}
	out := make([]RecentRequest, 0, m.recentCount)
	start := (m.recentNext - m.recentCount + len(m.recent)) % len(m.recent)
	for i := 0; i < m.recentCount; i++ {
		idx := (start + i) % len(m.recent)
		out = append(out, m.recent[idx])
	}
	return out
}

func normalizeMetricsKBID(kbID string) string {
	kbID = strings.TrimSpace(kbID)
	if kbID == "" {
		return "default"
	}
	return kbID
}

// copyMap returns a shallow copy of a map with string keys.
func copyMap[V any](in map[string]V) map[string]V {
	out := make(map[string]V, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
