package metrics

import (
	"hash/fnv"
	"maps"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type App interface {
	RecordRequest(method, path string, status int, latencyMS int64)
	RecordEmbed(kbID string, latencyMS int64, err error)
	RecordQuery(kbID string, latencyMS int64, resultCount int, topDistance float32, err error)
	RecordIngest(kbID string, latencyMS int64, docCount int, chunkCount int, err error)
	RecordWorkerTick(kind, workerID, outcome string, latencyMS int64)
	RecordMediaUpload(kbID, contentType string, sizeBytes int64, err error)
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

// WorkerStats captures per-(kind, outcome) worker tick counts.
type WorkerStats struct {
	Count        int64 `json:"count"`
	LatencySumMS int64 `json:"latency_sum_ms"`
	LatencyMaxMS int64 `json:"latency_max_ms"`
}

// MediaUploadStats captures per-kb upload totals.
type MediaUploadStats struct {
	Count      int64 `json:"count"`
	ErrorCount int64 `json:"error_count"`
	TotalBytes int64 `json:"total_bytes"`
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
	RouteStats       map[string]RouteStats       `json:"route_stats"`
	EmbedStats       map[string]EmbedStats       `json:"embed_stats"`
	QueryStats       map[string]QueryStats       `json:"query_stats"`
	IngestStats      map[string]IngestStats      `json:"ingest_stats"`
	WorkerStats      map[string]WorkerStats      `json:"worker_stats"`
	MediaUploadStats map[string]MediaUploadStats `json:"media_upload_stats"`
	RecentRequests   []RecentRequest             `json:"recent_requests"`
	Runtime          RuntimeStats                `json:"runtime"`
	UptimeSeconds    int64                       `json:"uptime_seconds"`
	StartTime        time.Time                   `json:"start_time"`
}

// noop implementation: used when metrics are disabled.
type NoopApp struct{}

func (NoopApp) RecordRequest(method, path string, status int, latencyMS int64) {}

func (NoopApp) RecordEmbed(kbID string, latencyMS int64, err error) {}

func (NoopApp) RecordQuery(kbID string, latencyMS int64, resultCount int, topDistance float32, err error) {
}

func (NoopApp) RecordIngest(kbID string, latencyMS int64, docCount int, chunkCount int, err error) {
}

func (NoopApp) RecordWorkerTick(kind, workerID, outcome string, latencyMS int64) {}

func (NoopApp) RecordMediaUpload(kbID, contentType string, sizeBytes int64, err error) {}

func (NoopApp) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{}
}

const (
	RecentCapacity          = 200
	MetricsCardinalityLimit = 1024
	metricsOverflowKey      = "_other"
)

// in-memory implementation: records metrics into local maps and a ring buffer of recent requests.
type InMemApp struct {
	mu sync.Mutex

	routeStats       map[string]RouteStats
	embedStats       map[string]EmbedStats
	queryStats       map[string]QueryStats
	ingestStats      map[string]IngestStats
	workerStats      map[string]WorkerStats
	mediaUploadStats map[string]MediaUploadStats

	recent      []RecentRequest
	recentNext  int
	recentCount int

	startTime time.Time
}

func NewInMemApp() *InMemApp {
	return &InMemApp{
		routeStats:       make(map[string]RouteStats),
		embedStats:       make(map[string]EmbedStats),
		queryStats:       make(map[string]QueryStats),
		ingestStats:      make(map[string]IngestStats),
		workerStats:      make(map[string]WorkerStats),
		mediaUploadStats: make(map[string]MediaUploadStats),
		recent:           make([]RecentRequest, RecentCapacity),
		startTime:        time.Now().UTC(),
	}
}

func (m *InMemApp) RecordRequest(method, path string, status int, latencyMS int64) {
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

	key = boundedMetricsKey(m.routeStats, key)
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

func (m *InMemApp) RecordEmbed(kbID string, latencyMS int64, err error) {
	if m == nil {
		return
	}
	kbID = normalizeMetricsKBID(kbID)
	if latencyMS < 0 {
		latencyMS = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	kbID = boundedMetricsKey(m.embedStats, kbID)
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

func (m *InMemApp) RecordQuery(kbID string, latencyMS int64, resultCount int, topDistance float32, err error) {
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
	kbID = boundedMetricsKey(m.queryStats, kbID)
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

func (m *InMemApp) RecordIngest(kbID string, latencyMS int64, docCount int, chunkCount int, err error) {
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
	kbID = boundedMetricsKey(m.ingestStats, kbID)
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

func (m *InMemApp) RecordWorkerTick(kind, workerID, outcome string, latencyMS int64) {
	if m == nil {
		return
	}
	if latencyMS < 0 {
		latencyMS = 0
	}
	key := kind + "|" + outcome
	m.mu.Lock()
	defer m.mu.Unlock()
	key = boundedMetricsKey(m.workerStats, key)
	v := m.workerStats[key]
	v.Count++
	v.LatencySumMS += latencyMS
	if latencyMS > v.LatencyMaxMS {
		v.LatencyMaxMS = latencyMS
	}
	m.workerStats[key] = v
}

func (m *InMemApp) RecordMediaUpload(kbID, contentType string, sizeBytes int64, err error) {
	if m == nil {
		return
	}
	kbID = normalizeMetricsKBID(kbID)
	key := kbID + "|" + normalizeContentType(contentType)
	m.mu.Lock()
	defer m.mu.Unlock()
	key = boundedMetricsKey(m.mediaUploadStats, key)
	v := m.mediaUploadStats[key]
	v.Count++
	if err != nil {
		v.ErrorCount++
	}
	v.TotalBytes += sizeBytes
	m.mediaUploadStats[key] = v
}

// normalizeContentType maps an arbitrary user-supplied Content-Type header
// down to a fixed allowlist so metric keys cannot be used as a cardinality
// attack vector. Charset / boundary parameters after "." are stripped before
// matching.
func boundedMetricsKey[V any](values map[string]V, key string) string {
	key = compactMetricsKey(key)
	if _, exists := values[key]; exists {
		return key
	}
	if len(values) < MetricsCardinalityLimit-1 {
		return key
	}
	return metricsOverflowKey
}

func compactMetricsKey(key string) string {
	const maxKeyBytes = 256
	if len(key) <= maxKeyBytes {
		return key
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	suffix := "~" + strconv.FormatUint(hash.Sum64(), 16)
	return key[:maxKeyBytes-len(suffix)] + suffix
}

func normalizeContentType(ct string) string {
	ct = strings.SplitN(strings.TrimSpace(ct), ";", 2)[0]
	ct = strings.ToLower(strings.TrimSpace(ct))
	switch ct {
	case "image/png", "image/jpeg", "image/webp", "image/gif",
		"application/pdf",
		"text/plain", "text/html", "text/markdown", "text/csv",
		"application/json",
		"application/octet-stream":
		return ct
	}
	if strings.HasPrefix(ct, "image/") {
		return "image/other"
	}
	if strings.HasPrefix(ct, "text/") {
		return "text/other"
	}
	return "other"
}

func (m *InMemApp) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}

	m.mu.Lock()
	out := MetricsSnapshot{
		RouteStats:       copyMap(m.routeStats),
		EmbedStats:       copyMap(m.embedStats),
		QueryStats:       copyMap(m.queryStats),
		IngestStats:      copyMap(m.ingestStats),
		WorkerStats:      copyMap(m.workerStats),
		MediaUploadStats: copyMap(m.mediaUploadStats),
		RecentRequests:   m.recentSnapshotLocked(),
		StartTime:        m.startTime,
		UptimeSeconds:    int64(time.Since(m.startTime).Seconds()),
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

func (m *InMemApp) appendRecentLocked(entry RecentRequest) {
	m.recent[m.recentNext] = entry
	m.recentNext = (m.recentNext + 1) % len(m.recent)
	if m.recentCount < len(m.recent) {
		m.recentCount++
	}
}

func (m *InMemApp) recentSnapshotLocked() []RecentRequest {
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
	maps.Copy(out, in)
	return out
}
