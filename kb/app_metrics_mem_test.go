package kb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppMetricsInMem(t *testing.T) {
	t.Run("record_request", testAppMetricsRecordRequest)
	t.Run("record_by_kb", testAppMetricsRecordByKB)
	t.Run("snapshot_returns_copies", testAppMetricsSnapshotCopies)
	t.Run("concurrent_record", testAppMetricsConcurrentRecord)
	t.Run("recent_ring_buffer_wraps", testAppMetricsRecentRingBufferWraps)
}

func testAppMetricsRecordRequest(t *testing.T) {
	type requestCall struct {
		method    string
		path      string
		status    int
		latencyMS int64
	}

	tests := []struct {
		name              string
		calls             []requestCall
		routeKey          string
		expectedRoute     RouteStats
		expectedRecentLen int
		expectedFirstPath string
	}{
		{
			name: "aggregates_count_errors_and_latency",
			calls: []requestCall{
				{method: "GET", path: "/healthz", status: 200, latencyMS: 12},
				{method: "GET", path: "/healthz", status: 500, latencyMS: 30},
			},
			routeKey:          "GET /healthz",
			expectedRoute:     RouteStats{Count: 2, ErrorCount: 1, LatencySumMS: 42, LatencyMinMS: 12, LatencyMaxMS: 30},
			expectedRecentLen: 2,
			expectedFirstPath: "/healthz",
		},
		{
			name: "tracks_recent_requests_in_order",
			calls: []requestCall{
				{method: "POST", path: "/rag/ingest", status: 201, latencyMS: 7},
				{method: "POST", path: "/rag/query", status: 200, latencyMS: 9},
				{method: "POST", path: "/rag/ingest", status: 200, latencyMS: 6},
			},
			routeKey:          "POST /rag/ingest",
			expectedRoute:     RouteStats{Count: 2, ErrorCount: 0, LatencySumMS: 13, LatencyMinMS: 6, LatencyMaxMS: 7},
			expectedRecentLen: 3,
			expectedFirstPath: "/rag/ingest",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewInMemAppMetrics()
			require.NotNil(t, m)

			for _, call := range tc.calls {
				m.RecordRequest(call.method, call.path, call.status, call.latencyMS)
			}

			snap := m.Snapshot()
			stats, ok := snap.RouteStats[tc.routeKey]
			require.True(t, ok)
			assert.EqualValues(t, tc.expectedRoute.Count, stats.Count)
			assert.EqualValues(t, tc.expectedRoute.ErrorCount, stats.ErrorCount)
			assert.EqualValues(t, tc.expectedRoute.LatencySumMS, stats.LatencySumMS)
			assert.EqualValues(t, tc.expectedRoute.LatencyMinMS, stats.LatencyMinMS)
			assert.EqualValues(t, tc.expectedRoute.LatencyMaxMS, stats.LatencyMaxMS)
			require.Len(t, snap.RecentRequests, tc.expectedRecentLen)
			assert.Equal(t, tc.expectedFirstPath, snap.RecentRequests[0].Path)
		})
	}
}

func testAppMetricsRecordByKB(t *testing.T) {
	t.Run("embed", func(t *testing.T) {
		type embedCall struct {
			kbID      string
			latencyMS int64
			err       error
		}

		tests := []struct {
			name     string
			calls    []embedCall
			kbID     string
			expected EmbedStats
		}{
			{
				name: "single_kb_with_error",
				calls: []embedCall{
					{kbID: "kb-a", latencyMS: 7, err: nil},
					{kbID: "kb-a", latencyMS: 9, err: assert.AnError},
				},
				kbID:     "kb-a",
				expected: EmbedStats{Count: 2, ErrorCount: 1, LatencySumMS: 16, LatencyMaxMS: 9},
			},
			{
				name: "isolates_per_kb_aggregate",
				calls: []embedCall{
					{kbID: "kb-a", latencyMS: 4, err: nil},
					{kbID: "kb-b", latencyMS: 10, err: nil},
					{kbID: "kb-a", latencyMS: 6, err: nil},
				},
				kbID:     "kb-a",
				expected: EmbedStats{Count: 2, ErrorCount: 0, LatencySumMS: 10, LatencyMaxMS: 6},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				m := NewInMemAppMetrics()
				require.NotNil(t, m)

				for _, call := range tc.calls {
					m.RecordEmbed(call.kbID, call.latencyMS, call.err)
				}

				snap := m.Snapshot()
				stats := snap.EmbedStats[tc.kbID]
				assert.EqualValues(t, tc.expected.Count, stats.Count)
				assert.EqualValues(t, tc.expected.ErrorCount, stats.ErrorCount)
				assert.EqualValues(t, tc.expected.LatencySumMS, stats.LatencySumMS)
				assert.EqualValues(t, tc.expected.LatencyMaxMS, stats.LatencyMaxMS)
			})
		}
	})

	t.Run("query", func(t *testing.T) {
		type queryCall struct {
			kbID        string
			latencyMS   int64
			resultCount int
			topDistance float32
			err         error
		}

		tests := []struct {
			name     string
			calls    []queryCall
			kbID     string
			expected QueryStats
		}{
			{
				name: "single_kb_with_error",
				calls: []queryCall{
					{kbID: "kb-a", latencyMS: 15, resultCount: 5, topDistance: 0.25, err: nil},
					{kbID: "kb-a", latencyMS: 16, resultCount: 0, topDistance: 0, err: assert.AnError},
				},
				kbID:     "kb-a",
				expected: QueryStats{Count: 2, ErrorCount: 1, LatencySumMS: 31, LatencyMaxMS: 16, TotalResults: 5, TopDistanceSum: 0.25},
			},
			{
				name: "isolates_per_kb_aggregate",
				calls: []queryCall{
					{kbID: "kb-a", latencyMS: 5, resultCount: 1, topDistance: 0.1, err: nil},
					{kbID: "kb-b", latencyMS: 8, resultCount: 2, topDistance: 0.2, err: nil},
					{kbID: "kb-a", latencyMS: 7, resultCount: 3, topDistance: 0.3, err: nil},
				},
				kbID:     "kb-a",
				expected: QueryStats{Count: 2, ErrorCount: 0, LatencySumMS: 12, LatencyMaxMS: 7, TotalResults: 4, TopDistanceSum: 0.4},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				m := NewInMemAppMetrics()
				require.NotNil(t, m)

				for _, call := range tc.calls {
					m.RecordQuery(call.kbID, call.latencyMS, call.resultCount, call.topDistance, call.err)
				}

				snap := m.Snapshot()
				stats := snap.QueryStats[tc.kbID]
				assert.EqualValues(t, tc.expected.Count, stats.Count)
				assert.EqualValues(t, tc.expected.ErrorCount, stats.ErrorCount)
				assert.EqualValues(t, tc.expected.LatencySumMS, stats.LatencySumMS)
				assert.EqualValues(t, tc.expected.LatencyMaxMS, stats.LatencyMaxMS)
				assert.EqualValues(t, tc.expected.TotalResults, stats.TotalResults)
				assert.InDelta(t, tc.expected.TopDistanceSum, stats.TopDistanceSum, 0.0001)
			})
		}
	})

	t.Run("ingest", func(t *testing.T) {
		type ingestCall struct {
			kbID       string
			latencyMS  int64
			docCount   int
			chunkCount int
			err        error
		}

		tests := []struct {
			name     string
			calls    []ingestCall
			kbID     string
			expected IngestStats
		}{
			{
				name: "single_kb_with_error",
				calls: []ingestCall{
					{kbID: "kb-a", latencyMS: 20, docCount: 2, chunkCount: 8, err: nil},
					{kbID: "kb-a", latencyMS: 30, docCount: 1, chunkCount: 3, err: assert.AnError},
				},
				kbID:     "kb-a",
				expected: IngestStats{Count: 2, ErrorCount: 1, LatencySumMS: 50, LatencyMaxMS: 30, TotalDocs: 3, TotalChunks: 11},
			},
			{
				name: "isolates_per_kb_aggregate",
				calls: []ingestCall{
					{kbID: "kb-a", latencyMS: 10, docCount: 1, chunkCount: 2, err: nil},
					{kbID: "kb-b", latencyMS: 12, docCount: 3, chunkCount: 6, err: nil},
					{kbID: "kb-a", latencyMS: 20, docCount: 2, chunkCount: 4, err: nil},
				},
				kbID:     "kb-a",
				expected: IngestStats{Count: 2, ErrorCount: 0, LatencySumMS: 30, LatencyMaxMS: 20, TotalDocs: 3, TotalChunks: 6},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				m := NewInMemAppMetrics()
				require.NotNil(t, m)

				for _, call := range tc.calls {
					m.RecordIngest(call.kbID, call.latencyMS, call.docCount, call.chunkCount, call.err)
				}

				snap := m.Snapshot()
				stats := snap.IngestStats[tc.kbID]
				assert.EqualValues(t, tc.expected.Count, stats.Count)
				assert.EqualValues(t, tc.expected.ErrorCount, stats.ErrorCount)
				assert.EqualValues(t, tc.expected.LatencySumMS, stats.LatencySumMS)
				assert.EqualValues(t, tc.expected.LatencyMaxMS, stats.LatencyMaxMS)
				assert.EqualValues(t, tc.expected.TotalDocs, stats.TotalDocs)
				assert.EqualValues(t, tc.expected.TotalChunks, stats.TotalChunks)
			})
		}
	})
}

func testAppMetricsSnapshotCopies(t *testing.T) {
	m := NewInMemAppMetrics()
	m.RecordRequest("GET", "/x", 200, 1)
	m.RecordEmbed("kb-a", 1, nil)
	m.RecordQuery("kb-a", 2, 1, 0.2, nil)
	m.RecordIngest("kb-a", 3, 1, 1, nil)

	snap := m.Snapshot()
	snap.RouteStats["GET /x"] = RouteStats{}
	snap.EmbedStats["kb-a"] = EmbedStats{}
	snap.QueryStats["kb-a"] = QueryStats{}
	snap.IngestStats["kb-a"] = IngestStats{}
	require.NotEmpty(t, snap.RecentRequests)
	snap.RecentRequests[0].Path = "/changed"

	snap2 := m.Snapshot()
	assert.EqualValues(t, 1, snap2.RouteStats["GET /x"].Count)
	assert.EqualValues(t, 1, snap2.EmbedStats["kb-a"].Count)
	assert.EqualValues(t, 1, snap2.QueryStats["kb-a"].Count)
	assert.EqualValues(t, 1, snap2.IngestStats["kb-a"].Count)
	assert.Equal(t, "/x", snap2.RecentRequests[0].Path)
}

func testAppMetricsConcurrentRecord(t *testing.T) {
	m := NewInMemAppMetrics()
	const workers = 20
	const loops = 50

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				path := fmt.Sprintf("/p/%d", id%3)
				m.RecordRequest("GET", path, 200, int64(j))
				m.RecordEmbed("kb-concurrent", int64(j), nil)
				m.RecordQuery("kb-concurrent", int64(j), 1, 0.1, nil)
				m.RecordIngest("kb-concurrent", int64(j), 1, 2, nil)
			}
		}(i)
	}
	wg.Wait()

	snap := m.Snapshot()
	reqTotal := int64(0)
	for _, s := range snap.RouteStats {
		reqTotal += s.Count
	}
	assert.EqualValues(t, workers*loops, reqTotal)
	assert.EqualValues(t, workers*loops, snap.EmbedStats["kb-concurrent"].Count)
	assert.EqualValues(t, workers*loops, snap.QueryStats["kb-concurrent"].Count)
	assert.EqualValues(t, workers*loops, snap.IngestStats["kb-concurrent"].Count)
}

func testAppMetricsRecentRingBufferWraps(t *testing.T) {
	m := NewInMemAppMetrics()
	for i := 0; i < appMetricsRecentCapacity+50; i++ {
		m.RecordRequest("GET", fmt.Sprintf("/item/%03d", i), 200, int64(i))
	}

	snap := m.Snapshot()
	require.Len(t, snap.RecentRequests, appMetricsRecentCapacity)
	assert.Equal(t, "/item/050", snap.RecentRequests[0].Path)
	assert.Equal(t, "/item/249", snap.RecentRequests[len(snap.RecentRequests)-1].Path)
}
