package kb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheEviction(t *testing.T) {
	t.Run("size_budget", testCacheSizeBudget)
	t.Run("load_budget_exceeded", testCacheLoadBudgetExceeded)
	t.Run("ttl_evicts_expired", testCacheTTLEvictsExpired)
	t.Run("ttl_keeps_protected", testCacheTTLKeepsProtected)
	t.Run("ttl_composes_with_size_budget", testCacheTTLComposesSizeBudget)
}

func TestCacheQueryBudget(t *testing.T) {
	ctx := context.Background()
	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, "kb-vector-budget").WithEmbedder(embedder).Setup()
	defer harness.Cleanup()

	require.NoError(t, harness.KB().UpsertDocsAndUpload(ctx, "kb-vector-budget", []Document{
		{ID: "doc-1", Text: "hello world"},
		{ID: "doc-2", Text: "goodbye world"},
	}))
	queryVec, err := harness.KB().Embed(ctx, "hello world")
	require.NoError(t, err)

	kb := harness.KB()
	kb.SetMaxCacheBytes(1)

	_, err = kb.TopK(ctx, "kb-vector-budget", queryVec, 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCacheBudgetExceeded)

	kb.SetMaxCacheBytes(5 * 1024 * 1024)
	results, err := kb.TopK(ctx, "kb-vector-budget", queryVec, 2)
	require.NoError(t, err)
	require.NotEmpty(t, results)
}

func TestCacheMetrics(t *testing.T) {
	t.Run("eviction_and_budget_metrics", testCacheMetricsEvictionAndBudget)
	t.Run("openmetrics_handler", testCacheMetricsHandler)
}

func testCacheSizeBudget(t *testing.T) {
	tests := []struct {
		name            string
		maxCacheBytes   int64
		protectKBID     string
		expectedError   error
		expectedPresent []string
		expectedMissing []string
	}{
		{
			name:          "evicts_oldest_first",
			maxCacheBytes: 180,
			protectKBID:   "kb-a",
			expectedPresent: []string{
				"kb-a",
			},
			expectedMissing: []string{"kb-b", "kb-c"},
		},
		{
			name:          "returns_budget_error_when_protected_entry_exceeds_limit",
			maxCacheBytes: 50,
			protectKBID:   "kb-a",
			expectedError: ErrCacheBudgetExceeded,
			expectedPresent: []string{
				"kb-a",
			},
			expectedMissing: []string{"kb-b"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cacheDir := t.TempDir()
			kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, cacheDir, WithMaxCacheBytes(tc.maxCacheBytes))

			now := time.Now().UTC()
			require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-a", 80, 20, now.Add(-3*time.Hour)))
			require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-b", 80, 20, now.Add(-2*time.Hour)))
			if tc.name == "evicts_oldest_first" {
				require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-c", 80, 20, now.Add(-1*time.Hour)))
			}

			err := kb.evictCacheIfNeeded(context.Background(), tc.protectKBID)
			if tc.expectedError != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			for _, kbID := range tc.expectedPresent {
				_, statErr := os.Stat(filepath.Join(cacheDir, kbID))
				require.NoError(t, statErr)
			}
			for _, kbID := range tc.expectedMissing {
				_, statErr := os.Stat(filepath.Join(cacheDir, kbID))
				assert.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}

func testCacheLoadBudgetExceeded(t *testing.T) {
	ctx := context.Background()
	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, "kb-over-budget").WithEmbedder(embedder).Setup()
	defer harness.Cleanup()

	require.NoError(t, harness.KB().UpsertDocsAndUpload(ctx, "kb-over-budget", []Document{
		{ID: "doc-1", Text: "hello world"},
		{ID: "doc-2", Text: "goodbye world"},
	}))

	kb := NewKB(&LocalBlobStore{Root: harness.BlobRoot()}, harness.CacheDir(), WithMaxCacheBytes(100))
	_, err := kb.Load(ctx, "kb-over-budget")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCacheBudgetExceeded)
}

func testCacheTTLEvictsExpired(t *testing.T) {
	cacheDir := t.TempDir()
	kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, cacheDir, WithCacheEntryTTL(30*time.Minute))

	now := time.Now().UTC()
	require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-expired", 80, 20, now.Add(-2*time.Hour)))
	require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-recent", 80, 20, now.Add(-5*time.Minute)))

	require.NoError(t, kb.evictCacheIfNeeded(context.Background(), ""))

	_, expiredErr := os.Stat(filepath.Join(cacheDir, "kb-expired"))
	assert.ErrorIs(t, expiredErr, os.ErrNotExist)
	_, recentErr := os.Stat(filepath.Join(cacheDir, "kb-recent"))
	require.NoError(t, recentErr)
}

func testCacheTTLKeepsProtected(t *testing.T) {
	cacheDir := t.TempDir()
	kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, cacheDir, WithCacheEntryTTL(1*time.Minute))

	require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-protected", 80, 20, time.Now().UTC().Add(-2*time.Hour)))
	require.NoError(t, kb.evictCacheIfNeeded(context.Background(), "kb-protected"))

	_, err := os.Stat(filepath.Join(cacheDir, "kb-protected"))
	require.NoError(t, err)
}

func testCacheTTLComposesSizeBudget(t *testing.T) {
	cacheDir := t.TempDir()
	kb := NewKB(
		&LocalBlobStore{Root: t.TempDir()},
		cacheDir,
		WithCacheEntryTTL(30*time.Minute),
		WithMaxCacheBytes(150),
	)

	now := time.Now().UTC()
	require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-expired", 80, 20, now.Add(-3*time.Hour)))
	require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-older", 80, 20, now.Add(-20*time.Minute)))
	require.NoError(t, writeCacheSnapshotForTest(cacheDir, "kb-newer", 80, 20, now.Add(-5*time.Minute)))

	require.NoError(t, kb.evictCacheIfNeeded(context.Background(), ""))

	_, expiredErr := os.Stat(filepath.Join(cacheDir, "kb-expired"))
	assert.ErrorIs(t, expiredErr, os.ErrNotExist)
	_, olderErr := os.Stat(filepath.Join(cacheDir, "kb-older"))
	assert.ErrorIs(t, olderErr, os.ErrNotExist)
	_, newerErr := os.Stat(filepath.Join(cacheDir, "kb-newer"))
	require.NoError(t, newerErr)
}

func writeCacheSnapshotForTest(cacheDir, kbID string, dbBytes, metaBytes int, ts time.Time) error {
	kbPath := filepath.Join(cacheDir, kbID)
	if err := os.MkdirAll(kbPath, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(kbPath, "vectors.duckdb")
	metaPath := filepath.Join(kbPath, "snapshot.json")
	if err := os.WriteFile(dbPath, make([]byte, dbBytes), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(metaPath, make([]byte, metaBytes), 0o644); err != nil {
		return err
	}
	if err := os.Chtimes(dbPath, ts, ts); err != nil {
		return err
	}
	if err := os.Chtimes(metaPath, ts, ts); err != nil {
		return err
	}
	return os.Chtimes(kbPath, ts, ts)
}

func testCacheMetricsEvictionAndBudget(t *testing.T) {
	harness := NewTestHarness(t, "kb-metrics").Setup()
	defer harness.Cleanup()
	kb := NewKB(
		&LocalBlobStore{Root: harness.BlobRoot()},
		harness.CacheDir(),
		WithCacheEntryTTL(30*time.Minute),
		WithMaxCacheBytes(150),
	)

	now := time.Now().UTC()
	require.NoError(t, writeCacheSnapshotForTest(harness.CacheDir(), "kb-expired", 80, 20, now.Add(-3*time.Hour)))
	require.NoError(t, writeCacheSnapshotForTest(harness.CacheDir(), "kb-older", 80, 20, now.Add(-20*time.Minute)))
	require.NoError(t, writeCacheSnapshotForTest(harness.CacheDir(), "kb-newer", 80, 20, now.Add(-5*time.Minute)))

	require.NoError(t, kb.evictCacheIfNeeded(context.Background(), ""))

	m := kb.CacheEvictionMetricsSnapshot()
	assert.Equal(t, uint64(1), m.CacheEvictionsTTLTotal)
	assert.Equal(t, uint64(1), m.CacheEvictionsSizeTotal)
	assert.Equal(t, uint64(0), m.CacheEvictionErrorsTotal)
	assert.Equal(t, uint64(0), m.CacheBudgetExceededTotal)
	assert.GreaterOrEqual(t, m.CacheBytesCurrent, int64(90))

	require.NoError(t, writeCacheSnapshotForTest(harness.CacheDir(), "kb-protected", 160, 40, now.Add(-1*time.Hour)))
	err := kb.evictCacheIfNeeded(context.Background(), "kb-protected")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCacheBudgetExceeded)

	m = kb.CacheEvictionMetricsSnapshot()
	assert.GreaterOrEqual(t, m.CacheBudgetExceededTotal, uint64(1))

	metricsText := kb.CacheEvictionOpenMetricsText()
	assert.Contains(t, metricsText, "kbcore_cache_evictions_total{reason=\"ttl\"}")
	assert.Contains(t, metricsText, "kbcore_cache_evictions_total{reason=\"size\"}")
	assert.Contains(t, metricsText, "kbcore_cache_budget_exceeded_total")
}

func testCacheMetricsHandler(t *testing.T) {
	harness := NewTestHarness(t, "kb-metrics-http").Setup()
	defer harness.Cleanup()
	kb := NewKB(&LocalBlobStore{Root: harness.BlobRoot()}, harness.CacheDir(), WithMaxCacheBytes(1))
	require.NoError(t, writeCacheSnapshotForTest(harness.CacheDir(), "kb-a", 10, 10, time.Now().UTC().Add(-1*time.Hour)))
	_ = kb.evictCacheIfNeeded(context.Background(), "")

	h := NewCacheOpenMetricsHandler(kb)
	reqCtx := context.Background()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "/metrics/cache", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Header().Get("Content-Type"), "application/openmetrics-text")
	assert.Contains(t, rr.Body.String(), "kbcore_cache_bytes_current")
}
