package cmd

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/kbcore/kb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestApp(t *testing.T, opts ...kb.KBOption) (string, *App) {
	t.Helper()
	tmp := t.TempDir()
	loader := kb.NewKB(
		&kb.LocalBlobStore{Root: filepath.Join(tmp, "blobs")},
		filepath.Join(tmp, "cache"),
		opts...,
	)
	app := NewApp(loader, AppConfig{Address: "127.0.0.1:0"})
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		_ = app.Stop(context.Background())
		_ = app.Wait()
	})
	require.NotEmpty(t, app.Address())
	return "http://" + app.Address(), app
}

func TestAppHTTP(t *testing.T) {
	t.Run("endpoints", testAppEndpoints)
	t.Run("ui_content_type", testAppUIContentType)
	t.Run("kb_id_middleware", testAppKBIDMiddleware)
	t.Run("background_cache_eviction", testAppBackgroundCacheEviction)
}

func testAppEndpoints(t *testing.T) {
	base, _ := newTestApp(t)

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{name: "healthz", method: http.MethodGet, path: "/healthz", status: http.StatusOK},
		{name: "ui_index", method: http.MethodGet, path: "/", status: http.StatusOK},
		{name: "metrics_cache", method: http.MethodGet, path: "/metrics/cache", status: http.StatusOK},
		{name: "metrics_app", method: http.MethodGet, path: "/metrics/app", status: http.StatusOK},
		{name: "cache_sweep", method: http.MethodPost, path: "/cache/sweep", status: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, base+tc.path, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.status, resp.StatusCode)
		})
	}
}

func testAppUIContentType(t *testing.T) {
	base, _ := newTestApp(t)

	req, err := http.NewRequest(http.MethodGet, base+"/", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "KBCore")
}

func testAppKBIDMiddleware(t *testing.T) {
	base, _ := newTestApp(t)

	tests := []struct {
		name            string
		sendHeader      string
		expectHeader    string
		expectHeaderSet bool
	}{
		{
			name:            "echoes_header_back",
			sendHeader:      "my-test-kb",
			expectHeader:    "my-test-kb",
			expectHeaderSet: true,
		},
		{
			name:            "no_header_no_response_header",
			sendHeader:      "",
			expectHeader:    "",
			expectHeaderSet: false,
		},
		{
			name:            "whitespace_only_treated_as_absent",
			sendHeader:      "   ",
			expectHeader:    "",
			expectHeaderSet: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, base+"/healthz", nil)
			require.NoError(t, err)

			if tc.sendHeader != "" {
				req.Header.Set("X-KB-ID", tc.sendHeader)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			got := resp.Header.Get("X-KB-ID")
			if tc.expectHeaderSet {
				assert.Equal(t, tc.expectHeader, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func testAppBackgroundCacheEviction(t *testing.T) {
	tmp := t.TempDir()
	cacheDir := filepath.Join(tmp, "cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))

	loader := kb.NewKB(
		&kb.LocalBlobStore{Root: filepath.Join(tmp, "blobs")},
		cacheDir,
		kb.WithCacheEntryTTL(50*time.Millisecond),
	)

	// seed a stale cache entry that should be evicted
	kbID := "kb-app-cache-ttl"
	kbCacheDir := filepath.Join(cacheDir, kbID)
	require.NoError(t, os.MkdirAll(kbCacheDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(kbCacheDir, "vectors.duckdb"), make([]byte, 256), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(kbCacheDir, "snapshot.json"), []byte(`{"version":"v1"}`), 0o644))

	old := time.Now().UTC().Add(-2 * time.Second)
	for _, name := range []string{"vectors.duckdb", "snapshot.json"} {
		require.NoError(t, os.Chtimes(filepath.Join(kbCacheDir, name), old, old))
	}
	require.NoError(t, os.Chtimes(kbCacheDir, old, old))

	app := NewApp(loader, AppConfig{
		Address:               "127.0.0.1:0",
		CacheEvictionInterval: 20 * time.Millisecond,
	})
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		_ = app.Stop(context.Background())
		_ = app.Wait()
	})

	require.Eventually(t,
		func() bool {
			_, err := os.Stat(kbCacheDir)
			return os.IsNotExist(err)
		},
		2*time.Second,
		20*time.Millisecond,
	)
}
