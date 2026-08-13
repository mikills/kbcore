package cmd

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppStopClosesStatefulMCPSessions(t *testing.T) {
	loader := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir())
	app := NewApp(loader, AppConfig{
		Address: "127.0.0.1:0",
		MCP: mcpserver.Config{
			Enabled:      true,
			HTTPEnabled:  true,
			HTTPPath:     "/mcp",
			HTTPStateful: true,
		},
	})
	require.NoError(t, app.Start())
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint: "http://" + app.Address() + "/mcp",
	}, nil)
	require.NoError(t, err)
	require.Len(t, app.mcpHandlers, 1)
	require.Equal(t, 1, app.mcpHandlers[0].ActiveSessions())

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
	require.Zero(t, app.mcpHandlers[0].ActiveSessions())
	_ = session.Close()
}

func TestRemoteClientIPIgnoresUntrustedForwardingHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.test", nil)
	require.NoError(t, err)
	req.RemoteAddr = "192.0.2.10:4321"
	req.Header.Set("X-Forwarded-For", strings.Repeat("a", 10000))
	req.Header.Set("X-Real-IP", "203.0.113.99")
	require.Equal(t, "192.0.2.10", remoteClientIP(req))

	req.RemoteAddr = "127.0.0.1:4321"
	req.Header.Set("X-Real-IP", "203.0.113.99")
	require.Equal(t, "203.0.113.99", remoteClientIP(req))
}

func TestIPRateLimiterKeepsCollidingIPsIndependent(t *testing.T) {
	first, second := "198.51.100.18", "198.51.100.102"
	require.Equal(t, rateLimitStripeFor(first), rateLimitStripeFor(second))
	limiter := newIPRateLimiter(0.000001, 1)
	require.True(t, limiter.Allow(first))
	require.False(t, limiter.Allow(first))
	require.True(t, limiter.Allow(second))
}

func TestAppFileIngestRequiresMediaStore(t *testing.T) {
	loader := kb.NewKB(
		&kb.LocalBlobStore{Root: t.TempDir()},
		t.TempDir(),
		kb.WithEventStore(kb.NewInMemoryEventStore()),
	)
	require.Nil(t, appFileIngestFn(loader))

	loader.MediaStore = kb.NewInMemoryMediaStore()
	require.NotNil(t, appFileIngestFn(loader))
}

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
	t.Run("mcp http mount", testAppMCPHTTPMount)
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
		t.Run(tc.name, func(t *testing.T) { assertAppEndpoint(t, base, tc.method, tc.path, tc.status) })
	}
}

func assertAppEndpoint(t *testing.T, base, method, path string, status int) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, status, resp.StatusCode)
}

func testAppMCPHTTPMount(t *testing.T) {
	tmp := t.TempDir()
	loader := kb.NewKB(&kb.LocalBlobStore{Root: filepath.Join(tmp, "blobs")}, filepath.Join(tmp, "cache"))
	app := NewApp(
		loader,
		AppConfig{
			Address: "127.0.0.1:0",
			MCP:     mcpserver.Config{Enabled: true, HTTPEnabled: true, HTTPPath: "/mcp", HTTPJSONResponse: true},
		},
	)
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		_ = app.Stop(context.Background())
		_ = app.Wait()
	})

	resp, err := http.Get("http://" + app.Address() + "/mcp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.NotEqual(t, http.StatusNotFound, resp.StatusCode)
}

func testAppUIContentType(t *testing.T) {
	base, _ := newTestApp(t)

	req, err := http.NewRequest(http.MethodGet, base+"/", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Minnow")
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
			assertKBIDMiddlewareCase(t, base, tc.sendHeader, tc.expectHeaderSet, tc.expectHeader)
		})
	}
}

func assertKBIDMiddlewareCase(t *testing.T, base, sendHeader string, expectHeaderSet bool, expectHeader string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/healthz", nil)
	require.NoError(t, err)
	if sendHeader != "" {
		req.Header.Set("X-KB-ID", sendHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	got := resp.Header.Get("X-KB-ID")
	if expectHeaderSet {
		assert.Equal(t, expectHeader, got)
	} else {
		assert.Empty(t, got)
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
