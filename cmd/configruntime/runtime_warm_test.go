package configruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/config"
	"github.com/stretchr/testify/require"
)

// TestWarmCacheLifecycle checks Stop cancels and drains the pre-warm goroutine
// without deadlocking.
func TestWarmCacheLifecycle(t *testing.T) {
	blobRoot := filepath.Join(t.TempDir(), "blobs")

	cfg := func(cacheDir string, warm int) string {
		return "" +
			"http:\n" +
			"  address: 127.0.0.1:0\n" +
			"  shutdown_timeout: 2s\n" +
			"embedder:\n" +
			"  provider: local\n" +
			"  local:\n" +
			"    dim: 16\n" +
			"scheduler:\n" +
			"  enabled: false\n" +
			"media:\n" +
			"  enabled: false\n" +
			"storage:\n" +
			"  blob:\n" +
			"    kind: local\n" +
			"    root: " + blobRoot + "\n" +
			"  cache:\n" +
			"    dir: " + cacheDir + "\n" +
			"    warm_shards: " + strconv.Itoa(warm) + "\n"
	}

	ingestDir := filepath.Join(t.TempDir(), "cache")
	rtA, baseURL := buildAndStart(t, cfg(ingestDir, 0))
	requirePublishedShard(t, baseURL, "warm-kb")
	stopWithin(t, rtA, 5*time.Second)

	t.Run("drains after warm completes", func(t *testing.T) {
		cacheDir := filepath.Join(t.TempDir(), "cache")
		rt, _ := buildAndStart(t, cfg(cacheDir, 5))
		require.Eventually(t, func() bool { return dirHasFile(cacheDir) }, 10*time.Second, 25*time.Millisecond,
			"warm should populate the cache from the shared blob store")
		stopWithin(t, rt, 5*time.Second)
		stopWithin(t, rt, 5*time.Second) // Stop is documented idempotent
	})

	t.Run("drains when stopped mid-warm", func(t *testing.T) {
		cacheDir := filepath.Join(t.TempDir(), "cache")
		rt, _ := buildAndStart(t, cfg(cacheDir, 5))
		stopWithin(t, rt, 5*time.Second)
		stopWithin(t, rt, 5*time.Second)
	})
}

func buildAndStart(t *testing.T, yamlBody string) (*Runtime, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "minnow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlBody), 0o644))
	cfg, err := config.Load(path)
	require.NoError(t, err)
	rt, err := Build(context.Background(), cfg, BuildOptions{Logger: quietLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Stop(context.Background()) })
	require.NoError(t, rt.Start(context.Background()))
	return rt, "http://" + rt.App().Address()
}

func stopWithin(t *testing.T, rt *Runtime, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Stop(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(d + time.Second):
		t.Fatal("Stop did not return — warm drain likely deadlocked")
	}
}

func requirePublishedShard(t *testing.T, baseURL, kbID string) {
	t.Helper()
	body := map[string]any{
		"kb_id":         kbID,
		"graph_enabled": false,
		"documents":     []map[string]string{{"id": "doc-1", "text": "hello from warm test"}},
		"chunk_size":    100,
	}
	resp, err := postJSON(baseURL+"/rag/ingest", body)
	require.NoError(t, err)
	var accepted struct {
		EventID string `json:"event_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&accepted))
	resp.Body.Close()
	require.NotEmpty(t, accepted.EventID)

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		opResp, err := http.Get(baseURL + "/rag/operations/" + accepted.EventID)
		if err == nil && opResp.StatusCode == http.StatusOK {
			var payload map[string]any
			_ = json.NewDecoder(opResp.Body).Decode(&payload)
			opResp.Body.Close()
			if terminal, ok := payload["terminal"].(map[string]any); ok {
				if kind, _ := terminal["kind"].(string); kind == "kb.published" {
					return
				}
				require.NotEqual(t, "worker.failed", terminal["kind"], "ingest worker failed")
			}
		} else if opResp != nil {
			opResp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("ingest did not reach kb.published within deadline")
}

// dirHasFile ignores the server's own control files, which exist before any
// warming happens and would make every caller trivially true.
func dirHasFile(dir string) bool {
	var found bool
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() || info.Size() == 0 {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), ".") {
			return nil
		}
		for _, part := range strings.Split(filepath.Dir(path), string(filepath.Separator)) {
			if strings.HasPrefix(part, ".") {
				return nil
			}
		}
		found = true
		return nil
	})
	return found
}
