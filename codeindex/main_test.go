package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	indexer "github.com/mikills/minnow/codeindex/indexer"
)

func TestIndexCLIOptions(t *testing.T) {
	opts, err := parseIndexCLIOptions([]string{
		"--kb", "code", "--index-key", "api", "--root", ".",
		"--batch-size", "4", "--max-rss-bytes", "1024", "--throttle", "5ms", "-y",
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.kbID != "code" || opts.indexKey != "api" || opts.requestBatchSize != 4 || opts.maxRSSBytes != 1024 || !opts.yes {
		t.Fatalf("unexpected options: %+v", opts)
	}
	if opts.throttle != 5*time.Millisecond {
		t.Fatalf("unexpected throttle: %s", opts.throttle)
	}
	if _, err := parseIndexCLIOptions([]string{"unexpected"}); err == nil {
		t.Fatal("expected positional argument error")
	}
}

func TestSetupConfigRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	opts := setupCLIOptions{configPath: path, minnowURL: "http://127.0.0.1:9090", tokenEnv: "TEST_MINNOW_TOKEN"}
	t.Setenv("TEST_MINNOW_TOKEN", "secret")
	if err := writeConfig(path, setupConfig(opts), false); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Minnow.URL != opts.minnowURL || cfg.Minnow.Token != "secret" {
		t.Fatalf("unexpected config: %+v", cfg.Minnow)
	}
}

func TestIndexLockSerializesRefreshes(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{})
	release, err := acquireIndexLock(target, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireIndexLock(target, time.Hour); err == nil {
		t.Fatal("expected a concurrent refresh error")
	}
	release()
	releaseAgain, err := acquireIndexLock(target, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	releaseAgain()
}

func TestStaleIndexLockIsRecovered(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{})
	path := indexStatePath(target) + ".lock"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"pid":99999999,"token":"abandoned"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	release, err := acquireIndexLock(target, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestLongRunningLiveIndexLockIsNotDeclaredStale(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{})
	release, err := acquireIndexLock(target, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	time.Sleep(120 * time.Millisecond)
	if _, err := acquireIndexLock(target, 30*time.Millisecond); err == nil {
		t.Fatal("a live long-running lock was removed as stale")
	}
}

func TestRefreshUsesBranchSpecificIndexAndSkipsUnchangedFiles(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\nfunc Main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, root, "add", "main.go")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm
	opts := indexCLIOptions{root: root, yes: true}

	first, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.KBID == "" || first.ChunksIndexed == 0 || second.ChunksIndexed != 0 || second.UnchangedFiles != 1 {
		t.Fatalf("unexpected results: first=%+v second=%+v", first, second)
	}
	server.assertIngestCount(t, 1)
	cfg.CodeIndex.ChunkSize = 1000
	reconfigured, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if reconfigured.ChunksIndexed == 0 || reconfigured.UnchangedFiles != 0 {
		t.Fatalf("pipeline change did not force reindex: %+v", reconfigured)
	}
	server.assertIngestCount(t, 2)

	runTestGit(t, root, "checkout", "-b", "feature/client")
	third, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if third.KBID == first.KBID || third.IndexKey == first.IndexKey {
		t.Fatalf("branch did not select an isolated index: main=%+v feature=%+v", first, third)
	}
	server.assertIngestCount(t, 3)
}

func TestExplicitIdentityOverridesRemainBranchIsolated(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, root, "add", "main.go")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm
	opts := indexCLIOptions{root: root, indexKey: "api", kbID: "shared", yes: true}
	mainResult, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}

	runTestGit(t, root, "checkout", "-b", "feature")
	featureResult, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if mainResult.IndexKey == featureResult.IndexKey || mainResult.KBID == featureResult.KBID {
		t.Fatalf("explicit overrides collapsed branches: main=%+v feature=%+v", mainResult, featureResult)
	}
	if mainResult.StatePath == featureResult.StatePath {
		t.Fatalf("explicit branch refreshes shared state path %q", mainResult.StatePath)
	}
	server.assertIngestCount(t, 2)
}

func TestStatusReadsLocalStateWithoutConfig(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{})
	state := indexState{
		SchemaVersion: indexStateSchema, KBID: target.KBID, RepoID: target.RepoID,
		Root: target.Root, UpdatedAt: time.Now(), Files: map[string]stateFile{},
	}
	if _, err := saveIndexState(target, state); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(configPath, []byte("minnow: [not valid for this schema]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEINDEX_CONFIG", configPath)
	if code := runStatus([]string{"--root", root}); code != 0 {
		t.Fatalf("status returned %d with invalid connection config", code)
	}
}

type testMinnowServer struct {
	*httptest.Server
	mu      sync.Mutex
	ingests []ingestRequest
	polls   int
}

func newTestMinnowServer(t *testing.T) *testMinnowServer {
	t.Helper()
	server := &testMinnowServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/rag/ingest", func(w http.ResponseWriter, r *http.Request) {
		var request ingestRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !request.PreChunked {
			t.Error("expected pre_chunked request")
		}
		server.mu.Lock()
		server.ingests = append(server.ingests, request)
		server.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "evt-1"})
	})
	mux.HandleFunc("/rag/operations/evt-1", func(w http.ResponseWriter, _ *http.Request) {
		server.mu.Lock()
		server.polls++
		polls := server.polls
		server.mu.Unlock()
		if polls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"terminal": map[string]any{"kind": "kb.published", "status": "pending"},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{"kind": "kb.published", "status": "done"},
		})
	})
	mux.HandleFunc("/v1/vectors", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ids": []string{}})
	})
	server.Server = httptest.NewServer(mux)
	return server
}

func TestPollOperationRetriesRateLimits(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{"kind": "kb.published", "status": "done"},
		})
	}))
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	client, err := newMinnowClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.waitForOperation(context.Background(), "evt"); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("expected retry, got %d requests", requests)
	}
}

func TestMinnowMutationsRetryTransientFailures(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.Method+" "+r.URL.Path]++
		count := counts[r.Method+" "+r.URL.Path]
		mu.Unlock()
		if count == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/rag/ingest":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "evt"})
		case "/rag/operations/evt":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"terminal": map[string]any{"kind": "kb.published", "status": "done"},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		}
	}))
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	client, err := newMinnowClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ingest(context.Background(), "kb", []indexer.Document{{ID: "id", Text: "text"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.delete(context.Background(), "kb", []string{"id"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if counts["POST /rag/ingest"] != 2 || counts["DELETE /v1/vectors"] != 2 {
		t.Fatalf("expected retries, got %+v", counts)
	}
}

func TestRetryableWorkerFailureContinuesPolling(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"terminal": map[string]any{"kind": "worker.failed", "status": "done", "will_retry": true},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{"kind": "kb.published", "status": "done"},
		})
	}))
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	client, err := newMinnowClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.waitForOperation(context.Background(), "evt"); err != nil {
		t.Fatal(err)
	}
}

func (s *testMinnowServer) assertIngestCount(t *testing.T, expected int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ingests) != expected {
		t.Fatalf("expected %d ingests, got %d", expected, len(s.ingests))
	}
}

func runTestGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
