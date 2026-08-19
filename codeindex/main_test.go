package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

func TestRefreshCoordinatesWithLegacyIndexLock(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{indexKey: "api/v1"})
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	releaseLegacy, err := acquireIndexLock(legacyTarget, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseLegacy()
	if _, err := acquireRefreshLocks(target, time.Hour); err == nil {
		t.Fatal("refresh did not coordinate with a legacy index lock")
	}
	releaseCurrent, err := acquireIndexLock(target, time.Hour)
	if err != nil {
		t.Fatalf("failed refresh left current lock held: %v", err)
	}
	releaseCurrent()
}

func TestMalformedStateIsNotTreatedAsExisting(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{})
	path := indexStatePath(target)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := loadIndexState(target); err == nil {
		t.Fatal("semantically empty state was accepted")
	}
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
	if err := os.WriteFile(filepath.Join(root, "helper.go"), []byte("package main\nfunc Helper() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, root, "add", "main.go", "helper.go")
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
	if first.KBID == "" || first.ChunksIndexed == 0 || second.ChunksIndexed != 0 || second.UnchangedFiles != 2 {
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

	runTestGit(t, root, "checkout", "main")
	returned, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if returned.KBID != first.KBID || returned.ChunksIndexed != 0 || returned.UnchangedFiles != 2 {
		t.Fatalf("returning to main did not reuse incremental state: first=%+v returned=%+v", first, returned)
	}
	server.assertIngestCount(t, 3)

	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\nfunc Main() { println(\"changed\") }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if changed.IndexedFiles != 1 || changed.UnchangedFiles != 1 || changed.ChunksDeleted == 0 {
		t.Fatalf("changed-file refresh was not incremental: %+v", changed)
	}
	server.assertLastMutationOrder(t, "ingest", "publish", "delete")

	if err := os.WriteFile(filepath.Join(root, "new.go"), []byte("package main\nfunc New() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, root, "add", "new.go")
	withNewFile, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if withNewFile.IndexedFiles != 1 || withNewFile.UnchangedFiles != 2 || withNewFile.ChunksDeleted != 0 {
		t.Fatalf("new-file refresh was not incremental: %+v", withNewFile)
	}
	server.assertLastMutationOrder(t, "ingest", "publish")

	if err := os.Remove(filepath.Join(root, "helper.go")); err != nil {
		t.Fatal(err)
	}
	withDeletedFile, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if withDeletedFile.DeletedFiles != 1 || withDeletedFile.UnchangedFiles != 2 || withDeletedFile.ChunksDeleted == 0 {
		t.Fatalf("deleted-file refresh did not remove stale chunks: %+v", withDeletedFile)
	}
	server.assertLastMutationOrder(t, "delete")
}

func TestPipelineVersionForcesReindex(t *testing.T) {
	opts := indexer.NormalizeOptions(indexer.Options{})
	if pipelineFingerprintForVersion(opts, "pipeline/v1") == pipelineFingerprintForVersion(opts, "pipeline/v2") {
		t.Fatal("pipeline implementation version did not affect fingerprint")
	}
}

func TestMissingStateStartsNewKBGeneration(t *testing.T) {
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
	opts := indexCLIOptions{root: root, yes: true}
	first, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(first.StatePath); err != nil {
		t.Fatal(err)
	}
	second, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.KBID == first.KBID || second.ChunksIndexed == 0 {
		t.Fatalf("missing state reused stale remote KB: first=%+v second=%+v", first, second)
	}
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

func TestLegacyExplicitKBStateMigratesInPlace(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root, kbID: "shared", indexKey: "api/v1"})
	if err != nil {
		t.Fatal(err)
	}
	legacy := indexState{
		SchemaVersion: indexStateSchema, KBID: target.LegacyKBID, RepoID: target.RepoID,
		Ref: target.Ref, Root: target.Root, Files: map[string]stateFile{},
	}
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	if _, err := saveIndexState(legacyTarget, legacy); err != nil {
		t.Fatal(err)
	}
	loaded, loadedPath, exists, err := loadIndexState(target)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || loaded.KBID != target.LegacyKBID || loadedPath != indexStatePath(legacyTarget) {
		t.Fatalf("legacy explicit identity state was not reused: target=%+v state=%+v path=%s", target, loaded, loadedPath)
	}
	migrated, err := assignIndexGeneration(target, loaded, exists)
	if err != nil {
		t.Fatal(err)
	}
	if migrated.KBID != target.LegacyKBID {
		t.Fatalf("legacy KB was unexpectedly rotated: %+v", migrated)
	}
}

func TestLegacyExplicitIndexKeyStateMigratesInPlace(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root, indexKey: "api/v1"})
	if err != nil {
		t.Fatal(err)
	}
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	legacy := indexState{
		SchemaVersion: indexStateSchema, KBID: target.LegacyKBID, RepoID: target.RepoID,
		Ref: target.Ref, Root: target.Root, Files: map[string]stateFile{},
	}
	legacyPath, err := saveIndexState(legacyTarget, legacy)
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, exists, err := loadIndexState(target)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || loaded.KBID != target.LegacyKBID {
		t.Fatalf("legacy explicit index-key state was not reused: target=%+v state=%+v", target, loaded)
	}
	plan, err := buildIndexPlan(
		context.Background(), target, indexer.NormalizeOptions(indexer.Options{}),
		pipelineFingerprint(indexer.NormalizeOptions(indexer.Options{})), loaded, nil, 0,
		func(context.Context, []indexer.Document) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	newPath, err := saveIndexState(target, plan.state)
	if err != nil {
		t.Fatal(err)
	}
	if newPath == legacyPath {
		t.Fatal("legacy state did not migrate to the new identity path")
	}
	if _, err := os.Stat(legacyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy state remained after migration: %v", err)
	}
}

func TestLegacyDirectoryKBStateMigratesInPlace(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{kbID: "shared", indexKey: "api/v1"})
	legacy := indexState{
		SchemaVersion: indexStateSchema, KBID: target.LegacyKBID, RepoID: target.RepoID,
		Root: target.Root, Files: map[string]stateFile{},
	}
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	if _, err := saveIndexState(legacyTarget, legacy); err != nil {
		t.Fatal(err)
	}
	loaded, loadedPath, exists, err := loadIndexState(target)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || loaded.KBID != "shared" || loadedPath != indexStatePath(legacyTarget) {
		t.Fatalf("legacy directory identity state was not reused: target=%+v state=%+v path=%s", target, loaded, loadedPath)
	}
}

func TestLegacyDirectoryIndexKeyStateMigratesInPlace(t *testing.T) {
	root := t.TempDir()
	target := directoryTarget(root, indexCLIOptions{indexKey: "api/v1"})
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	legacy := indexState{
		SchemaVersion: indexStateSchema, KBID: target.LegacyKBID, RepoID: target.RepoID,
		Root: target.Root, Files: map[string]stateFile{},
	}
	if _, err := saveIndexState(legacyTarget, legacy); err != nil {
		t.Fatal(err)
	}
	loaded, _, exists, err := loadIndexState(target)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || loaded.KBID != target.LegacyKBID {
		t.Fatalf("legacy directory index-key state was not reused: target=%+v state=%+v", target, loaded)
	}
}

func TestExplicitIdentitiesDoNotCollideAcrossRepositories(t *testing.T) {
	roots := []string{t.TempDir(), t.TempDir()}
	targets := make([]indexTarget, 0, len(roots))
	for _, root := range roots {
		runTestGit(t, root, "init", "-b", "main")
		target, err := resolveTarget(indexCLIOptions{root: root, indexKey: "api/v1", kbID: "shared"})
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
	}
	if targets[0].KBID == targets[1].KBID {
		t.Fatalf("explicit KB collided across repositories: %+v", targets)
	}
	first, err := resolveTarget(indexCLIOptions{root: roots[0], indexKey: "api/v1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolveTarget(indexCLIOptions{root: roots[0], indexKey: "api-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if first.IndexKey == second.IndexKey {
		t.Fatalf("sanitized explicit keys collided: %q", first.IndexKey)
	}
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
	mu        sync.Mutex
	ingests   []ingestRequest
	deletes   [][]string
	mutations []string
	polls     int
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
		server.mutations = append(server.mutations, "ingest")
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
		server.mu.Lock()
		server.mutations = append(server.mutations, "publish")
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{"kind": "kb.published", "status": "done"},
		})
	})
	mux.HandleFunc("/v1/vectors", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		server.deletes = append(server.deletes, append([]string(nil), request.IDs...))
		server.mutations = append(server.mutations, "delete")
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ids": request.IDs})
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

func (s *testMinnowServer) assertLastMutationOrder(t *testing.T, expected ...string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mutations) < len(expected) {
		t.Fatalf("expected mutation suffix %v, got %v", expected, s.mutations)
	}
	actual := s.mutations[len(s.mutations)-len(expected):]
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected mutation suffix %v, got %v", expected, actual)
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
