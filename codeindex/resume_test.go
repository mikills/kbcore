package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	indexer "github.com/mikills/minnow/kb/codeindex"
	"github.com/stretchr/testify/require"
)

func TestLegacyResume(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.published["done"] = struct{}{}
	server.mu.Unlock()

	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(path, []byte("kb legacy\ni done\ni missing\n"), 0o600))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	target := indexTarget{KBID: "legacy", ScopeID: "branch"}
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, target, path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.Contains(t, confirmed, "done")
	require.NotContains(t, confirmed, "missing")
	require.NoError(t, journal.close())
}

func TestActiveLegacyResume(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.session = "instance-1:legacy"
	server.ingests = append(server.ingests, ingestRequest{Documents: []ingestDocument{{ID: "done"}}})
	server.mu.Unlock()

	dir := t.TempDir()
	target := indexTarget{KBID: "legacy", ScopeID: "branch", MigrationDir: dir, MigrationIDs: []string{"done"}}
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, sessionFileName(target.KBID)), []byte("instance-1:legacy"), 0o600,
	))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, target, filepath.Join(t.TempDir(), "run.journal"), newProgressReporter(true),
	)
	require.NoError(t, err)
	require.Contains(t, confirmed, "done")
	require.NoError(t, journal.close())
}

func TestLostSession(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(
		path, []byte("kb legacy\nscope branch\ni missing\nc missing\n"), 0o600,
	))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "legacy", ScopeID: "branch"},
		path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.NotContains(t, confirmed, "missing")
	require.NoError(t, journal.close())
}

func TestPublishedResume(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(
		path, []byte("kb legacy\nscope branch\ni missing\nc missing\npublished\n"), 0o600,
	))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "legacy", ScopeID: "branch"},
		path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.NotContains(t, confirmed, "missing")
	require.NoError(t, journal.close())
}

func TestPublishCrash(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	server.scopeStatus = http.StatusInternalServerError
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm

	_, err := refreshIndex(context.Background(), cfg, indexCLIOptions{root: root, yes: true})
	require.Error(t, err)
	server.mu.Lock()
	require.NotEmpty(t, server.published)
	require.True(t, server.ingests[0].GCUnscoped)
	require.NotEmpty(t, server.gc)
	scheduled := make(map[string]struct{})
	for _, ids := range server.gc {
		for _, id := range ids {
			scheduled[id] = struct{}{}
		}
	}
	for id := range server.published {
		require.Contains(t, scheduled, id)
	}
	server.mu.Unlock()

	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	checkpoint, exists, err := loadRunCheckpoint(target)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, runPhaseFinalizing, checkpoint.Phase)

	server.mu.Lock()
	server.scopeStatus = 0
	server.session = ""
	server.mu.Unlock()
	result, err := refreshIndex(context.Background(), cfg, indexCLIOptions{root: root, yes: true})
	require.NoError(t, err)
	require.NotEmpty(t, result.StatePath)

	server.mu.Lock()
	require.Len(t, server.ingests, 1)
	require.Contains(t, server.scopes, result.ScopeID)
	require.Len(t, server.commits, 3)
	require.Empty(t, server.commits[1])
	require.Empty(t, server.commits[2])
	server.mu.Unlock()
	_, exists, err = loadRunCheckpoint(target)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRecoveredSession(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	state := emptyIndexState(target)
	state.Files["main.go"] = stateFile{ChunkIDs: []string{"wanted"}}
	require.NoError(t, saveRunCheckpoint(target, runCheckpoint{
		Phase: runPhaseFinalizing, State: state,
	}))
	handle := "instance-1:token-1"
	saveSession(target, handle)

	server := newTestMinnowServer(t)
	defer server.Close()
	server.session = handle
	server.scopes[target.ScopeID] = []string{"wanted"}
	server.published["wanted"] = struct{}{}
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"

	_, recovered, err := resumeRunCheckpoint(
		context.Background(), cfg, target, indexer.Options{}, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.True(t, recovered)
	server.mu.Lock()
	require.Equal(t, []string{handle}, server.commits)
	server.mu.Unlock()
	require.Empty(t, loadSession(target))
}

func TestMissingPublishedData(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	server.scopeStatus = http.StatusInternalServerError
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm

	_, err := refreshIndex(context.Background(), cfg, indexCLIOptions{root: root, yes: true})
	require.Error(t, err)
	server.mu.Lock()
	require.NotEmpty(t, server.published)
	var missing string
	for id := range server.published {
		missing = id
		delete(server.published, id)
		break
	}
	initialIngests := len(server.ingests)
	server.scopeStatus = 0
	server.session = ""
	server.mu.Unlock()

	_, err = refreshIndex(context.Background(), cfg, indexCLIOptions{root: root, yes: true})
	require.NoError(t, err)
	server.mu.Lock()
	defer server.mu.Unlock()
	require.Greater(t, len(server.ingests), initialIngests)
	var uploaded []string
	for _, ingest := range server.ingests[initialIngests:] {
		for _, document := range ingest.Documents {
			uploaded = append(uploaded, document.ID)
		}
	}
	require.Equal(t, []string{missing}, uploaded)
}

func TestFinalizeCAS(t *testing.T) {
	var mu sync.Mutex
	commits := 0
	var revisions []string
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"capabilities": []string{"ingest_sessions", "document_scopes", "session_commit_scope"},
		})
	})
	mux.HandleFunc("/v1/scopes/documents", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"document_ids": []string{"other"}})
	})
	mux.HandleFunc("/rag/commit", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Scope struct {
				Revision string `json:"revision"`
			} `json:"scope"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		mu.Lock()
		commits++
		revisions = append(revisions, request.Scope.Revision)
		mu.Unlock()
		if request.Scope.Revision == "rev-b" {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "overwritten"})
			return
		}
		http.Error(w, "session lapsed", http.StatusConflict)
	})
	mux.HandleFunc("/rag/operations/overwritten", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{"kind": "kb.published", "status": "done"},
		})
	})
	mux.HandleFunc("/v1/vectors/fetch", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"records": []map[string]any{{"id": "wanted"}}})
	})
	mux.HandleFunc("/v1/scopes/main", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"scope_id": "main", "document_ids": []string{"other"}, "revision": "rev-b",
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "run.journal")
	journal, _, err := resumeUploadJournal(path, "kb", "main")
	require.NoError(t, err)
	require.NoError(t, journal.recordScope("rev-a", true))
	require.NoError(t, journal.close())
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	client, resumed, _, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "kb", ScopeID: "main"}, path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.NoError(t, resumed.close())
	require.Equal(t, "rev-a", client.scopeRevision)
	client.sessionID = "instance:stale"
	client.conflictBudget = 0
	client.scopeRevision = "rev-b"
	client.scopeIDs = []string{"other"}

	err = finalizeRun(context.Background(), client, "kb", "main", []string{"wanted"}, "rev-a", true)
	require.Error(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, commits)
	require.Equal(t, []string{"rev-a"}, revisions)
}

func TestScopeAttempt(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	mux := http.NewServeMux()
	mux.HandleFunc("/rag/commit", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "dead"})
	})
	mux.HandleFunc("/rag/operations/dead", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{
				"kind": "worker.failed", "status": "failed", "last_error": "scope unavailable",
			},
		})
	})
	mux.HandleFunc("/v1/vectors/fetch", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"records": []map[string]any{{"id": "wanted"}}})
	})
	mux.HandleFunc("/v1/scopes/main", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client := newTestMinnowClient(t, server.URL)
	var attempts []int
	client.onScopeAttempt = func(attempt int) error {
		attempts = append(attempts, attempt)
		return nil
	}

	err := finalizeRun(context.Background(), client, "kb", "main", []string{"wanted"}, "", false)
	require.Error(t, err)
	require.Equal(t, []int{1, 2}, attempts)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, keys, 2)
	require.NotEqual(t, keys[0], keys[1])
}

func TestResumeEpoch(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.session = "instance-1:retry"
	server.mu.Unlock()
	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(path, []byte(
		"kb legacy\nscope branch\ni old\nc old\npublished\ni retry\nc retry\nsession instance-1:retry\n",
	), 0o600))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "legacy", ScopeID: "branch"},
		path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.Contains(t, confirmed, "retry")
	require.NotContains(t, confirmed, "old")
	require.NoError(t, journal.close())
}

func TestResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.journal")
	journal, contents, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	require.Empty(t, contents.confirmed)

	first := &recordingIngester{}
	sink := &documentSink{
		client: first, kbID: "kb", journal: journal,
		policy:   indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20},
		progress: newProgressReporter(true),
	}
	require.NoError(t, sink.emit(context.Background(), docsOfSize("d", 10, 10)))
	require.NoError(t, sink.close(context.Background()))
	require.NoError(t, journal.close())

	resumed, contents, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"da", "db"}, contents.confirmed)
	confirmed := map[string]struct{}{"da": {}, "db": {}}
	second := &recordingIngester{}
	sink = &documentSink{
		client: second, kbID: "kb", journal: resumed, confirmed: confirmed,
		policy:   indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20},
		progress: newProgressReporter(true),
	}
	require.NoError(t, sink.emit(context.Background(), docsOfSize("d", 10, 10)))
	require.NoError(t, sink.close(context.Background()))
	require.Empty(t, second.batches)
	require.NoError(t, resumed.close())
}

type confirmFailure struct{ uploadJournal }

func (confirmFailure) confirm([]string) error { return errors.New("confirm failed") }

func TestUncertainBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.journal")
	journal, _, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	recorder := &confirmFailure{uploadJournal: *journal}
	sink := &documentSink{
		client: &recordingIngester{}, kbID: "kb", journal: recorder,
		policy:   indexer.ResourcePolicy{EmbedBatchSize: 1, MaxBatchBytes: 1 << 20},
		progress: newProgressReporter(true),
	}
	require.Error(t, sink.emit(context.Background(), docsOfSize("d", 10, 10)))
	require.NoError(t, recorder.close())

	_, contents, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	require.Empty(t, contents.confirmed)
}
