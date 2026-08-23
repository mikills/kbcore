package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIngestSession(t *testing.T) {
	t.Run("every batch defers the publish and one commit ends the run", func(t *testing.T) {
		root := t.TempDir()
		runTestGit(t, root, "init", "-b", "main")
		runTestGit(t, root, "config", "user.email", "codeindex@example.com")
		runTestGit(t, root, "config", "user.name", "Code Index")
		for name, body := range map[string]string{
			"main.go":  "package main\nfunc Main() {}\n",
			"other.go": "package main\nfunc Other() {}\n",
		} {
			if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		runTestGit(t, root, "add", ".")
		runTestGit(t, root, "commit", "-m", "initial")

		server := newTestMinnowServer(t)
		defer server.Close()
		cfg := defaultConfig()
		cfg.Minnow.URL = server.URL
		cfg.CodeIndex.PollInterval = "1ms"
		cfg.CodeIndex.OperationTimeout = "1s"
		requireConfirm := false
		cfg.CodeIndex.RequireConfirm = &requireConfirm
		// One chunk per request, so the run has to carry its handle from one
		// batch to the next rather than opening a session and never using it.
		opts := indexCLIOptions{root: root, yes: true, requestBatchSize: 1}

		if _, err := refreshIndex(context.Background(), cfg, opts); err != nil {
			t.Fatal(err)
		}

		server.mu.Lock()
		ingests := append([]ingestRequest(nil), server.ingests...)
		commits := append([]string(nil), server.commits...)
		server.mu.Unlock()

		if len(ingests) < 2 {
			t.Fatalf("expected several batches, got %d", len(ingests))
		}
		// The server owns the session, so the first batch opens one and every
		// later request carries back the handle it issued.
		if ingests[0].SessionID != "" {
			t.Fatalf("first ingest invented a session id: %q", ingests[0].SessionID)
		}
		issued := ingests[1].SessionID
		if issued == "" {
			t.Fatal("the second batch did not carry the handle the first was issued")
		}
		for i, in := range ingests {
			if !in.DeferPublish {
				t.Fatalf("ingest %d did not defer the publish", i)
			}
			if i > 0 && in.SessionID != issued {
				t.Fatalf("ingest %d used session %q, want %q", i, in.SessionID, issued)
			}
		}
		if len(commits) != 1 {
			t.Fatalf("expected exactly one commit, got %d", len(commits))
		}
		if commits[0] != issued {
			t.Fatalf("commit used session %q, want %q", commits[0], issued)
		}
	})

	t.Run("a committed session is not presented again by a later run", func(t *testing.T) {
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

		result, err := refreshIndex(context.Background(), cfg, opts)
		if err != nil {
			t.Fatal(err)
		}
		target := indexTarget{StateRoot: root, KBID: result.KBID}
		if id := loadSession(target); id != "" {
			t.Fatalf("a published session was left on disk: %q", id)
		}
	})
}

// An interrupted run must present its own handle again rather than wait out the
// server's lease, and unrelated indexes must not share one.
func TestSessionStore(t *testing.T) {
	t.Run("the handle survives the run that received it", func(t *testing.T) {
		target := indexTarget{StateRoot: t.TempDir(), KBID: "code-main"}
		if id := loadSession(target); id != "" {
			t.Fatalf("empty state returned a session: %q", id)
		}
		saveSession(target, "instance-1:token-1")
		if id := loadSession(target); id != "instance-1:token-1" {
			t.Fatalf("session did not survive: %q", id)
		}
		clearSession(target)
		if id := loadSession(target); id != "" {
			t.Fatalf("cleared session came back: %q", id)
		}
	})

	t.Run("each knowledge base keeps its own handle", func(t *testing.T) {
		root := t.TempDir()
		main := indexTarget{StateRoot: root, KBID: "code-main"}
		branch := indexTarget{StateRoot: root, KBID: "code-feature"}
		saveSession(main, "instance-1:main-token")
		saveSession(branch, "instance-1:branch-token")
		if loadSession(main) == loadSession(branch) {
			t.Fatal("two knowledge bases shared one session handle")
		}
	})
}

func TestCommit(t *testing.T) {
	t.Run("a server that cannot commit is never asked to defer", func(t *testing.T) {
		var paths []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		}))
		defer server.Close()

		cfg := defaultConfig()
		cfg.Minnow.URL = server.URL
		client, err := newMinnowClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.check(context.Background()); err != nil {
			t.Fatal(err)
		}
		if client.canDeferPublish {
			t.Fatal("deferring against a server that never advertised the capability")
		}
		if err := client.commit(context.Background(), "kb"); err != nil {
			t.Fatalf("commit without a session: %v", err)
		}
		for _, p := range paths {
			if p == "/rag/commit" {
				t.Fatal("commit was sent to a server that cannot commit")
			}
		}
	})

	t.Run("a server advertising the capability is deferred against", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "ok", "capabilities": []string{"ingest_sessions"},
			})
		}))
		defer server.Close()

		cfg := defaultConfig()
		cfg.Minnow.URL = server.URL
		client, err := newMinnowClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.check(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !client.canDeferPublish {
			t.Fatal("an advertised capability was not honoured")
		}
	})

	t.Run("any other failure is surfaced", func(t *testing.T) {

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "another client holds the session", http.StatusConflict)
		}))
		defer server.Close()

		cfg := defaultConfig()
		cfg.Minnow.URL = server.URL
		client, err := newMinnowClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		client.sessionID = "session-token"
		err = client.commit(context.Background(), "kb")
		if err == nil || !strings.Contains(err.Error(), "another client") {
			t.Fatalf("conflict was not surfaced: %v", err)
		}
	})
}

func TestSessionConflictIsWaitedOut(t *testing.T) {
	newConflictServer := func(retryAfter string, conflicts int) (*httptest.Server, *int32) {
		var calls int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if int(atomic.AddInt32(&calls, 1)) <= conflicts {
				if retryAfter != "" {
					w.Header().Set("Retry-After", retryAfter)
				}
				http.Error(w, "another client is ingesting into kb", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ids": []string{"a"}, "session_id": "instance:token"})
		}))
		return server, &calls
	}
	deferringClient := func(t *testing.T, url string, waits *[]time.Duration) *minnowClient {
		t.Helper()
		client := newTestMinnowClient(t, url)
		client.wait = func(_ context.Context, d time.Duration) error {
			*waits = append(*waits, d)
			return nil
		}
		client.canDeferPublish, client.sessionKB = true, "kb"
		return client
	}

	t.Run("a lease that will lapse is waited for", func(t *testing.T) {
		server, calls := newConflictServer("2", 2)
		defer server.Close()
		var waits []time.Duration
		client := deferringClient(t, server.URL, &waits)

		if err := client.delete(context.Background(), "kb", []string{"a"}); err != nil {
			t.Fatal(err)
		}
		// A run refused by the orphan its own lost response left behind has to
		// outlast it, not abandon everything already uploaded.
		if len(waits) != 2 || waits[0] != 2*time.Second {
			t.Fatalf("waited %v, want two 2s waits", waits)
		}
		if atomic.LoadInt32(calls) != 3 {
			t.Fatalf("%d requests, want 3", atomic.LoadInt32(calls))
		}
		if client.sessionID != "instance:token" {
			t.Fatalf("session %q was not adopted after the wait", client.sessionID)
		}
	})

	t.Run("a deadline that has already passed is not hammered", func(t *testing.T) {
		server, calls := newConflictServer("0", 1)
		defer server.Close()
		var waits []time.Duration
		client := deferringClient(t, server.URL, &waits)

		if err := client.delete(context.Background(), "kb", []string{"a"}); err != nil {
			t.Fatal(err)
		}
		// Retry-After is whatever the far end says. Without a floor a server
		// reporting a lapsed deadline is polled flat out for the whole budget.
		if len(waits) != 1 || waits[0] < time.Second {
			t.Fatalf("waited %v over %d requests, want at least a second", waits, atomic.LoadInt32(calls))
		}
	})

	t.Run("a conflict with no deadline is reported", func(t *testing.T) {
		server, calls := newConflictServer("", 1)
		defer server.Close()
		var waits []time.Duration
		client := deferringClient(t, server.URL, &waits)

		err := client.delete(context.Background(), "kb", []string{"a"})
		if err == nil || !strings.Contains(err.Error(), "another client") {
			t.Fatalf("conflict was not reported: %v", err)
		}
		if len(waits) != 0 || atomic.LoadInt32(calls) != 1 {
			t.Fatalf("waited %v over %d requests with nothing to wait for", waits, atomic.LoadInt32(calls))
		}
	})

	t.Run("a lease outlasting the budget is reported", func(t *testing.T) {
		server, calls := newConflictServer("600", 1)
		defer server.Close()
		var waits []time.Duration
		client := deferringClient(t, server.URL, &waits)
		client.conflictBudget = time.Minute

		if err := client.delete(context.Background(), "kb", []string{"a"}); err == nil {
			t.Fatal("a conflict past the budget was waited for anyway")
		}
		if len(waits) != 0 || atomic.LoadInt32(calls) != 1 {
			t.Fatalf("waited %v over %d requests, want neither", waits, atomic.LoadInt32(calls))
		}
	})
}

func TestCommitResume(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\nfunc Main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.commitStatus = http.StatusInternalServerError
	server.mu.Unlock()

	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm
	opts := indexCLIOptions{root: root, yes: true}

	if _, err := refreshIndex(context.Background(), cfg, opts); err == nil {
		t.Fatal("a run whose commit failed reported success")
	}
	target, err := resolveTarget(opts)
	if err != nil {
		t.Fatal(err)
	}
	// State must not record success before the deferred writes are published,
	// or the next run treats every file as indexed and never sends it again.
	if _, err := os.Stat(indexStatePath(target)); !os.IsNotExist(err) {
		t.Fatalf("state was written for a run that never published: %v", err)
	}
	// The knowledge base id has to be pinned, or the retry opens a new one and
	// abandons everything this run uploaded.
	reserved := loadReservedKBID(target)
	if reserved == "" {
		t.Fatal("the knowledge base this run uploaded into was not reserved")
	}
	// The handle has to survive so the retry resumes its own session instead of
	// waiting out the server's lease on it.
	uploaded := target
	uploaded.KBID = reserved
	if loadSession(uploaded) == "" {
		t.Fatal("the run's session handle was not kept for the retry")
	}
	journal, err := loadUploadJournal(uploadJournalPath(uploaded))
	require.NoError(t, err)
	require.NotEmpty(t, journal.sessionID)
	server.mu.Lock()
	uploads := len(server.ingests)
	server.commitStatus = 0
	server.mu.Unlock()
	result, err := refreshIndex(context.Background(), cfg, opts)
	require.NoError(t, err)
	server.mu.Lock()
	resumedUploads := len(server.ingests)
	server.mu.Unlock()
	require.Equal(t, uploads, resumedUploads)
	require.NotEmpty(t, result.StatePath)
}

func TestSessionsDoNotCrossKnowledgeBases(t *testing.T) {
	client := newTestMinnowClient(t, "http://example.invalid")
	client.canDeferPublish, client.sessionKB = true, "kb-current"

	// Journal recovery addresses knowledge bases from earlier runs. Deferring
	// those would strand their rows under a session nothing ever commits.
	if client.defers("kb-old") {
		t.Fatal("a knowledge base from an earlier run was deferred into this session")
	}
	if !client.defers("kb-current") {
		t.Fatal("this run's own knowledge base was not deferred")
	}
	client.adoptSession("kb-old", "instance:stranger")
	if client.sessionID != "" {
		t.Fatalf("adopted %q from another knowledge base", client.sessionID)
	}
	client.adoptSession("kb-current", "instance:mine")
	if client.sessionID != "instance:mine" {
		t.Fatalf("session %q, want the handle issued for this run", client.sessionID)
	}
}
