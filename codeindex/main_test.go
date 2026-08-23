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
	"strings"
	"sync"
	"testing"
	"time"

	indexer "github.com/mikills/minnow/kb/codeindex"
	"github.com/stretchr/testify/require"
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

func TestBranchReuse(t *testing.T) {
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
	if reconfigured.IndexedFiles == 0 || reconfigured.UnchangedFiles != 0 || reconfigured.ChunksIndexed != 0 {
		t.Fatalf("pipeline change did not force reindex: %+v", reconfigured)
	}
	server.assertIngestCount(t, 1)

	runTestGit(t, root, "checkout", "-b", "feature/client")
	third, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, first.KBID, third.KBID)
	require.NotEqual(t, first.ScopeID, third.ScopeID)
	require.NotEqual(t, first.IndexKey, third.IndexKey)
	require.Zero(t, third.ChunksIndexed)
	require.Equal(t, 2, third.ChunksReused)
	server.assertIngestCount(t, 1)

	runTestGit(t, root, "checkout", "main")
	returned, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if returned.KBID != first.KBID || returned.ChunksIndexed != 0 || returned.UnchangedFiles != 2 {
		t.Fatalf("returning to main did not reuse incremental state: first=%+v returned=%+v", first, returned)
	}
	server.assertIngestCount(t, 1)

	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\nfunc Main() { println(\"changed\") }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if changed.IndexedFiles != 1 || changed.UnchangedFiles != 1 || changed.ChunksScheduled != 0 {
		t.Fatalf("changed-file refresh was not incremental: %+v", changed)
	}
	// The commit is queued and then awaited, so the run ends on the publish the
	// server reports for it rather than on the request that asked for it.
	server.assertLastMutationOrder(t, "ingest", "publish", "commit", "publish")

	if err := os.WriteFile(filepath.Join(root, "new.go"), []byte("package main\nfunc New() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, root, "add", "new.go")
	withNewFile, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if withNewFile.IndexedFiles != 1 || withNewFile.UnchangedFiles != 2 || withNewFile.ChunksScheduled != 0 {
		t.Fatalf("new-file refresh was not incremental: %+v", withNewFile)
	}
	server.assertLastMutationOrder(t, "ingest", "publish", "commit", "publish")

	if err := os.Remove(filepath.Join(root, "helper.go")); err != nil {
		t.Fatal(err)
	}
	withDeletedFile, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if withDeletedFile.DeletedFiles != 1 || withDeletedFile.UnchangedFiles != 2 || withDeletedFile.ChunksScheduled != 0 {
		t.Fatalf("deleted-file refresh did not remove stale chunks: %+v", withDeletedFile)
	}
	server.assertLastMutationOrder(t, "commit", "publish")
}

func TestPipelineVersionForcesReindex(t *testing.T) {
	opts := indexer.NormalizeOptions(indexer.Options{})
	if pipelineFingerprintForVersion(opts, "pipeline/v1") == pipelineFingerprintForVersion(opts, "pipeline/v2") {
		t.Fatal("pipeline implementation version did not affect fingerprint")
	}
}

func TestMissingStateReusesScope(t *testing.T) {
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
	require.Equal(t, first.KBID, second.KBID)
	require.Positive(t, second.IndexedFiles)
	require.Zero(t, second.ChunksIndexed)
	require.Positive(t, second.ChunksReused)
	server.assertIngestCount(t, 1)
}

func TestExplicitBranchScope(t *testing.T) {
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
	require.NotEqual(t, mainResult.IndexKey, featureResult.IndexKey)
	require.Equal(t, mainResult.KBID, featureResult.KBID)
	require.NotEqual(t, mainResult.ScopeID, featureResult.ScopeID)
	if mainResult.StatePath == featureResult.StatePath {
		t.Fatalf("explicit branch refreshes shared state path %q", mainResult.StatePath)
	}
	server.assertIngestCount(t, 1)
}

func TestWorktreeIdentity(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")
	worktree := filepath.Join(t.TempDir(), "feature")
	runTestGit(t, root, "worktree", "add", "-b", "feature", worktree)

	mainTarget, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	mainTarget, err = assignIndexGeneration(mainTarget, indexState{}, false)
	require.NoError(t, err)
	saveReservedKBID(mainTarget)

	featureTarget, err := resolveTarget(indexCLIOptions{root: worktree})
	require.NoError(t, err)
	featureTarget, err = assignIndexGeneration(featureTarget, indexState{}, false)
	require.NoError(t, err)
	require.Equal(t, mainTarget.RepoID, featureTarget.RepoID)
	require.Equal(t, mainTarget.KBID, featureTarget.KBID)
	require.NotEqual(t, mainTarget.ScopeID, featureTarget.ScopeID)
	release, err := acquireRefreshLocks(mainTarget, time.Hour)
	require.NoError(t, err)
	defer release()
	_, err = acquireRefreshLocks(featureTarget, time.Hour)
	require.Error(t, err)
}

func TestRemove(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
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
	opts := indexCLIOptions{root: root, yes: true}
	indexed, err := refreshIndex(context.Background(), cfg, opts)
	require.NoError(t, err)
	target, err := resolveTarget(opts)
	require.NoError(t, err)
	target.KBID = indexed.KBID
	require.NoError(t, os.Mkdir(uploadJournalPath(target), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(uploadJournalPath(target), "blocked"), []byte("x"), 0o600))
	_, err = removeIndex(context.Background(), cfg, opts)
	require.Error(t, err)
	_, err = os.Stat(indexed.StatePath)
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(uploadJournalPath(target), "blocked")))
	require.NoError(t, os.Remove(uploadJournalPath(target)))

	removed, err := removeIndex(context.Background(), cfg, opts)
	require.NoError(t, err)
	require.Equal(t, indexed.ScopeID, removed.ScopeID)
	server.mu.Lock()
	_, exists := server.scopes[indexed.ScopeID]
	server.mu.Unlock()
	require.False(t, exists)
	require.Positive(t, removed.Scheduled)
	_, err = os.Stat(indexed.StatePath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRemoveLegacy(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	state := emptyIndexState(target)
	state.SchemaVersion = "codeindex.state/v1"
	state.KBID = target.LegacyKBID + "-1111111111111111"
	require.NoError(t, writeIndexStateFile(indexStatePath(target), state))

	_, err = removeIndex(context.Background(), defaultConfig(), indexCLIOptions{root: root})
	require.ErrorContains(t, err, "refresh")
	_, err = os.Stat(indexStatePath(target))
	require.NoError(t, err)
}

func TestRemoveInterrupted(t *testing.T) {
	setup := func(t *testing.T) (indexTarget, Config, indexCLIOptions, *testMinnowServer) {
		root := t.TempDir()
		runTestGit(t, root, "init", "-b", "main")
		target, err := resolveTarget(indexCLIOptions{root: root})
		require.NoError(t, err)
		server := newTestMinnowServer(t)
		t.Cleanup(server.Close)
		cfg := defaultConfig()
		cfg.Minnow.URL = server.URL
		return target, cfg, indexCLIOptions{root: root}, server
	}

	t.Run("first run", func(t *testing.T) {
		target, cfg, opts, server := setup(t)
		journal, _, err := startUploadJournal(uploadJournalPath(target), target)
		require.NoError(t, err)
		require.NoError(t, journal.record([]string{"partial"}))
		require.NoError(t, journal.confirm([]string{"partial"}))
		require.NoError(t, journal.close())
		server.published["partial"] = struct{}{}

		removed, err := removeIndex(context.Background(), cfg, opts)
		require.NoError(t, err)
		require.Equal(t, 1, removed.Scheduled)
		_, err = os.Stat(uploadJournalPath(target))
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("incremental run", func(t *testing.T) {
		target, cfg, opts, server := setup(t)
		state := emptyIndexState(target)
		state.Files["main.go"] = stateFile{ChunkIDs: []string{"existing"}}
		_, err := saveIndexState(target, state)
		require.NoError(t, err)
		journal, _, err := startUploadJournal(uploadJournalPath(target), target)
		require.NoError(t, err)
		require.NoError(t, journal.record([]string{"new"}))
		require.NoError(t, journal.confirm([]string{"new"}))
		require.NoError(t, journal.close())
		server.scopes[target.ScopeID] = []string{"existing"}
		server.published["existing"] = struct{}{}
		server.published["new"] = struct{}{}

		removed, err := removeIndex(context.Background(), cfg, opts)
		require.NoError(t, err)
		require.Equal(t, 2, removed.Scheduled)
	})

	t.Run("remote drift", func(t *testing.T) {
		target, cfg, opts, server := setup(t)
		state := emptyIndexState(target)
		state.Files["main.go"] = stateFile{ChunkIDs: []string{"local"}}
		_, err := saveIndexState(target, state)
		require.NoError(t, err)
		server.scopes[target.ScopeID] = []string{"local", "remote"}
		server.published["local"] = struct{}{}
		server.published["remote"] = struct{}{}

		removed, err := removeIndex(context.Background(), cfg, opts)
		require.NoError(t, err)
		require.Equal(t, 2, removed.Scheduled)
	})

	t.Run("scope race", func(t *testing.T) {
		target, cfg, opts, server := setup(t)
		state := emptyIndexState(target)
		state.Files["main.go"] = stateFile{ChunkIDs: []string{"local"}}
		path, err := saveIndexState(target, state)
		require.NoError(t, err)
		server.scopes[target.ScopeID] = []string{"local"}
		server.scopeDeleteStatus = http.StatusConflict

		_, err = removeIndex(context.Background(), cfg, opts)
		require.Error(t, err)
		_, err = os.Stat(path)
		require.NoError(t, err)
		require.Contains(t, server.scopes, target.ScopeID)
	})

	t.Run("schedule retry", func(t *testing.T) {
		target, cfg, opts, server := setup(t)
		state := emptyIndexState(target)
		state.Files["main.go"] = stateFile{ChunkIDs: []string{"local"}}
		path, err := saveIndexState(target, state)
		require.NoError(t, err)
		server.scopes[target.ScopeID] = []string{"local", "remote"}
		server.published["local"] = struct{}{}
		server.published["remote"] = struct{}{}
		server.gcStatus = http.StatusInternalServerError

		_, err = removeIndex(context.Background(), cfg, opts)
		require.Error(t, err)
		_, err = os.Stat(path)
		require.NoError(t, err)
		require.NotContains(t, server.scopes, target.ScopeID)

		server.gcStatus = 0
		removed, err := removeIndex(context.Background(), cfg, opts)
		require.NoError(t, err)
		require.Equal(t, 2, removed.Scheduled)
	})
}

func TestLegacyState(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	remote := "https://example.com/acme/repo.git"
	runTestGit(t, root, "remote", "add", "origin", remote)
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	require.Equal(t, shortHash(remote), target.RepoID)
	legacyKBID := target.LegacyKBID + "-0123456789abcdef"
	state := indexState{
		SchemaVersion: "codeindex.state/v1", KBID: legacyKBID, RepoID: target.RepoID,
		Ref: target.Ref, Root: target.Root, Files: map[string]stateFile{},
	}
	require.NoError(t, writeIndexStateFile(indexStatePath(target), state))
	loaded, _, exists, err := loadIndexState(target)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, legacyKBID, loaded.KBID)
}

func TestLocalLegacy(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	require.Equal(t, shortHash(target.Root), target.RepoID)
	legacyKBID := target.LegacyKBID + "-0123456789abcdef"
	state := indexState{
		SchemaVersion: "codeindex.state/v1", KBID: legacyKBID, RepoID: shortHash(target.Root),
		Ref: target.Ref, Root: target.Root, Files: map[string]stateFile{},
	}
	require.NoError(t, writeIndexStateFile(indexStatePath(target), state))
	loaded, _, exists, err := loadIndexState(target)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, legacyKBID, loaded.KBID)
}

func TestSeparateGit(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "primary")
	metadata := filepath.Join(base, "metadata")
	runTestGit(t, base, "init", "-b", "main", "--separate-git-dir", metadata, root)
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	runTestGit(t, root, "commit", "--allow-empty", "-m", "initial")
	worktree := filepath.Join(base, "feature-name")
	runTestGit(t, root, "worktree", "add", "-b", "feature", worktree)

	main, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	saveRepositoryRoot(main)
	feature, err := resolveTarget(indexCLIOptions{root: worktree})
	require.NoError(t, err)
	require.Equal(t, main.RepoID, feature.RepoID)
	require.Equal(t, shortHash(main.Root), main.RepoID)
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
	commits   []string
	gc        [][]string
	scopes    map[string][]string
	scopePuts int
	published map[string]struct{}
	polls     int
	session   string
	// commitStatus, when set, is the status /rag/commit answers with instead of
	// accepting the publish.
	commitStatus      int
	scopeStatus       int
	scopeDeleteStatus int
	gcStatus          int
}

// refuses mirrors the server: once a session is open, a request that does not
// carry its handle belongs to another client and is refused.
func (s *testMinnowServer) refuses(presented string) bool {
	return s.session != "" && presented != s.session
}

// issueSession mirrors the server: the caller's handle is renewed when it sends
// one, otherwise a new session opens.
func (s *testMinnowServer) issueSession(presented string) string {
	if presented != "" {
		s.session = presented
	} else if s.session == "" {
		s.session = "instance-1:token-1"
	}
	return s.session
}

func newTestMinnowServer(t *testing.T) *testMinnowServer {
	t.Helper()
	server := &testMinnowServer{scopes: make(map[string][]string), published: make(map[string]struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok", "capabilities": []string{"ingest_sessions", "document_scopes"},
		})
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
		if refused := server.refuses(request.SessionID); refused {
			server.mu.Unlock()
			http.Error(w, "another client holds the session", http.StatusConflict)
			return
		}
		server.ingests = append(server.ingests, request)
		if request.GCUnscoped {
			ids := make([]string, 0, len(request.Documents))
			for _, doc := range request.Documents {
				ids = append(ids, doc.ID)
			}
			server.gc = append(server.gc, ids)
		}
		server.mutations = append(server.mutations, "ingest")
		issued := server.issueSession(request.SessionID)
		server.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "evt-1", "session_id": issued})
	})
	mux.HandleFunc("/rag/commit", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			KBID      string `json:"kb_id"`
			SessionID string `json:"session_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		if status := server.commitStatus; status != 0 {
			server.mu.Unlock()
			http.Error(w, "commit failed", status)
			return
		}
		server.commits = append(server.commits, request.SessionID)
		server.mutations = append(server.mutations, "commit")
		for _, ingest := range server.ingests {
			for _, doc := range ingest.Documents {
				server.published[doc.ID] = struct{}{}
			}
		}
		// The publish releases the session, so the next run opens its own.
		server.session = ""
		server.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kb_id": request.KBID, "event_id": "evt-commit",
			"status_url": "/rag/operations/evt-commit", "session_id": request.SessionID,
		})
	})
	mux.HandleFunc("/rag/operations/evt-commit", func(w http.ResponseWriter, _ *http.Request) {
		server.mu.Lock()
		server.mutations = append(server.mutations, "publish")
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"terminal": map[string]any{"kind": "kb.published", "status": "done"},
		})
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
			IDs       []string `json:"ids"`
			SessionID string   `json:"session_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		if refused := server.refuses(request.SessionID); refused {
			server.mu.Unlock()
			http.Error(w, "another client holds the session", http.StatusConflict)
			return
		}
		server.deletes = append(server.deletes, append([]string(nil), request.IDs...))
		server.mutations = append(server.mutations, "delete")
		issued := server.issueSession(request.SessionID)
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ids": request.IDs, "session_id": issued})
	})
	mux.HandleFunc("/v1/scopes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			server.mu.Lock()
			scopes := make([]map[string]any, 0, len(server.scopes))
			for scopeID, ids := range server.scopes {
				scopes = append(scopes, map[string]any{
					"scope_id": scopeID, "document_ids": ids, "revision": "rev-" + scopeID,
				})
			}
			server.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": scopes})
		case http.MethodPut:
			var request struct {
				ScopeID     string   `json:"scope_id"`
				DocumentIDs []string `json:"document_ids"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			server.mu.Lock()
			if status := server.scopeStatus; status != 0 {
				server.mu.Unlock()
				http.Error(w, "scope failed", status)
				return
			}
			server.scopePuts++
			server.scopes[request.ScopeID] = append([]string(nil), request.DocumentIDs...)
			server.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"scope_id": request.ScopeID, "document_ids": request.DocumentIDs,
				"revision": "rev-" + request.ScopeID,
			})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/scopes/documents", func(w http.ResponseWriter, _ *http.Request) {
		server.mu.Lock()
		set := make(map[string]struct{})
		for _, ids := range server.scopes {
			for _, id := range ids {
				set[id] = struct{}{}
			}
		}
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"document_ids": ids})
	})
	mux.HandleFunc("/v1/scopes/gc", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			DocumentIDs []string `json:"document_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		if status := server.gcStatus; status != 0 {
			server.mu.Unlock()
			http.Error(w, "scope GC failed", status)
			return
		}
		server.gc = append(server.gc, append([]string(nil), request.DocumentIDs...))
		referenced := make(map[string]struct{})
		for _, ids := range server.scopes {
			for _, id := range ids {
				referenced[id] = struct{}{}
			}
		}
		scheduled := make([]string, 0, len(request.DocumentIDs))
		for _, id := range request.DocumentIDs {
			if _, ok := referenced[id]; !ok {
				scheduled = append(scheduled, id)
			}
		}
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"scheduled_ids": scheduled})
	})
	mux.HandleFunc("/v1/scopes/", func(w http.ResponseWriter, r *http.Request) {
		scopeID := strings.TrimPrefix(r.URL.Path, "/v1/scopes/")
		server.mu.Lock()
		ids, ok := server.scopes[scopeID]
		if r.Method == http.MethodDelete {
			if status := server.scopeDeleteStatus; status != 0 {
				server.mu.Unlock()
				http.Error(w, "scope delete failed", status)
				return
			}
			if revision := r.URL.Query().Get("revision"); revision != "" && revision != "rev-"+scopeID {
				server.mu.Unlock()
				http.Error(w, "scope changed", http.StatusConflict)
				return
			}
			delete(server.scopes, scopeID)
			server.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		server.mu.Unlock()
		if !ok {
			http.Error(w, "scope not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"scope_id": scopeID, "document_ids": ids, "revision": "rev-" + scopeID,
		})
	})
	mux.HandleFunc("/v1/vectors/fetch", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		records := make([]map[string]any, 0, len(request.IDs))
		for _, id := range request.IDs {
			if _, ok := server.published[id]; ok {
				records = append(records, map[string]any{"id": id})
			}
		}
		server.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"records": records})
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

func TestEmptyScope(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	client, err := newMinnowClient(cfg)
	require.NoError(t, err)
	require.NoError(t, client.check(context.Background()))
	_, err = client.scopeMembers(context.Background(), "kb", "empty")
	require.NoError(t, err)
	require.NoError(t, client.replaceScope(context.Background(), "kb", "empty", []string{}))
	server.mu.Lock()
	_, exists := server.scopes["empty"]
	server.mu.Unlock()
	require.True(t, exists)
}

func TestScopeRefresh(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.scopes["branch"] = []string{"a"}
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	client, err := newMinnowClient(cfg)
	require.NoError(t, err)
	require.NoError(t, client.check(context.Background()))
	_, err = client.scopeMembers(context.Background(), "kb", "branch")
	require.NoError(t, err)
	require.NoError(t, client.replaceScope(context.Background(), "kb", "branch", []string{"a"}))
	server.mu.Lock()
	defer server.mu.Unlock()
	require.Equal(t, 1, server.scopePuts)
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
