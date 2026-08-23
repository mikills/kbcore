package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A run writes its state only after it commits, so an interrupted first index
// has nothing recording which knowledge base it filled.
func TestKBIDReservation(t *testing.T) {
	newTarget := func(root string) indexTarget {
		return indexTarget{StateRoot: root, Root: root, IndexKey: "main-abc", KBID: "code-repo-main-abc"}
	}

	// refreshIndex reserves the id once the run is committed to uploading.
	reserve := func(t *testing.T, target indexTarget) indexTarget {
		t.Helper()
		assigned, err := assignIndexGeneration(target, indexState{}, false)
		if err != nil {
			t.Fatal(err)
		}
		saveReservedKBID(assigned)
		return assigned
	}

	t.Run("an interrupted first index resumes into the same knowledge base", func(t *testing.T) {
		target := newTarget(t.TempDir())
		first := reserve(t, target)
		second, err := assignIndexGeneration(target, indexState{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if second.KBID != first.KBID {
			t.Fatalf("the retry opened a new knowledge base: %q then %q", first.KBID, second.KBID)
		}
	})

	t.Run("a reservation for another knowledge base is ignored", func(t *testing.T) {
		root := t.TempDir()
		reserve(t, newTarget(root))

		// An explicit --kb, or a repository moved to this path, must not be
		// captured by whatever the previous run happened to reserve.
		chosen := newTarget(root)
		chosen.KBID = "code-repo-chosen-by-flag"
		assigned, err := assignIndexGeneration(chosen, indexState{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if assigned.KBID != "code-repo-chosen-by-flag" {
			t.Fatalf("a stale reservation overrode the requested knowledge base: %q", assigned.KBID)
		}
	})

	t.Run("a truncated reservation is not indexed into", func(t *testing.T) {
		target := newTarget(t.TempDir())
		reserve(t, target)
		truncated := target.KBID + "-bad"
		if err := os.WriteFile(reservedKBIDPath(target), []byte(truncated), 0o600); err != nil {
			t.Fatal(err)
		}
		resumed, err := assignIndexGeneration(target, indexState{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if resumed.KBID == truncated {
			t.Fatalf("a truncated reservation was trusted: %q", truncated)
		}
	})

	t.Run("repository mapping wins over branch state", func(t *testing.T) {
		target := newTarget(t.TempDir())
		reserved := reserve(t, target)
		resumed, err := assignIndexGeneration(target, indexState{KBID: "code-repo-from-state"}, true)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, reserved.KBID, resumed.KBID)
	})

	t.Run("completed indexes retain the repository generation", func(t *testing.T) {
		target := newTarget(t.TempDir())
		first := reserve(t, target)
		// Deleting the state file must still mean "index this from scratch".
		second, err := assignIndexGeneration(target, indexState{}, false)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, first.KBID, second.KBID)
	})

	t.Run("branches share the repository generation", func(t *testing.T) {
		root := t.TempDir()
		main := newTarget(root)
		branch := newTarget(root)
		branch.IndexKey = "feature-xyz"

		mainKB := reserve(t, main)
		branchKB := reserve(t, branch)
		require.Equal(t, mainKB.KBID, branchKB.KBID)
	})

	t.Run("an unwritable state directory still indexes", func(t *testing.T) {
		blocked := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		target := newTarget(filepath.Join(blocked, "root"))
		// Losing the reservation costs a retry its uploads. Failing the run
		// outright would be worse.
		if _, err := assignIndexGeneration(target, indexState{}, false); err != nil {
			t.Fatalf("an unwritable reservation failed the run: %v", err)
		}
	})
}

func TestCloneIdentity(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "repo.git")
	runTestGit(t, base, "init", "--bare", remote)
	seed := filepath.Join(base, "seed")
	runTestGit(t, base, "init", "-b", "main", seed)
	runTestGit(t, seed, "config", "user.email", "codeindex@example.com")
	runTestGit(t, seed, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, seed, "add", ".")
	runTestGit(t, seed, "commit", "-m", "initial")
	runTestGit(t, seed, "remote", "add", "origin", remote)
	runTestGit(t, seed, "push", "-u", "origin", "main")
	runTestGit(t, remote, "symbolic-ref", "HEAD", "refs/heads/main")
	firstRoot := filepath.Join(base, "first")
	secondRoot := filepath.Join(base, "second")
	runTestGit(t, base, "clone", remote, firstRoot)
	runTestGit(t, base, "clone", remote, secondRoot)
	first, err := resolveTarget(indexCLIOptions{root: firstRoot})
	require.NoError(t, err)
	second, err := resolveTarget(indexCLIOptions{root: secondRoot})
	require.NoError(t, err)
	first, err = assignIndexGeneration(first, indexState{}, false)
	require.NoError(t, err)
	second, err = assignIndexGeneration(second, indexState{}, false)
	require.NoError(t, err)
	require.Equal(t, first.KBID, second.KBID)
	require.Equal(t, first.ScopeID, second.ScopeID)
	require.Equal(t, shortHash(remote), first.RepoID)
	require.Equal(t, shortHash(remote), second.RepoID)
}

func TestLegacySelection(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")
	sibling := filepath.Join(t.TempDir(), "feature")
	runTestGit(t, root, "worktree", "add", "-b", "feature", sibling)
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	dir := filepath.Dir(indexStatePath(target))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	current := target.LegacyKBID + "-1111111111111111"
	feature, err := resolveTarget(indexCLIOptions{root: sibling})
	require.NoError(t, err)
	featureDir := filepath.Dir(indexStatePath(feature))
	require.NoError(t, os.MkdirAll(featureDir, 0o755))
	canonical := feature.LegacyKBID + "-2222222222222222"
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "current.journal"), []byte("kb "+current+"\ni a\n"), 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(featureDir, "canonical.journal"), []byte("kb "+canonical+"\ni a\ni b\ni c\n"), 0o600,
	))

	assigned, err := assignIndexGeneration(target, indexState{}, false)
	require.NoError(t, err)
	require.Equal(t, canonical, assigned.KBID)
	require.Equal(t, []string{"a", "b", "c"}, assigned.MigrationIDs)
	require.Equal(t, featureDir, assigned.MigrationDir)

	retried, err := assignIndexGeneration(target, indexState{KBID: current, Legacy: true}, true)
	require.NoError(t, err)
	require.Equal(t, canonical, retried.KBID)
	require.Equal(t, assigned.MigrationIDs, retried.MigrationIDs)
	require.Equal(t, assigned.MigrationDir, retried.MigrationDir)
}

func TestLegacyExplicitIsolation(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	explicit, err := resolveTarget(indexCLIOptions{root: root, indexKey: "other"})
	require.NoError(t, err)
	dir := filepath.Dir(indexStatePath(target))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	defaultKB := target.LegacyKBID + "-1111111111111111"
	explicitKB := explicit.LegacyKBID + "-2222222222222222"
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "default.journal"), []byte("kb "+defaultKB+"\ni a\n"), 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "explicit.journal"), []byte("kb "+explicitKB+"\ni a\ni b\n"), 0o600,
	))

	assigned, err := assignIndexGeneration(target, indexState{}, false)
	require.NoError(t, err)
	require.Equal(t, defaultKB, assigned.KBID)
}

func TestHistoricalLegacy(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")
	main, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	runTestGit(t, root, "checkout", "-b", "feature")
	feature, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	dir := filepath.Dir(indexStatePath(feature))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	mainKB := main.LegacyKBID + "-1111111111111111"
	featureKB := feature.LegacyKBID + "-2222222222222222"
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "main.journal"), []byte("kb "+mainKB+"\ni a\ni b\ni c\n"), 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "feature.journal"), []byte("kb "+featureKB+"\ni a\n"), 0o600,
	))

	assigned, err := assignIndexGeneration(feature, indexState{}, false)
	require.NoError(t, err)
	require.Equal(t, mainKB, assigned.KBID)
	require.Equal(t, []string{"a", "b", "c"}, assigned.MigrationIDs)
}

func TestDetachedLegacy(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")
	runTestGit(t, root, "checkout", "--detach")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	dir := filepath.Dir(indexStatePath(target))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	legacyKB := target.LegacyKBID + "-1111111111111111"
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "detached.journal"), []byte("kb "+legacyKB+"\ni a\n"), 0o600,
	))

	assigned, err := assignIndexGeneration(target, indexState{}, false)
	require.NoError(t, err)
	require.Equal(t, legacyKB, assigned.KBID)
}

// Go kills a process on SIGINT by default, so without a handler an interrupted
// run never unwinds and its index lock survives to block the next one for the
// whole stale window.
func TestInterruptContextCancels(t *testing.T) {
	ctx, stop := interruptContext()
	defer stop()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal self: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("an interrupt did not cancel the run")
	}
}

func TestInterruptContextStopIsIndependent(t *testing.T) {
	ctx, stop := interruptContext()
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop did not release the context")
	}
}

func TestRepositoryGeneration(t *testing.T) {
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
	target, err := resolveTarget(opts)
	if err != nil {
		t.Fatal(err)
	}
	// The repository reservation remains the branch-independent KB identity.
	broken := cfg
	broken.Minnow.URL = "http://127.0.0.1:1"
	if _, err := refreshIndex(context.Background(), broken, opts); err == nil {
		t.Fatal("a run against a dead server reported success")
	}
	require.Equal(t, first.KBID, loadReservedKBID(target))

	if err := os.Remove(indexStatePath(target)); err != nil {
		t.Fatal(err)
	}
	third, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, first.KBID, third.KBID)
}
