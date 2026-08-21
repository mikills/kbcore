package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
		if first.KBID == target.KBID {
			t.Fatal("no generation was assigned")
		}
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
		if !strings.HasPrefix(assigned.KBID, "code-repo-chosen-by-flag-") {
			t.Fatalf("a stale reservation overrode the requested knowledge base: %q", assigned.KBID)
		}
	})

	t.Run("a truncated reservation is not indexed into", func(t *testing.T) {
		target := newTarget(t.TempDir())
		assigned := reserve(t, target)
		truncated := assigned.KBID[:len(target.KBID)+3]
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

	t.Run("recorded state wins over the reservation", func(t *testing.T) {
		target := newTarget(t.TempDir())
		reserve(t, target)
		resumed, err := assignIndexGeneration(target, indexState{KBID: "code-repo-from-state"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if resumed.KBID != "code-repo-from-state" {
			t.Fatalf("state was ignored in favour of the reservation: %q", resumed.KBID)
		}
	})

	t.Run("clearing the reservation forces a fresh knowledge base", func(t *testing.T) {
		target := newTarget(t.TempDir())
		first := reserve(t, target)
		// Deleting the state file must still mean "index this from scratch".
		clearReservedKBID(target)
		second, err := assignIndexGeneration(target, indexState{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if second.KBID == first.KBID {
			t.Fatal("a cleared reservation still pinned the old knowledge base")
		}
	})

	t.Run("two indexes in one checkout reserve separately", func(t *testing.T) {
		root := t.TempDir()
		main := newTarget(root)
		branch := newTarget(root)
		branch.IndexKey = "feature-xyz"
		branch.KBID = "code-repo-feature-xyz"

		mainKB := reserve(t, main)
		branchKB := reserve(t, branch)
		if mainKB.KBID == branchKB.KBID {
			t.Fatal("two indexes shared one reservation")
		}
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

func TestReservationOnlyPinsAMintedKnowledgeBase(t *testing.T) {
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
	// A second run reads its id from state, so there is nothing to reserve.
	broken := cfg
	broken.Minnow.URL = "http://127.0.0.1:1"
	if _, err := refreshIndex(context.Background(), broken, opts); err == nil {
		t.Fatal("a run against a dead server reported success")
	}
	if reserved := loadReservedKBID(target); reserved != "" {
		t.Fatalf("a failed run that read its id from state reserved %q", reserved)
	}

	if err := os.Remove(indexStatePath(target)); err != nil {
		t.Fatal(err)
	}
	third, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if third.KBID == first.KBID {
		t.Fatalf("deleting the state file reused knowledge base %s", third.KBID)
	}
}
