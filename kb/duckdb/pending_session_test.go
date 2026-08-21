package duckdb

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	kb "github.com/mikills/minnow/kb"
)

// A refresh rebuilds the local shard from the published manifest, so running it
// while a deferred session holds unpublished rows would destroy them silently.
func TestMutableShardRefresh(t *testing.T) {
	newFormat := func(t *testing.T) (*DuckDBArtifactFormat, string) {
		t.Helper()
		cache := t.TempDir()
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
			CacheDir:      cache,
			ManifestStore: &stubManifestStore{head: "v2"},
		}}
		return f, filepath.Join(cache, "kb-1")
	}

	t.Run("a marked directory is refused instead of overwritten", func(t *testing.T) {
		f, kbDir := newFormat(t)
		if err := kb.MarkPendingSession(kbDir); err != nil {
			t.Fatal(err)
		}
		err := f.ensureMutableShardDBLocked(context.Background(), mutableShardRequest{
			kbID:   "kb-1",
			kbDir:  kbDir,
			dbPath: filepath.Join(kbDir, vectorsDuckDBFileName),
		})
		if !errors.Is(err, kb.ErrUnpublishedWrites) {
			t.Fatalf("stale local shard with unpublished writes was not refused: %v", err)
		}
	})
}

// A deferred session's rows are only in the local shard until the commit, so a
// knowledge base that has never published is not an empty one.
func TestUnpublishedKnowledgeBaseIsNotUninitialized(t *testing.T) {
	ctx := context.Background()
	cache := t.TempDir()
	f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
		CacheDir:      cache,
		ManifestStore: &stubManifestStore{head: ""},
	}}
	kbDir := filepath.Join(cache, "kb-1")
	dbPath := filepath.Join(kbDir, vectorsDuckDBFileName)
	req := mutableShardRequest{kbID: "kb-1", kbDir: kbDir, dbPath: dbPath, embeddingDim: 8}

	t.Run("an absent knowledge base is still refused", func(t *testing.T) {
		err := f.ensureMutableShardDBLocked(ctx, req)
		if !errors.Is(err, kb.ErrKBUninitialized) {
			t.Fatalf("a knowledge base with no rows anywhere was accepted: %v", err)
		}
	})

	t.Run("a knowledge base holding deferred rows is reachable", func(t *testing.T) {
		bootstrap := req
		bootstrap.allowBootstrap = true
		if err := f.ensureMutableShardDBLocked(ctx, bootstrap); err != nil {
			t.Fatal(err)
		}
		if err := kb.MarkPendingSession(kbDir); err != nil {
			t.Fatal(err)
		}
		// This is the path a deferred delete takes: it may not create a knowledge
		// base, but it must reach one whose rows are sitting on disk uncommitted.
		joining := req
		joining.joinsSession = true
		if err := f.ensureMutableShardDBLocked(ctx, joining); err != nil {
			t.Fatalf("deferred rows were reported as an uninitialized knowledge base: %v", err)
		}
	})

	t.Run("a write outside the session is refused", func(t *testing.T) {
		// The session does not move the manifest, so the local shard looks fresh.
		err := f.ensureMutableShardDBLocked(ctx, req)
		if !errors.Is(err, kb.ErrUnpublishedWrites) {
			t.Fatalf("an unrelated write joined an open session: %v", err)
		}
	})
}

// An interrupted first index leaves a journal of chunk ids for a knowledge
// base that was never created.
func TestDeleteFromAKnowledgeBaseThatWasNeverCreated(t *testing.T) {
	ctx := context.Background()
	cache := t.TempDir()
	var mu sync.Mutex
	locks := map[string]*sync.Mutex{}
	f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
		CacheDir:      cache,
		ManifestStore: &stubManifestStore{head: ""},
		LockFor: func(id string) *sync.Mutex {
			mu.Lock()
			defer mu.Unlock()
			if locks[id] == nil {
				locks[id] = &sync.Mutex{}
			}
			return locks[id]
		},
	}}

	result, err := f.Delete(ctx, kb.IngestDeleteRequest{KBID: "kb-1", DocIDs: []string{"c1", "c2"}})
	if err != nil {
		t.Fatalf("deleting from an absent knowledge base failed: %v", err)
	}
	if result.MutatedCount != 0 {
		t.Fatalf("reported %d deletions from a knowledge base with no rows", result.MutatedCount)
	}
}

// Both of these run before anything else is wired, so a stub with no manifest
// store proves the guard fired rather than the work being skipped by accident.
func TestAnOpenSessionIsLeftAloneByHousekeeping(t *testing.T) {
	ctx := context.Background()
	cache := t.TempDir()
	var mu sync.Mutex
	locks := map[string]*sync.Mutex{}
	f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
		CacheDir: cache,
		LockFor: func(id string) *sync.Mutex {
			mu.Lock()
			defer mu.Unlock()
			if locks[id] == nil {
				locks[id] = &sync.Mutex{}
			}
			return locks[id]
		},
	}}

	t.Run("compaction skips a knowledge base with unpublished rows", func(t *testing.T) {
		if err := kb.MarkPendingSession(filepath.Join(cache, "kb-1")); err != nil {
			t.Fatal(err)
		}
		// Compacting moves the manifest past the session's local rows, which
		// strands them with nothing left pointing at the shard they are in.
		result, err := f.CompactIfNeeded(ctx, "kb-1")
		if err != nil {
			t.Fatalf("compaction did not skip an open session: %v", err)
		}
		if result == nil || result.Performed {
			t.Fatalf("compaction ran against an open session: %+v", result)
		}
	})

	t.Run("a commit for a knowledge base with nothing deferred publishes nothing", func(t *testing.T) {
		// Republishing costs a full rebuild for no change and churns every
		// reader's cached shards.
		if err := f.CommitPrepared(ctx, "kb-2"); err != nil {
			t.Fatalf("an empty commit did work anyway: %v", err)
		}
	})
}
