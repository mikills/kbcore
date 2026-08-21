package kb_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/cacheevict"
)

func TestDeferPublish(t *testing.T) {
	docs := []EmbeddedDocument{{ID: "d1", Text: "x", Embedding: []float32{1}}}

	t.Run("a deferred write does not publish a snapshot", func(t *testing.T) {
		format := &preparedOnlyFormat{}
		h, _, _ := newAsyncHarness(t, "defer-on", WithArtifactFormat(format))
		err := h.KB().PublishPreparedDocs(context.Background(), "kb", docs, nil, UpsertDocsOptions{DeferPublish: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(format.uploads) != 1 || format.uploads[0] {
			t.Fatalf("deferred write still published: %v", format.uploads)
		}
	})

	t.Run("an ordinary write still publishes", func(t *testing.T) {
		format := &preparedOnlyFormat{}
		h, _, _ := newAsyncHarness(t, "defer-off", WithArtifactFormat(format))
		err := h.KB().PublishPreparedDocs(context.Background(), "kb", docs, nil, UpsertDocsOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(format.uploads) != 1 || !format.uploads[0] {
			t.Fatalf("ordinary write did not publish: %v", format.uploads)
		}
	})
}

func TestPendingSessionMarker(t *testing.T) {
	t.Run("marking survives a reread and clearing removes it", func(t *testing.T) {
		dir := t.TempDir()
		if HasPendingSession(dir) {
			t.Fatal("a fresh directory reported unpublished writes")
		}
		if err := MarkPendingSession(dir); err != nil {
			t.Fatal(err)
		}
		if !HasPendingSession(dir) {
			t.Fatal("marker did not survive")
		}
		if err := ClearPendingSession(dir); err != nil {
			t.Fatal(err)
		}
		if HasPendingSession(dir) {
			t.Fatal("marker outlived the clear")
		}
	})

	t.Run("clearing an unmarked directory is not an error", func(t *testing.T) {
		if err := ClearPendingSession(t.TempDir()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("an unreadable directory is never reported as safe", func(t *testing.T) {
		dir := t.TempDir()
		if err := MarkPendingSession(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o000); err != nil {
			t.Skip("cannot drop directory permissions here")
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
		if !HasPendingSession(dir) {
			t.Fatal("an unreadable directory was treated as safe to overwrite")
		}
	})

	t.Run("a directory that cannot be reached is never reported as safe", func(t *testing.T) {
		parent := t.TempDir()
		dir := filepath.Join(parent, "kb")
		if err := MarkPendingSession(dir); err != nil {
			t.Fatal(err)
		}
		// Not the same as an unreadable directory: here the lookup of the
		// directory itself fails, and treating that as "no marker" would let a
		// refresh overwrite rows nothing else is protecting.
		if err := os.Chmod(parent, 0o000); err != nil {
			t.Skip("cannot drop directory permissions here")
		}
		t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
		if !HasPendingSession(dir) {
			t.Fatal("a directory that could not be reached was treated as safe to overwrite")
		}
	})

	t.Run("a shard file is not a knowledge base directory", func(t *testing.T) {
		dir := t.TempDir()
		shard := filepath.Join(dir, "shard-0001.duckdb")
		if err := os.WriteFile(shard, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		// Eviction walks shard files; treating one as marked stops the sweeper
		// freeing anything and the cache fills until every request fails.
		if HasPendingSession(shard) {
			t.Fatal("a shard file was reported as holding unpublished writes")
		}
	})

	t.Run("marking creates the directory when the cache entry is gone", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "kb-1")
		if err := MarkPendingSession(dir); err != nil {
			t.Fatal(err)
		}
		if !HasPendingSession(dir) {
			t.Fatal("marker was not written")
		}
	})
}

// committingFormat stands in for the shard layer: a real commit publishes and
// clears the marker, which is exactly what leaves a redelivery unable to tell
// itself apart from a commit with nothing to do.
type committingFormat struct {
	preparedOnlyFormat
	cacheDir string
	commits  []string
	failWith error
}

func (f *committingFormat) CommitPrepared(_ context.Context, kbID string) error {
	f.commits = append(f.commits, kbID)
	if f.failWith != nil {
		return f.failWith
	}
	return ClearPendingSession(filepath.Join(f.cacheDir, kbID))
}

func TestSessionCommitIsIdempotentAcrossRedelivery(t *testing.T) {
	ctx := context.Background()
	format := &committingFormat{}
	h, _, _ := newAsyncHarness(t, "commit-redelivery", WithArtifactFormat(format))
	loader := h.KB()
	format.cacheDir = loader.CacheDir
	if err := MarkPendingSession(filepath.Join(loader.CacheDir, "kb")); err != nil {
		t.Fatal(err)
	}

	sessions := loader.IngestSessionsFor()
	handle, err := sessions.Hold(ctx, "kb", "")
	if err != nil {
		t.Fatal(err)
	}
	worker := &SessionCommitWorker{KB: loader, ID: "test", ReleaseSession: sessions.Release}
	payload, err := json.Marshal(SessionCommitPayload{KBID: "kb", SessionID: handle})
	if err != nil {
		t.Fatal(err)
	}
	event := &KBEvent{
		EventID: "evt-commit", Kind: EventSessionCommit, KBID: "kb", Payload: payload, Attempt: 1,
	}

	if _, err := worker.Handle(ctx, event); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if len(format.commits) != 1 {
		t.Fatalf("published %d times, want 1", len(format.commits))
	}
	// The rows are durable, so the session has to come back. Holding it for the
	// rest of the TTL locks every other writer out of a knowledge base that has
	// nothing pending.
	next, err := sessions.Hold(ctx, "kb", "")
	if err != nil {
		t.Fatalf("a finished commit kept the session: %v", err)
	}
	if err := sessions.Release(ctx, "kb", next); err != nil {
		t.Fatal(err)
	}

	// The pool can hand the same event back after a crash. Failing here would
	// dead-letter work that already succeeded and make the caller redo the run.
	event.Attempt = 2
	if _, err := worker.Handle(ctx, event); err != nil {
		t.Fatalf("a redelivery of a finished commit failed: %v", err)
	}
	if len(format.commits) != 1 {
		t.Fatalf("a redelivery republished: %d commits", len(format.commits))
	}

	// A different commit for the same knowledge base has nothing to publish and
	// must not inherit this one's success.
	other := &KBEvent{
		EventID: "evt-other", Kind: EventSessionCommit, KBID: "kb", Payload: payload, Attempt: 2,
	}
	if _, err := worker.Handle(ctx, other); !errors.Is(err, ErrNothingDeferred) {
		t.Fatalf("another commit inherited this one's publish: %v", err)
	}
}

// A reaper that swallows failures reports a clean tick while every knowledge
// base it touched is still wedged, and one that walks control entries treats
// the cache's own bookkeeping as a knowledge base.
func TestReapAbandonedSessionsReporting(t *testing.T) {
	ctx := context.Background()
	format := &committingFormat{failWith: errors.New("publish failed")}
	h, _, _ := newAsyncHarness(t, "reap-reporting", WithArtifactFormat(format))
	loader := h.KB()
	format.cacheDir = loader.CacheDir

	if err := MarkPendingSession(filepath.Join(loader.CacheDir, "kb-broken")); err != nil {
		t.Fatal(err)
	}
	// The cache keeps its own bookkeeping beside the knowledge bases. Treating
	// one as a knowledge base publishes a directory that holds no shard.
	control := filepath.Join(loader.CacheDir, cacheevict.LeaseDirName)
	if err := MarkPendingSession(control); err != nil {
		t.Fatal(err)
	}

	reaped, err := loader.ReapAbandonedSessions(ctx)
	if err == nil {
		t.Fatal("a tick that published nothing was reported as clean")
	}
	if reaped != 0 {
		t.Fatalf("reaped %d knowledge bases while every publish failed", reaped)
	}
	if len(format.commits) != 1 || format.commits[0] != "kb-broken" {
		t.Fatalf("published %v, want only the knowledge base with rows", format.commits)
	}
	// The lease has to come back, or the next tick and every client are locked
	// out for the whole TTL.
	if _, err := loader.IngestSessionsFor().Hold(ctx, "kb-broken", ""); err != nil {
		t.Fatalf("a failed reap kept the session: %v", err)
	}
}

// Both leases are keyed by the same kbID. Without separate key spaces a
// compaction would report a client's ingest session as its own conflict.
func TestIngestSessionsDoNotCollideWithWriteLeases(t *testing.T) {
	ctx := context.Background()
	h, _, _ := newAsyncHarness(t, "lease-keys")
	loader := h.KB()

	held, err := loader.IngestSessionsFor().Hold(ctx, "kb", "")
	if err != nil {
		t.Fatal(err)
	}
	if held == "" {
		t.Fatal("no session handle was issued")
	}
	lease, err := loader.WriteLeaseManager.Acquire(ctx, "kb", time.Minute)
	if err != nil {
		t.Fatalf("an ingest session blocked an unrelated write lease: %v", err)
	}
	if err := loader.WriteLeaseManager.Release(ctx, lease); err != nil {
		t.Fatal(err)
	}
}

// The rows are durable by the time the release runs, so a release failure must
// not fail the commit: the caller would redo a whole run whose writes are safe.
func TestACommitSurvivesAFailedSessionRelease(t *testing.T) {
	ctx := context.Background()
	format := &committingFormat{}
	h, _, _ := newAsyncHarness(t, "commit-release-fails", WithArtifactFormat(format))
	loader := h.KB()
	format.cacheDir = loader.CacheDir
	if err := MarkPendingSession(filepath.Join(loader.CacheDir, "kb")); err != nil {
		t.Fatal(err)
	}

	worker := &SessionCommitWorker{
		KB: loader, ID: "test",
		ReleaseSession: func(context.Context, string, string) error {
			return errors.New("lease backend is down")
		},
	}
	payload, err := json.Marshal(SessionCommitPayload{KBID: "kb"})
	if err != nil {
		t.Fatal(err)
	}
	event := &KBEvent{
		EventID: "evt-commit", Kind: EventSessionCommit, KBID: "kb", Payload: payload, Attempt: 1,
	}

	result, err := worker.Handle(ctx, event)
	if err != nil {
		t.Fatalf("a failed release failed the commit: %v", err)
	}
	if len(result.FollowUps) != 1 || result.FollowUps[0].Kind != EventKBPublished {
		t.Fatalf("the publish was not reported: %+v", result.FollowUps)
	}
	if len(format.commits) != 1 {
		t.Fatalf("published %d times, want 1", len(format.commits))
	}
}

// The reaper is the only thing that clears an abandoned session.
func TestTheSchedulerRunsTheSessionReaper(t *testing.T) {
	ctx := context.Background()
	format := &committingFormat{failWith: errors.New("publish failed")}
	h, _, _ := newAsyncHarness(t, "reap-scheduled", WithArtifactFormat(format))
	loader := h.KB()
	format.cacheDir = loader.CacheDir
	if err := MarkPendingSession(filepath.Join(loader.CacheDir, "kb-wedged")); err != nil {
		t.Fatal(err)
	}

	s := NewScheduler(NewInMemoryWriteLeaseManager(), time.Minute, nil, nil)
	if err := loader.RegisterDefaultJobs(s); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(s.JobIDs(), SessionReapJobID) {
		t.Fatalf("the session reaper was not registered: %v", s.JobIDs())
	}

	// The publish fails, so the job has to surface that. A dispatch that never
	// reaches the reaper reports a clean run instead.
	if _, err := s.RunOnce(ctx, SessionReapJobID); err == nil {
		t.Fatal("the scheduled job did not reach the reaper")
	}
	if len(format.commits) != 1 {
		t.Fatalf("the scheduled job published %v", format.commits)
	}
}
