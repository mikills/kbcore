package kb

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestSessionCommitWorker(t *testing.T) {
	newEvent := func(t *testing.T, payload SessionCommitPayload) *KBEvent {
		t.Helper()
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return &KBEvent{EventID: "evt-1", Kind: EventSessionCommit, KBID: payload.KBID, Payload: encoded}
	}

	t.Run("a commit with nothing deferred fails instead of reporting success", func(t *testing.T) {
		loader := &KB{CacheDir: t.TempDir(), Clock: RealClock}
		worker := &SessionCommitWorker{KB: loader, ID: "test"}
		// Reporting success would let the caller record every document of the
		// run as indexed and never send them again.
		_, err := worker.Handle(context.Background(), newEvent(t, SessionCommitPayload{KBID: "kb"}))
		if !errors.Is(err, ErrNothingDeferred) {
			t.Fatalf("an empty commit was not reported as a failure: %v", err)
		}
	})

	t.Run("a retry of an empty commit still fails", func(t *testing.T) {
		loader := &KB{CacheDir: t.TempDir(), Clock: RealClock}
		worker := &SessionCommitWorker{KB: loader, ID: "test"}
		event := newEvent(t, SessionCommitPayload{KBID: "kb"})
		event.Attempt = 3
		// A later attempt is not evidence that an earlier one published. Taking
		// it as such turns every dead-lettering commit into a reported success.
		if _, err := worker.Handle(context.Background(), event); !errors.Is(err, ErrNothingDeferred) {
			t.Fatalf("a retry reported success for a commit that published nothing: %v", err)
		}
	})

	t.Run("a redelivery of a finished commit reports it published", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		worker := &SessionCommitWorker{KB: loader, ID: "test"}
		event := newEvent(t, SessionCommitPayload{KBID: "kb"})
		// What a successful publish leaves behind: the marker cleared and the
		// commit that cleared it recorded.
		if err := RecordSessionCommit(filepath.Join(cache, "kb"), event.EventID); err != nil {
			t.Fatal(err)
		}

		result, err := worker.Handle(context.Background(), event)
		if err != nil {
			t.Fatalf("a redelivery of a finished commit failed: %v", err)
		}
		if len(result.FollowUps) != 1 || result.FollowUps[0].Kind != EventKBPublished {
			t.Fatalf("no publish was reported: %+v", result.FollowUps)
		}
		// A different commit for the same knowledge base is not this one.
		other := newEvent(t, SessionCommitPayload{KBID: "kb"})
		other.EventID = "evt-2"
		if _, err := worker.Handle(context.Background(), other); !errors.Is(err, ErrNothingDeferred) {
			t.Fatalf("another commit inherited this one's publish: %v", err)
		}
	})

	t.Run("a commit whose session was taken over publishes nothing", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		kbDir := filepath.Join(cache, "kb")
		if err := MarkPendingSession(kbDir); err != nil {
			t.Fatal(err)
		}
		held, err := loader.IngestSessionsFor().Hold(context.Background(), "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		_ = held

		worker := &SessionCommitWorker{KB: loader, ID: "test"}
		event := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "someone:else"})
		// Publishing here would hand the current run a half-written shard, and
		// reporting success would tell this caller its rows are searchable.
		if _, err := worker.Handle(context.Background(), event); !errors.Is(err, ErrSessionReplaced) {
			t.Fatalf("a replaced session was committed anyway: %v", err)
		}
		if !HasPendingSession(kbDir) {
			t.Fatal("a skipped commit cleared the marker guarding the rows")
		}
	})

	t.Run("an event for another kind is rejected", func(t *testing.T) {
		worker := &SessionCommitWorker{KB: &KB{CacheDir: t.TempDir()}, ID: "test"}
		event := newEvent(t, SessionCommitPayload{KBID: "kb"})
		event.Kind = EventDocumentUpsert
		if _, err := worker.Handle(context.Background(), event); err == nil {
			t.Fatal("the worker accepted an unrelated event")
		}
	})

	t.Run("a payload without a knowledge base is rejected", func(t *testing.T) {
		worker := &SessionCommitWorker{KB: &KB{CacheDir: t.TempDir()}, ID: "test"}
		if _, err := worker.Handle(context.Background(), newEvent(t, SessionCommitPayload{})); err == nil {
			t.Fatal("the worker accepted a payload with no kb_id")
		}
	})
}
