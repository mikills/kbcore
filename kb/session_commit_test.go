package kb

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type peekErrorManager struct{ WriteLeaseManager }

func (peekErrorManager) Peek(context.Context, string) (*WriteLease, error) {
	return nil, errors.New("lease backend unavailable")
}

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
		_, err := worker.Handle(context.Background(), newEvent(t, SessionCommitPayload{
			KBID: "kb", SessionID: "instance:token",
		}))
		if !errors.Is(err, ErrNothingDeferred) {
			t.Fatalf("an empty commit was not reported as a failure: %v", err)
		}
	})

	t.Run("a retry of an empty commit still fails", func(t *testing.T) {
		loader := &KB{CacheDir: t.TempDir(), Clock: RealClock}
		worker := &SessionCommitWorker{KB: loader, ID: "test"}
		event := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "instance:token"})
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
		event := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "instance:token"})
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
		other := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "instance:token"})
		other.EventID = "evt-2"
		if _, err := worker.Handle(context.Background(), other); !errors.Is(err, ErrNothingDeferred) {
			t.Fatalf("another commit inherited this one's publish: %v", err)
		}
	})

	t.Run("a cleared marker can still finalize its scope", func(t *testing.T) {
		loader := &KB{CacheDir: t.TempDir(), Clock: RealClock}
		event := newEvent(t, SessionCommitPayload{
			KBID: "kb", SessionID: "instance:expired",
			Scope: &SessionCommitScope{ScopeID: "main", DocumentIDs: []string{"a"}},
		})
		called := false
		worker := &SessionCommitWorker{
			KB: loader, ID: "test",
			FinalizeScope: func(context.Context, string, SessionCommitScope) error {
				called = true
				return nil
			},
		}

		result, err := worker.Handle(context.Background(), event)
		require.NoError(t, err)
		require.True(t, called)
		require.Len(t, result.FollowUps, 1)
	})

	t.Run("a redelivery finishes its scope before reporting success", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		event := newEvent(t, SessionCommitPayload{
			KBID:      "kb",
			SessionID: "instance:old",
			Scope:     &SessionCommitScope{ScopeID: "main", DocumentIDs: []string{"b", "a"}},
		})
		require.NoError(t, RecordSessionCommit(filepath.Join(cache, "kb"), event.EventID))
		_, err := loader.IngestSessionsFor().Hold(context.Background(), "kb", "")
		require.NoError(t, err)
		require.NoError(t, MarkPendingSession(filepath.Join(cache, "kb")))

		called := false
		worker := &SessionCommitWorker{
			KB: loader,
			ID: "test",
			FinalizeScope: func(_ context.Context, kbID string, scope SessionCommitScope) error {
				called = true
				require.Equal(t, "kb", kbID)
				require.Equal(t, "main", scope.ScopeID)
				require.Equal(t, []string{"b", "a"}, scope.DocumentIDs)
				return nil
			},
		}
		result, err := worker.Handle(context.Background(), event)
		require.NoError(t, err)
		require.True(t, called)
		require.Len(t, result.FollowUps, 1)
		require.Equal(t, EventKBPublished, result.FollowUps[0].Kind)
	})

	t.Run("a redelivery leaves unowned pending rows alone", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		event := newEvent(t, SessionCommitPayload{
			KBID: "kb", SessionID: "instance:expired",
			Scope: &SessionCommitScope{ScopeID: "main", DocumentIDs: []string{"old"}},
		})
		require.NoError(t, RecordSessionCommit(filepath.Join(cache, "kb"), event.EventID))
		require.NoError(t, MarkPendingSession(filepath.Join(cache, "kb")))
		worker := &SessionCommitWorker{
			KB: loader, ID: "test",
			FinalizeScope: func(context.Context, string, SessionCommitScope) error { return nil },
		}

		_, err := worker.Handle(context.Background(), event)
		require.NoError(t, err)
		require.True(t, HasPendingSession(filepath.Join(cache, "kb")))
	})

	t.Run("a lease lookup error keeps pending rows retryable", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{
			CacheDir: cache, Clock: RealClock,
			WriteLeaseManager: peekErrorManager{WriteLeaseManager: NewInMemoryWriteLeaseManager()},
		}
		event := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "instance:token"})
		require.NoError(t, MarkPendingSession(filepath.Join(cache, "kb")))

		_, err := (&SessionCommitWorker{KB: loader, ID: "test"}).Handle(context.Background(), event)
		require.ErrorContains(t, err, "lease backend unavailable")
		require.True(t, HasPendingSession(filepath.Join(cache, "kb")))
	})

	t.Run("a scope failure keeps the commit retryable", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		event := newEvent(t, SessionCommitPayload{
			KBID:      "kb",
			SessionID: "instance:token",
			Scope:     &SessionCommitScope{ScopeID: "main", DocumentIDs: []string{"a"}},
		})
		require.NoError(t, RecordSessionCommit(filepath.Join(cache, "kb"), event.EventID))
		released := false
		worker := &SessionCommitWorker{
			KB: loader, ID: "test",
			ReleaseSession: func(context.Context, string, string) error {
				released = true
				return nil
			},
			FinalizeScope: func(context.Context, string, SessionCommitScope) error {
				return errors.New("scope unavailable")
			},
		}
		_, err := worker.Handle(context.Background(), event)
		require.ErrorContains(t, err, "finalize session scope")
		require.True(t, released)
	})

	t.Run("session release outlives the worker context", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		event := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "instance:token"})
		require.NoError(t, RecordSessionCommit(filepath.Join(cache, "kb"), event.EventID))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		worker := &SessionCommitWorker{
			KB: loader, ID: "test",
			ReleaseSession: func(ctx context.Context, _, _ string) error {
				require.NoError(t, ctx.Err())
				return nil
			},
		}

		_, err := worker.Handle(ctx, event)
		require.NoError(t, err)
	})

	t.Run("a scope-only commit needs no rows", func(t *testing.T) {
		loader := &KB{CacheDir: t.TempDir(), Clock: RealClock}
		handle, err := loader.IngestSessionsFor().Hold(context.Background(), "kb", "")
		require.NoError(t, err)
		event := newEvent(t, SessionCommitPayload{
			KBID:      "kb",
			SessionID: handle,
			Scope:     &SessionCommitScope{ScopeID: "feature", DocumentIDs: []string{"a"}},
			ScopeOnly: true,
		})
		called := false
		worker := &SessionCommitWorker{
			KB: loader, ID: "test",
			FinalizeScope: func(context.Context, string, SessionCommitScope) error {
				called = true
				return nil
			},
		}
		result, err := worker.Handle(context.Background(), event)
		require.NoError(t, err)
		require.True(t, called)
		require.Len(t, result.FollowUps, 1)
	})

	t.Run("a stale scope commit is rejected", func(t *testing.T) {
		cache := t.TempDir()
		loader := &KB{CacheDir: cache, Clock: RealClock}
		_, err := loader.IngestSessionsFor().Hold(context.Background(), "kb", "")
		require.NoError(t, err)
		event := newEvent(t, SessionCommitPayload{
			KBID:      "kb",
			SessionID: "instance:stale",
			Scope:     &SessionCommitScope{ScopeID: "feature", DocumentIDs: []string{"a"}},
			ScopeOnly: true,
		})
		called := false
		worker := &SessionCommitWorker{
			KB: loader, ID: "test",
			FinalizeScope: func(context.Context, string, SessionCommitScope) error {
				called = true
				return nil
			},
		}
		_, err = worker.Handle(context.Background(), event)
		require.ErrorIs(t, err, ErrSessionReplaced)
		require.False(t, called)
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
		event := newEvent(t, SessionCommitPayload{KBID: "kb", SessionID: "instance:token"})
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

func TestLargeCommitPayload(t *testing.T) {
	store := NewInMemoryEventStore()
	blobs := &LocalBlobStore{Root: t.TempDir()}
	loader := &KB{
		EventStore: store, BlobStore: blobs,
		CacheDir: t.TempDir(), Clock: RealClock,
	}
	handle, err := loader.IngestSessionsFor().Hold(context.Background(), "kb", "")
	require.NoError(t, err)
	largeID := strings.Repeat("x", sessionCommitInlineScopeBytes+1)
	eventID, _, _, err := loader.AppendSessionCommit(context.Background(), SessionCommitPayload{
		KBID: "kb", SessionID: handle, ScopeOnly: true,
		Scope: &SessionCommitScope{ScopeID: "main", DocumentIDs: []string{largeID}},
	}, "key", "correlation")
	require.NoError(t, err)

	event, err := store.Get(context.Background(), eventID)
	require.NoError(t, err)
	require.Less(t, len(event.Payload), sessionCommitInlineScopeBytes)
	var payload SessionCommitPayload
	require.NoError(t, json.Unmarshal(event.Payload, &payload))
	require.Nil(t, payload.Scope)
	require.NotEmpty(t, payload.ScopeRef)

	duplicateID, _, created, err := loader.AppendSessionCommit(context.Background(), SessionCommitPayload{
		KBID: "kb", SessionID: handle, ScopeOnly: true,
		Scope: &SessionCommitScope{ScopeID: "main", DocumentIDs: []string{largeID}},
	}, "key", "correlation")
	require.NoError(t, err)
	require.Equal(t, eventID, duplicateID)
	require.False(t, created)
	objects, err := blobs.List(context.Background(), "session-commit-scopes/")
	require.NoError(t, err)
	require.Len(t, objects, 1)

	worker := &SessionCommitWorker{
		KB: loader, ID: "test",
		FinalizeScope: func(_ context.Context, _ string, scope SessionCommitScope) error {
			require.Equal(t, []string{largeID}, scope.DocumentIDs)
			return nil
		},
	}
	_, err = worker.Handle(context.Background(), event)
	require.NoError(t, err)
	data, err := blobs.DownloadBytes(context.Background(), payload.ScopeRef)
	require.NoError(t, err)
	require.Equal(t, finalizedScopeRef, data)
	result, err := worker.Handle(context.Background(), event)
	require.NoError(t, err)
	require.Len(t, result.FollowUps, 1)

	claimed, err := store.Claim(context.Background(), EventSessionCommit, "test", time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.Ack(context.Background(), claimed.EventID))
	removed, err := loader.SweepSessionCommitScopes(context.Background(), time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, removed)
}

func TestSessionPending(t *testing.T) {
	loader := &KB{CacheDir: t.TempDir(), Clock: RealClock}
	handle, err := loader.IngestSessionsFor().Hold(context.Background(), "kb", "")
	require.NoError(t, err)
	require.NoError(t, MarkPendingSession(filepath.Join(loader.CacheDir, "kb")))

	pending, err := loader.SessionPending(context.Background(), "kb", handle)
	require.NoError(t, err)
	require.True(t, pending)
	pending, err = loader.SessionPending(context.Background(), "kb", "instance:other")
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, loader.IngestSessionsFor().Release(context.Background(), "kb", handle))
	pending, err = loader.SessionPending(context.Background(), "kb", handle)
	require.NoError(t, err)
	require.True(t, pending)
}
