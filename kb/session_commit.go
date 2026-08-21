package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
)

// ErrNothingDeferred reports a commit for a knowledge base holding no
// unpublished rows, so the caller must not record the run as indexed.
var ErrNothingDeferred = errors.New("no deferred writes to publish")

// ErrSessionReplaced reports a commit whose session another run has taken over.
var ErrSessionReplaced = errors.New("ingest session was replaced by a later run")

type SessionCommitPayload struct {
	KBID      string `json:"kb_id"`
	SessionID string `json:"session_id,omitempty"`
}

// AppendSessionCommit queues the publish, which outlasts any proxy's read
// timeout if run inline.
func (l *KB) AppendSessionCommit(
	ctx context.Context,
	payload SessionCommitPayload,
	idempotencyKey, correlationID string,
) (string, string, bool, error) {
	if l.EventStore == nil {
		return "", "", false, errors.New("commit: EventStore not configured")
	}
	payload.KBID = strings.TrimSpace(payload.KBID)
	if payload.KBID == "" {
		return "", "", false, errors.New("commit: kb_id required")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", "", false, fmt.Errorf("marshal payload: %w", err)
	}
	idempotencyKey, correlationID = l.ensureEventKeys(idempotencyKey, correlationID)
	event := l.newRootPendingEvent(pendingEventInput{
		kind:           EventSessionCommit,
		kbID:           payload.KBID,
		schema:         "session.commit/v1",
		correlationID:  correlationID,
		idempotencyKey: idempotencyKey,
		payload:        encoded,
	})
	if err := l.EventStore.Append(ctx, event); err != nil {
		existingID, effectiveKey, dupErr := l.handleAppendDuplicate(
			ctx, err, EventSessionCommit, payload.KBID, idempotencyKey,
		)
		return existingID, effectiveKey, false, dupErr
	}
	return event.EventID, idempotencyKey, true, nil
}

// SessionCommitWorker publishes what a deferred ingest session left behind.
type SessionCommitWorker struct {
	KB *KB
	ID string
	// ReleaseSession frees the client's session once the rows are durable.
	ReleaseSession func(ctx context.Context, kbID, sessionID string) error
}

func (w *SessionCommitWorker) Kind() EventKind  { return EventSessionCommit }
func (w *SessionCommitWorker) WorkerID() string { return w.ID }

func (w *SessionCommitWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	if event == nil || event.Kind != EventSessionCommit {
		return WorkerResult{}, fmt.Errorf("session commit worker: unexpected event kind")
	}
	var payload SessionCommitPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode session commit payload: %w", err)
	}
	if payload.KBID == "" {
		return WorkerResult{}, fmt.Errorf("session commit payload has no kb_id")
	}
	switch {
	case !w.hasDeferredWrites(payload.KBID):
		if w.publishedEarlier(payload.KBID, event.EventID) {
			// A redelivery of a commit that already published.
			w.releaseSession(ctx, payload)
			return w.publishedResult(event, payload), nil
		}
		// A failure, not an empty success. The caller is about to record every
		// document of the run as indexed and would never re-send them.
		return WorkerResult{}, fmt.Errorf("%w for %s", ErrNothingDeferred, payload.KBID)
	case w.sessionMovedOn(ctx, payload):
		// A newer run owns these rows and will commit them itself.
		return WorkerResult{}, fmt.Errorf("%w for %s", ErrSessionReplaced, payload.KBID)
	}
	if err := w.KB.CommitPreparedDocs(ctx, payload.KBID); err != nil {
		return WorkerResult{}, err
	}
	// Lets a redelivery tell a finished commit from one with nothing to do.
	if err := w.recordCommitted(payload.KBID, event.EventID); err != nil {
		slog.Default().WarnContext(ctx, "could not record a finished commit",
			logKeyKBID, payload.KBID, logKeyError, err)
	}
	w.releaseSession(ctx, payload)
	return w.publishedResult(event, payload), nil
}

// releaseSession logs rather than fails, since the publish already happened
// and the caller would redo a run whose writes are safe.
func (w *SessionCommitWorker) releaseSession(ctx context.Context, payload SessionCommitPayload) {
	if w.ReleaseSession == nil {
		return
	}
	if err := w.ReleaseSession(ctx, payload.KBID, payload.SessionID); err != nil {
		slog.Default().WarnContext(ctx, "ingest session release failed after commit",
			logKeyKBID, payload.KBID, logKeyError, err)
	}
}

// sessionMovedOn compares tokens, which a renewal keeps and a takeover
// changes. Nothing holding the key means the session lapsed, not that it moved.
func (w *SessionCommitWorker) sessionMovedOn(ctx context.Context, payload SessionCommitPayload) bool {
	if w.KB == nil {
		return false
	}
	_, token, ok := strings.Cut(strings.TrimSpace(payload.SessionID), ":")
	if !ok || token == "" {
		return false
	}
	current, err := w.KB.IngestSessionsFor().Peek(ctx, payload.KBID)
	if err != nil || current == nil {
		return false
	}
	return current.Token != token
}

func (w *SessionCommitWorker) recordCommitted(kbID, commitID string) error {
	if w.KB == nil || w.KB.CacheDir == "" {
		return nil
	}
	return RecordSessionCommit(filepath.Join(w.KB.CacheDir, kbID), commitID)
}

// publishedEarlier reports whether this same commit already published, which
// the cleared marker alone cannot say.
func (w *SessionCommitWorker) publishedEarlier(kbID, commitID string) bool {
	if w.KB == nil || w.KB.CacheDir == "" {
		return false
	}
	return SessionCommitted(filepath.Join(w.KB.CacheDir, kbID), commitID)
}

func (w *SessionCommitWorker) publishedResult(event *KBEvent, payload SessionCommitPayload) WorkerResult {
	pubPayload, _ := json.Marshal(KBPublishedPayload{KBID: payload.KBID, SourceEventID: event.EventID})
	pub := w.KB.newChildDoneEvent(
		event, EventKBPublished, "kb.published/v1", event.EventID+"|kb.published", pubPayload,
	)
	return WorkerResult{FollowUps: []KBEvent{pub}}
}

// hasDeferredWrites reports whether this instance holds the session's rows.
func (w *SessionCommitWorker) hasDeferredWrites(kbID string) bool {
	if w.KB == nil || w.KB.CacheDir == "" {
		return false
	}
	return HasPendingSession(filepath.Join(w.KB.CacheDir, kbID))
}
