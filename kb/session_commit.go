package kb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// ErrNothingDeferred reports a commit for a knowledge base holding no
// unpublished rows, so the caller must not record the run as indexed.
var ErrNothingDeferred = errors.New("no deferred writes to publish")

// ErrSessionReplaced reports a commit whose session another run has taken over.
var ErrSessionReplaced = errors.New("ingest session was replaced by a later run")

const sessionCommitInlineScopeBytes = 1 << 20

var finalizedScopeRef = []byte(`{"finalized":true}`)

type SessionCommitPayload struct {
	KBID      string              `json:"kb_id"`
	SessionID string              `json:"session_id,omitempty"`
	Scope     *SessionCommitScope `json:"scope,omitempty"`
	ScopeRef  string              `json:"scope_ref,omitempty"`
	ScopeOnly bool                `json:"scope_only,omitempty"`
	finalized bool
}

// SessionCommitScope is an opaque document selection applied after the
// deferred rows are published and before the commit reports success.
type SessionCommitScope struct {
	ScopeID     string   `json:"scope_id"`
	DocumentIDs []string `json:"document_ids"`
	Revision    string   `json:"revision,omitempty"`
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
	payload.SessionID = strings.TrimSpace(payload.SessionID)
	if payload.SessionID == "" {
		return "", "", false, errors.New("commit: session_id required")
	}
	if payload.ScopeOnly && payload.Scope == nil {
		return "", "", false, errors.New("commit: scope-only commit requires scope")
	}
	if payload.Scope != nil {
		payload.Scope.ScopeID = strings.TrimSpace(payload.Scope.ScopeID)
		if payload.Scope.ScopeID == "" {
			return "", "", false, errors.New("commit: scope_id required")
		}
		payload.Scope.DocumentIDs = normalizeScopeIDs(payload.Scope.DocumentIDs)
	}
	idempotencyKey, correlationID = l.ensureEventKeys(idempotencyKey, correlationID)
	event := l.newRootPendingEvent(pendingEventInput{
		kind:           EventSessionCommit,
		kbID:           payload.KBID,
		schema:         "session.commit/v1",
		correlationID:  correlationID,
		idempotencyKey: idempotencyKey,
	})
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", "", false, fmt.Errorf("marshal payload: %w", err)
	}
	if len(encoded) > sessionCommitInlineScopeBytes && payload.Scope != nil {
		if l.BlobStore == nil {
			return "", "", false, errors.New("commit: BlobStore required for large scope")
		}
		scopeData, marshalErr := json.Marshal(payload.Scope)
		if marshalErr != nil {
			return "", "", false, fmt.Errorf("marshal scope: %w", marshalErr)
		}
		sum := sha256.Sum256(scopeData)
		payload.ScopeRef = fmt.Sprintf("session-commit-scopes/%s-%x.json", event.EventID, sum[:8])
		if uploadErr := l.storeCommitScope(ctx, payload.ScopeRef, scopeData); uploadErr != nil {
			return "", "", false, uploadErr
		}
		payload.Scope = nil
		encoded, err = json.Marshal(payload)
		if err != nil {
			return "", "", false, fmt.Errorf("marshal payload: %w", err)
		}
	}
	event.Payload = encoded
	if err := l.EventStore.Append(ctx, event); err != nil {
		existingID, effectiveKey, dupErr := l.handleAppendDuplicate(
			ctx, err, EventSessionCommit, payload.KBID, idempotencyKey,
		)
		if dupErr == nil && existingID != "" && payload.ScopeRef != "" {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if deleteErr := l.BlobStore.Delete(cleanupCtx, payload.ScopeRef); deleteErr != nil {
				slog.Default().WarnContext(cleanupCtx, "duplicate commit scope cleanup failed",
					logKeyKBID, payload.KBID, logKeyError, deleteErr)
			}
		}
		return existingID, effectiveKey, false, dupErr
	}
	return event.EventID, idempotencyKey, true, nil
}

func (l *KB) storeCommitScope(ctx context.Context, key string, data []byte) error {
	if _, err := l.BlobStore.UploadBytesIfMatch(ctx, key, data, ""); err == nil {
		return nil
	} else {
		existing, downloadErr := l.BlobStore.DownloadBytes(ctx, key)
		if downloadErr != nil {
			return fmt.Errorf("store commit scope: %w", err)
		}
		if bytes.Equal(existing, data) {
			return nil
		}
		if !bytes.Equal(existing, finalizedScopeRef) {
			return fmt.Errorf("store commit scope: %w", err)
		}
		info, headErr := l.BlobStore.Head(ctx, key)
		if headErr != nil {
			return fmt.Errorf("inspect commit scope: %w", headErr)
		}
		if _, replaceErr := l.BlobStore.UploadBytesIfMatch(ctx, key, data, info.Version); replaceErr != nil {
			return fmt.Errorf("restore commit scope: %w", replaceErr)
		}
		return nil
	}
}

// SessionCommitWorker publishes what a deferred ingest session left behind.
type SessionCommitWorker struct {
	KB *KB
	ID string
	// ReleaseSession frees the client's session once the rows are durable.
	ReleaseSession func(ctx context.Context, kbID, sessionID string) error
	FinalizeScope  func(ctx context.Context, kbID string, scope SessionCommitScope) error
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
	if payload.SessionID == "" {
		return WorkerResult{}, fmt.Errorf("session commit payload has no session")
	}
	if err := w.loadScope(ctx, &payload); err != nil {
		return WorkerResult{}, err
	}
	if payload.finalized {
		w.releaseSession(ctx, payload)
		return w.publishedResult(event, payload), nil
	}
	hasDeferred := w.hasDeferredWrites(payload.KBID)
	if w.publishedEarlier(payload.KBID, event.EventID) {
		if hasDeferred {
			owner, err := w.sessionOwner(ctx, payload)
			if err != nil {
				return WorkerResult{}, err
			}
			if owner == sessionSame {
				return w.publish(ctx, event, payload)
			}
		}
		err := w.finalizeScope(ctx, payload)
		w.releaseSession(ctx, payload)
		if err != nil {
			return WorkerResult{}, err
		}
		return w.publishedResult(event, payload), nil
	}
	if !hasDeferred {
		if payload.Scope != nil {
			owner, err := w.sessionOwner(ctx, payload)
			if err != nil {
				return WorkerResult{}, err
			}
			if owner == sessionOther {
				return WorkerResult{}, fmt.Errorf("%w for %s", ErrSessionReplaced, payload.KBID)
			}
			err = w.finalizeScope(ctx, payload)
			w.releaseSession(ctx, payload)
			if err != nil {
				return WorkerResult{}, err
			}
			return w.publishedResult(event, payload), nil
		}
		return WorkerResult{}, fmt.Errorf("%w for %s", ErrNothingDeferred, payload.KBID)
	}
	owner, err := w.sessionOwner(ctx, payload)
	if err != nil {
		return WorkerResult{}, err
	}
	if owner == sessionOther {
		return WorkerResult{}, fmt.Errorf("%w for %s", ErrSessionReplaced, payload.KBID)
	}
	return w.publish(ctx, event, payload)
}

func (w *SessionCommitWorker) publish(
	ctx context.Context,
	event *KBEvent,
	payload SessionCommitPayload,
) (WorkerResult, error) {
	if err := w.KB.CommitPreparedDocs(ctx, payload.KBID); err != nil {
		return WorkerResult{}, err
	}
	// Lets a redelivery tell a finished commit from one with nothing to do.
	if err := w.recordCommitted(payload.KBID, event.EventID); err != nil {
		slog.Default().WarnContext(ctx, "could not record a finished commit",
			logKeyKBID, payload.KBID, logKeyError, err)
	}
	err := w.finalizeScope(ctx, payload)
	w.releaseSession(ctx, payload)
	if err != nil {
		return WorkerResult{}, err
	}
	return w.publishedResult(event, payload), nil
}

func (w *SessionCommitWorker) loadScope(ctx context.Context, payload *SessionCommitPayload) error {
	if payload.Scope != nil || payload.ScopeRef == "" {
		return nil
	}
	if w.KB == nil || w.KB.BlobStore == nil {
		return errors.New("session commit scope blob store is not configured")
	}
	data, err := w.KB.BlobStore.DownloadBytes(ctx, payload.ScopeRef)
	if err != nil {
		return fmt.Errorf("load session commit scope: %w", err)
	}
	if bytes.Equal(data, finalizedScopeRef) {
		payload.finalized = true
		return nil
	}
	if err := json.Unmarshal(data, &payload.Scope); err != nil {
		return fmt.Errorf("decode session commit scope: %w", err)
	}
	if payload.Scope == nil {
		return errors.New("session commit scope blob is empty")
	}
	return nil
}

func (w *SessionCommitWorker) finalizeScope(ctx context.Context, payload SessionCommitPayload) error {
	if payload.Scope == nil {
		return nil
	}
	if w.FinalizeScope == nil {
		return errors.New("session commit scope finalizer is not configured")
	}
	if err := w.FinalizeScope(ctx, payload.KBID, *payload.Scope); err != nil {
		return fmt.Errorf("finalize session scope: %w", err)
	}
	if err := w.compactScopeRef(ctx, payload.ScopeRef); err != nil {
		return err
	}
	return nil
}

func (w *SessionCommitWorker) compactScopeRef(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	info, err := w.KB.BlobStore.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("inspect session commit scope: %w", err)
	}
	data, err := w.KB.BlobStore.DownloadBytes(ctx, key)
	if err != nil {
		return fmt.Errorf("load session commit scope: %w", err)
	}
	if bytes.Equal(data, finalizedScopeRef) {
		return nil
	}
	if _, err := w.KB.BlobStore.UploadBytesIfMatch(ctx, key, finalizedScopeRef, info.Version); err != nil {
		return fmt.Errorf("compact session commit scope: %w", err)
	}
	return nil
}

// FinalizeSessionScope applies a scope idempotently. A worker redelivery after
// a lost acknowledgement accepts the already-written document set.
func (k *KB) FinalizeSessionScope(ctx context.Context, kbID string, desired SessionCommitScope) error {
	_, err := k.ReplaceScope(ctx, kbID, desired.ScopeID, desired.DocumentIDs, desired.Revision)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrBlobVersionMismatch) {
		return err
	}
	current, getErr := k.GetScope(ctx, kbID, desired.ScopeID)
	if getErr != nil {
		return errors.Join(err, getErr)
	}
	if slices.Equal(current.DocumentIDs, normalizeScopeIDs(desired.DocumentIDs)) {
		return nil
	}
	return err
}

// releaseSession logs rather than fails, since the publish already happened
// and the caller would redo a run whose writes are safe.
func (w *SessionCommitWorker) releaseSession(ctx context.Context, payload SessionCommitPayload) {
	if w.ReleaseSession == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := w.ReleaseSession(cleanupCtx, payload.KBID, payload.SessionID); err != nil {
		slog.Default().WarnContext(cleanupCtx, "ingest session release failed after commit",
			logKeyKBID, payload.KBID, logKeyError, err)
	}
}

type sessionOwnership uint8

const (
	sessionNone sessionOwnership = iota
	sessionSame
	sessionOther
)

func (w *SessionCommitWorker) sessionOwner(ctx context.Context, payload SessionCommitPayload) (sessionOwnership, error) {
	if w.KB == nil {
		return sessionNone, errors.New("session commit worker has no knowledge base")
	}
	_, token, ok := strings.Cut(strings.TrimSpace(payload.SessionID), ":")
	if !ok || token == "" {
		return sessionOther, nil
	}
	current, err := w.KB.IngestSessionsFor().Peek(ctx, payload.KBID)
	if err != nil {
		return sessionNone, fmt.Errorf("check ingest session: %w", err)
	}
	if current == nil {
		return sessionNone, nil
	}
	if current.Token == token {
		return sessionSame, nil
	}
	return sessionOther, nil
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

func (l *KB) SweepSessionCommitScopes(ctx context.Context, before time.Time) (int, error) {
	if l.BlobStore == nil || l.EventStore == nil {
		return 0, nil
	}
	objects, err := l.BlobStore.List(ctx, "session-commit-scopes/")
	if err != nil {
		return 0, err
	}
	removed := 0
	var sweepErr error
	for _, object := range objects {
		if !before.IsZero() && !object.UpdatedAt.Before(before) {
			continue
		}
		name := strings.TrimSuffix(filepath.Base(object.Key), ".json")
		separator := strings.LastIndexByte(name, '-')
		if separator <= 0 {
			continue
		}
		event, getErr := l.EventStore.Get(ctx, name[:separator])
		if getErr != nil && !errors.Is(getErr, ErrEventNotFound) {
			sweepErr = errors.Join(sweepErr, getErr)
			continue
		}
		if getErr == nil && event.Status != EventStatusDone && event.Status != EventStatusDead {
			continue
		}
		if deleteErr := l.BlobStore.Delete(ctx, object.Key); deleteErr != nil {
			sweepErr = errors.Join(sweepErr, deleteErr)
			continue
		}
		removed++
	}
	return removed, sweepErr
}
