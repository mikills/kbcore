package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mediapkg "github.com/mikills/minnow/kb/media"
	"log/slog"
	"os"
	"time"
)

const MediaUploadWorkerVisibilityTimeout = mediapkg.UploadWorkerVisibilityTimeout

type MediaUploadPayload = mediapkg.UploadPayload
type MediaUploadedPayload = mediapkg.UploadedPayload

func mediaStagingBlobKey(kbID, eventID, filename string) string {
	return mediapkg.StagingBlobKey(kbID, eventID, filename)
}

func (l *KB) AppendMediaUploadDetailed(
	ctx context.Context,
	input MediaUploadInput,
	maxBytes int64,
	idempotencyKey, correlationID string,
) (string, string, error) {
	cleanName, contentType, maxBytes, err := l.validateMediaUploadInput(input, maxBytes)
	if err != nil {
		return "", "", err
	}
	idempotencyKey, correlationID = l.ensureMediaUploadKeys(idempotencyKey, correlationID)
	staged, err := l.stageMediaUpload(ctx, input, cleanName, contentType, maxBytes)
	if err != nil {
		return "", "", err
	}
	defer staged.Cleanup()
	event, err := l.buildMediaUploadEvent(input, staged, idempotencyKey, correlationID)
	if err != nil {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, staged.BlobKey, "marshal payload failed")
		return "", "", err
	}
	return l.appendMediaUploadEvent(ctx, event, input.KBID, idempotencyKey, staged.BlobKey)
}

type stagedMediaUpload = mediapkg.StagedAsyncUpload

func (l *KB) validateMediaUploadInput(input MediaUploadInput, maxBytes int64) (string, string, int64, error) {
	return mediapkg.ValidateAsyncUploadInput(
		input,
		maxBytes,
		mediapkg.AsyncValidationConfig{
			HasEventStore:        l.EventStore != nil,
			HasBlobStore:         l.BlobStore != nil,
			ContentTypeAllowlist: l.MediaContentTypeAllowlist,
		},
	)
}

func (l *KB) ensureMediaUploadKeys(idempotencyKey, correlationID string) (string, string) {
	if idempotencyKey == "" {
		idempotencyKey = l.NewULIDLike("idem")
	}
	if correlationID == "" {
		correlationID = l.NewULIDLike("corr")
	}
	return idempotencyKey, correlationID
}

func (l *KB) stageMediaUpload(
	ctx context.Context,
	input MediaUploadInput,
	cleanName string,
	contentType string,
	maxBytes int64,
) (stagedMediaUpload, error) {
	return mediapkg.StageAsyncUpload(
		ctx,
		input,
		mediapkg.StageAsyncConfig{
			BlobStore:   l.BlobStore,
			NewEventID:  func() string { return l.NewULIDLike("evt") },
			NewMediaID:  l.newMediaID,
			CleanName:   cleanName,
			ContentType: contentType,
			MaxBytes:    maxBytes,
		},
	)
}

func (l *KB) buildMediaUploadEvent(
	input MediaUploadInput,
	staged stagedMediaUpload,
	idempotencyKey, correlationID string,
) (KBEvent, error) {
	payloadBytes, err := json.Marshal(mediapkg.UploadPayloadFromInput(input, staged, idempotencyKey, correlationID))
	if err != nil {
		return KBEvent{}, fmt.Errorf("media: marshal payload: %w", err)
	}
	return l.newRootPendingEvent(
		pendingEventInput{
			kind:           EventMediaUpload,
			kbID:           input.KBID,
			schema:         "media.upload/v1",
			correlationID:  correlationID,
			idempotencyKey: idempotencyKey,
			payload:        payloadBytes,
		},
	), nil
}

func (l *KB) appendMediaUploadEvent(
	ctx context.Context,
	event KBEvent,
	kbID string,
	idempotencyKey string,
	stagedBlobKey string,
) (string, string, error) {
	if err := l.EventStore.Append(ctx, event); err != nil {
		if errors.Is(err, ErrEventDuplicateKey) {
			return l.handleDuplicateMediaUpload(ctx, kbID, idempotencyKey, stagedBlobKey)
		}
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "event append failed")
		return "", "", err
	}
	return event.EventID, idempotencyKey, nil
}

func (l *KB) handleDuplicateMediaUpload(
	ctx context.Context,
	kbID string,
	idempotencyKey string,
	stagedBlobKey string,
) (string, string, error) {
	existingID, ok, err := l.findDuplicateMediaUpload(ctx, kbID, idempotencyKey)
	if err != nil {
		return "", "", err
	}
	if ok {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "duplicate idempotency key")
		return existingID, idempotencyKey, nil
	}
	bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "event append failed")
	return "", "", ErrEventDuplicateKey
}

func (l *KB) findDuplicateMediaUpload(ctx context.Context, kbID string, idempotencyKey string) (string, bool, error) {
	existing, err := l.EventStore.FindByIdempotency(ctx, EventMediaUpload, kbID, idempotencyKey)
	if err != nil {
		return "", false, err
	}
	if existing == nil {
		return "", false, nil
	}
	return existing.EventID, true, nil
}

// bestEffortDeleteStagedBlob removes a staged blob, treating NotFound as
// success and logging any other error. Cascade-cleanup paths use this so a
// failed delete does not mask the primary error, but operators still see the
// potential leak in logs.
func bestEffortDeleteStagedBlob(ctx context.Context, store BlobStore, blobKey, reason string) {
	if store == nil || blobKey == "" {
		return
	}
	err := store.Delete(ctx, blobKey)
	if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
		return
	}
	slog.Default().Warn("media: staged blob cleanup failed",
		"blob_key", blobKey, logKeyReason, reason, logKeyError, err)
}

type MediaUploadWorker struct {
	KB *KB
	ID string
}

func (w *MediaUploadWorker) Kind() EventKind  { return EventMediaUpload }
func (w *MediaUploadWorker) WorkerID() string { return w.ID }

func (w *MediaUploadWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	if w.KB.MediaStore == nil || w.KB.BlobStore == nil {
		return WorkerResult{}, errors.New("media worker: media subsystem not configured")
	}
	var payload MediaUploadPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	cleanup := mediaUploadCommit(w.KB.BlobStore, payload.StagedBlobKey)
	if err := w.ensureMediaObject(ctx, payload); err != nil {
		return WorkerResult{AfterTerminal: cleanup}, err
	}
	mediaUploadedPayload, err := json.Marshal(mediapkg.UploadedPayloadFromUpload(payload, event.EventID))
	if err != nil {
		return WorkerResult{}, fmt.Errorf("marshal media.uploaded: %w", err)
	}
	child := w.KB.newChildDoneEvent(
		event,
		EventMediaUploaded,
		"media.uploaded/v1",
		event.EventID+"|media.uploaded",
		mediaUploadedPayload,
	)
	cleanupPayload, _ := json.Marshal(StagingCleanupPayload{Keys: []string{payload.StagedBlobKey}})
	cleanupEvent := w.KB.newChildPendingEvent(
		event, EventStagingCleanup, "staging.cleanup/v1", event.EventID+"|staging.cleanup", cleanupPayload,
	)
	return WorkerResult{FollowUps: []KBEvent{child, cleanupEvent}}, nil
}

func (w *MediaUploadWorker) ensureMediaObject(ctx context.Context, payload MediaUploadPayload) error {
	if _, err := w.KB.MediaStore.Get(ctx, payload.MediaID); err != nil {
		if !errors.Is(err, ErrMediaNotFound) {
			return err
		}
		return w.KB.persistStagedMediaObject(ctx, payload.StagedBlobKey, mediapkg.ObjectFromUploadPayload(payload))
	}
	return nil
}

func mediaUploadCommit(store BlobStore, stagedBlobKey string) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := deleteStagedBlobWithRetry(ctx, store, stagedBlobKey); err != nil {
			return fmt.Errorf("media: delete staged blob %q: %w", stagedBlobKey, err)
		}
		return nil
	}
}

// mediaSweepPageSize bounds each ListUnfinishedBefore call so the sweep
// does not materialise a large backlog in memory.
const mediaSweepPageSize = 500

func (l *KB) SweepAbortedMediaUploads(ctx context.Context, now time.Time) (int, error) {
	if l.EventStore == nil || l.BlobStore == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = l.Clock.Now()
	}
	cutoff := now.Add(-l.MediaGCConfigOrDefault().UploadCompletion)
	count := 0
	var lastErr error
	token := ""
	for {
		events, next, err := l.EventStore.ListUnfinishedBefore(ctx, EventMediaUpload, cutoff, token, mediaSweepPageSize)
		if err != nil {
			return count, err
		}
		for _, event := range events {
			marked, err := l.sweepAbortedMediaUpload(ctx, event)
			if err != nil {
				lastErr = err
			}
			if marked {
				count++
			}
		}
		if next == "" {
			return count, lastErr
		}
		token = next
	}
}

func (l *KB) sweepAbortedMediaUpload(ctx context.Context, event KBEvent) (bool, error) {
	child, err := l.EventStore.FindByCausation(ctx, EventMediaUploaded, event.EventID)
	if err != nil {
		slog.Default().Warn("media-sweep: FindByCausation failed", logKeyEventID, event.EventID, logKeyError, err)
		return false, err
	}
	if child != nil {
		return false, nil
	}
	l.cleanupAbortedMediaUploadBlob(ctx, event)
	if err := l.EventStore.Fail(ctx, event.EventID, event.Attempt, "media upload completion timeout"); err != nil {
		return false, mediaSweepFailError(event.EventID, err)
	}
	return true, nil
}

func (l *KB) cleanupAbortedMediaUploadBlob(ctx context.Context, event KBEvent) {
	var payload MediaUploadPayload
	if err := json.Unmarshal(event.Payload, &payload); err == nil {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, payload.StagedBlobKey, "sweep aborted upload")
	} else {
		slog.Default().Warn("media-sweep: decode payload failed", logKeyEventID, event.EventID, logKeyError, err)
	}
}

func mediaSweepFailError(eventID string, err error) error {
	if errors.Is(err, ErrEventStateChanged) {
		slog.Default().
			Info("media-sweep: event state changed since observation; another worker updated it", logKeyEventID, eventID)
		return nil
	}
	slog.Default().Warn("media-sweep: EventStore.Fail failed", logKeyEventID, eventID, logKeyError, err)
	return err
}

func (l *KB) UploadMedia(ctx context.Context, input MediaUploadInput, maxBytes int64) (*MediaUploadResult, error) {
	return mediapkg.Upload(ctx, input, maxBytes, mediapkg.UploadConfig{
		BlobStore:            l.BlobStore,
		MediaStore:           l.MediaStore,
		Now:                  l.Clock.Now,
		NewMediaID:           l.newMediaID,
		ContentTypeAllowlist: l.MediaContentTypeAllowlist,
		DuplicateKeyErr:      ErrMediaDuplicateKey,
		DeleteBlobOnFailureNote: func(ctx context.Context, store mediapkg.BlobStore, key string, reason string) {
			bestEffortDeleteStagedBlob(ctx, l.BlobStore, key, reason)
		},
	})
}

func (l *KB) ValidateDocumentReferences(ctx context.Context, kbID string, docs []Document) error {
	refs := make([]mediapkg.DocumentRefs, 0, len(docs))
	for _, doc := range docs {
		refs = append(refs, mediapkg.DocumentRefs{MediaIDs: doc.MediaIDs, MediaRefs: doc.MediaRefs})
	}
	return mediapkg.ValidateDocumentReferences(ctx, l.MediaStore, kbID, refs)
}

const (
	DefaultPendingMediaTTL     = mediapkg.DefaultPendingTTL
	DefaultMediaTombstoneGrace = mediapkg.DefaultTombstoneGrace
	DefaultUploadCompletionTTL = mediapkg.DefaultUploadCompletion
)

type MediaGCConfig = mediapkg.GCConfig
type MediaGCResult = mediapkg.GCResult

func (l *KB) MediaGCConfigOrDefault() MediaGCConfig { return l.MediaGC.Defaults() }

func (l *KB) iterMediaByState(ctx context.Context, state MediaState, visit func(MediaObject) error) error {
	return mediapkg.IterByState(ctx, l.MediaStore, state, visit)
}

func (l *KB) SweepMediaGCMark(ctx context.Context, now time.Time) (MediaGCResult, error) {
	if now.IsZero() {
		now = l.Clock.Now()
	}
	return mediapkg.SweepGCMark(ctx, l.MediaStore, now, l.MediaGC, l.liveMediaRefsForKB)
}

func (l *KB) SweepMediaGCDelete(ctx context.Context, now time.Time) (MediaGCResult, error) {
	if now.IsZero() {
		now = l.Clock.Now()
	}
	return mediapkg.SweepGCDelete(ctx, l.MediaStore, l.BlobStore, now, l.MediaGC)
}

func (l *KB) PromoteReferencedMedia(ctx context.Context, kbID string, mediaIDs []string) error {
	return mediapkg.PromoteReferenced(ctx, l.MediaStore, kbID, mediaIDs)
}

func (l *KB) liveMediaRefsForKB(ctx context.Context, kbID string) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	if l.ManifestStore == nil {
		return out, nil
	}
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			return out, nil
		}
		return nil, err
	}
	for _, shard := range doc.Manifest.Shards {
		for _, id := range shard.MediaIDs {
			out[id] = struct{}{}
		}
	}
	return out, nil
}
