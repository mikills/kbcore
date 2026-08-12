package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/mikills/minnow/kb/workerpool"
)

type WorkerResult = workerpool.WorkerResult
type Worker = workerpool.Worker
type WorkerPoolConfig = workerpool.WorkerPoolConfig
type WorkerMetrics = workerpool.WorkerMetrics
type NoopWorkerMetrics = workerpool.NoopWorkerMetrics
type WorkerPool = workerpool.WorkerPool

var (
	ErrStoreNotTransactional = workerpool.ErrStoreNotTransactional
	ErrAlreadyStarted        = workerpool.ErrAlreadyStarted
)

func NewWorkerPool(worker Worker, store EventStore, inbox EventInbox, cfg WorkerPoolConfig) (*WorkerPool, error) {
	if cfg.BuildFailureEvent == nil {
		cfg.BuildFailureEvent = buildWorkerFailedEvent
	}
	return workerpool.NewWorkerPool(worker, store, inbox, cfg)
}

func buildWorkerFailedEvent(input workerpool.FailureEventInput) (KBEvent, error) {
	var resultCarrier fileResultCarrier
	hasFileResults := errors.As(input.Error, &resultCarrier)
	var fileResults []FileIngestResult
	if hasFileResults && resultCarrier != nil {
		fileResults = resultCarrier.FileIngestResults()
	}
	payload, err := json.Marshal(WorkerFailedPayload{
		Stage:         string(input.WorkerKind),
		SourceEventID: input.Event.EventID,
		Attempt:       input.Event.Attempt,
		Error:         input.Error.Error(),
		WillRetry:     input.WillRetry,
		FileResults:   fileResults,
	})
	if err != nil {
		return KBEvent{}, err
	}
	return KBEvent{
		EventID:        newULIDLikeAt("evt", input.Now),
		KBID:           input.Event.KBID,
		Kind:           EventWorkerFailed,
		Payload:        payload,
		PayloadSchema:  "worker.failed/v1",
		CorrelationID:  input.Event.CorrelationID,
		CausationID:    input.Event.EventID,
		IdempotencyKey: fmt.Sprintf("%s|worker.failed|%d", input.Event.EventID, input.Event.Attempt),
		Status:         EventStatusPending,
		CreatedAt:      input.Now,
	}, nil
}

type DocumentChunkedWorker struct {
	KB *KB
	ID string
}

func (w *DocumentChunkedWorker) Kind() EventKind  { return EventDocumentChunked }
func (w *DocumentChunkedWorker) WorkerID() string { return w.ID }

func (w *DocumentChunkedWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	var payload DocumentChunkedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	embedded := make([]EmbeddedDocument, 0, len(payload.Documents))
	for _, doc := range payload.Documents {
		vec, err := w.KB.Embed(ctx, doc.Text)
		if err != nil {
			return WorkerResult{}, fmt.Errorf("embed doc %q: %w", doc.ID, err)
		}
		embedded = append(embedded, EmbeddedDocument{
			ID:        doc.ID,
			Text:      doc.Text,
			MediaIDs:  doc.MediaIDs,
			MediaRefs: doc.MediaRefs,
			Metadata:  doc.Metadata,
			Embedding: vec,
		})
	}
	if payload.Options.GraphEnabled != nil && *payload.Options.GraphEnabled {
		return w.handleGraphExtract(ctx, event, payload, embedded)
	}
	return w.handleEmbed(event, payload, embedded), nil
}

func (w *DocumentChunkedWorker) handleEmbed(
	event *KBEvent,
	payload DocumentChunkedPayload,
	embedded []EmbeddedDocument,
) WorkerResult {
	nextPayload, _ := json.Marshal(DocumentEmbeddedPayload{
		KBID:          payload.KBID,
		DocumentCount: payload.DocumentCount,
		Documents:     embedded,
		FileResults:   payload.FileResults,
		Options:       payload.Options,
		SourceEventID: payload.SourceEventID,
	})
	return WorkerResult{
		FollowUps: []KBEvent{
			w.KB.newChildPendingEvent(
				event,
				EventDocumentEmbedded,
				"document.embedded/v1",
				event.EventID+"|document.embedded",
				nextPayload,
			),
		},
	}
}

func (w *DocumentChunkedWorker) handleGraphExtract(
	ctx context.Context,
	event *KBEvent,
	payload DocumentChunkedPayload,
	embedded []EmbeddedDocument,
) (WorkerResult, error) {
	if w.KB.GraphBuilder == nil {
		return WorkerResult{}, ErrGraphUnavailable
	}
	graphDocs := make([]Document, 0, len(payload.Documents))
	for _, doc := range payload.Documents {
		graphDocs = append(graphDocs, doc)
	}
	graphResult, err := w.KB.GraphBuilder.Build(ctx, graphDocs)
	if err != nil {
		return WorkerResult{}, fmt.Errorf("build graph: %w", err)
	}
	nextPayload, _ := json.Marshal(DocumentGraphExtractedPayload{
		KBID:          payload.KBID,
		DocumentCount: payload.DocumentCount,
		Documents:     embedded,
		FileResults:   payload.FileResults,
		GraphResult:   graphResult,
		Options:       payload.Options,
		SourceEventID: payload.SourceEventID,
	})
	return WorkerResult{
		FollowUps: []KBEvent{
			w.KB.newChildPendingEvent(
				event,
				EventDocumentGraphExtracted,
				"document.graph_extracted/v1",
				event.EventID+"|document.graph_extracted",
				nextPayload,
			),
		},
	}, nil
}

type DocumentUpsertWorker struct {
	KB *KB
	ID string
}

func (w *DocumentUpsertWorker) Kind() EventKind { return EventDocumentUpsert }

func (w *DocumentUpsertWorker) WorkerID() string { return w.ID }

func (w *DocumentUpsertWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	var payload DocumentUpsertPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	chunkedDocs := payload.Documents
	var fileResults []FileIngestResult
	var stagedBlobKeys []string
	if payload.PreChunked && len(payload.FileSources) != 0 {
		return WorkerResult{}, fmt.Errorf("pre-chunked ingest does not accept file sources")
	}
	if !payload.PreChunked {
		var err error
		chunkedDocs, fileResults, _, stagedBlobKeys, err = w.normalizeDocuments(
			ctx,
			payload.KBID,
			payload.Documents,
			payload.FileSources,
			payload.ChunkSize,
		)
		if err != nil {
			return WorkerResult{}, err
		}
	}
	nextPayload, _ := json.Marshal(DocumentChunkedPayload{
		KBID:          payload.KBID,
		DocumentCount: len(payload.Documents) + successfulFileCount(fileResults),
		Documents:     chunkedDocs,
		FileResults:   fileResults,
		Options:       payload.Options,
		SourceEventID: event.EventID,
	})
	next := w.KB.newChildPendingEvent(
		event,
		EventDocumentChunked,
		"document.chunked/v1",
		event.EventID+"|document.chunked",
		nextPayload,
	)
	return WorkerResult{FollowUps: []KBEvent{next}, Commit: func(ctx context.Context) error {
		var firstErr error
		for _, key := range stagedBlobKeys {
			err := w.KB.BlobStore.Delete(ctx, key)
			if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
				continue
			}
			slog.Default().Warn("ingest: staged blob delete failed",
				"blob_key", key, logKeyError, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("ingest: delete staged blob %q: %w", key, err)
			}
		}
		return firstErr
	}}, nil
}

func successfulFileCount(results []FileIngestResult) int {
	count := 0
	for _, result := range results {
		if result.Status == "succeeded" {
			count++
		}
	}
	return count
}
