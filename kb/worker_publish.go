package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type DocumentPublishWorker struct {
	KB        *KB
	ID        string
	KindValue EventKind
}

func (w *DocumentPublishWorker) Kind() EventKind  { return w.KindValue }
func (w *DocumentPublishWorker) WorkerID() string { return w.ID }

type publishWork struct {
	kbID          string
	docs          []EmbeddedDocument
	graphResult   *GraphBuildResult
	fileResults   []FileIngestResult
	documentCount int
	opts          UpsertDocsOptions
	sourceEventID string
}

func (w *DocumentPublishWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	if err := w.validateEvent(event); err != nil {
		return WorkerResult{}, err
	}
	work, err := decodePublishWork(event)
	if err != nil {
		return WorkerResult{}, err
	}
	plainDocs := embeddedToPlainDocuments(work.docs)
	if err := w.KB.ValidateDocumentReferences(ctx, work.kbID, plainDocs); err != nil {
		return WorkerResult{}, err
	}
	if err := w.KB.PublishPreparedDocs(ctx, work.kbID, work.docs, work.graphResult, work.opts); err != nil {
		return WorkerResult{}, err
	}
	return w.publishedResult(event, work, plainDocs), nil
}

func (w *DocumentPublishWorker) validateEvent(event *KBEvent) error {
	if event == nil {
		return errors.New("publish worker: nil event")
	}
	if event.Kind != w.KindValue {
		return fmt.Errorf("publish worker: event kind %q does not match worker kind %q", event.Kind, w.KindValue)
	}
	return nil
}

func decodePublishWork(event *KBEvent) (publishWork, error) {
	switch event.Kind {
	case EventDocumentEmbedded:
		return decodeEmbeddedPublishWork(event)
	case EventDocumentGraphExtracted:
		return decodeGraphPublishWork(event)
	default:
		return publishWork{}, fmt.Errorf("publish worker: unsupported kind %s", event.Kind)
	}
}

func decodeEmbeddedPublishWork(event *KBEvent) (publishWork, error) {
	if event.PayloadSchema != "" && event.PayloadSchema != "document.embedded/v1" {
		return publishWork{}, fmt.Errorf(
			"publish worker: expected document.embedded/v1 schema, got %q",
			event.PayloadSchema,
		)
	}
	var payload DocumentEmbeddedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return publishWork{}, fmt.Errorf("decode payload: %w", err)
	}
	return publishWork{
		kbID:          payload.KBID,
		docs:          payload.Documents,
		fileResults:   payload.FileResults,
		documentCount: payload.DocumentCount,
		opts:          payload.Options,
		sourceEventID: payload.SourceEventID,
	}, nil
}

func decodeGraphPublishWork(event *KBEvent) (publishWork, error) {
	if event.PayloadSchema != "" && event.PayloadSchema != "document.graph_extracted/v1" {
		return publishWork{}, fmt.Errorf(
			"publish worker: expected document.graph_extracted/v1 schema, got %q",
			event.PayloadSchema,
		)
	}
	var payload DocumentGraphExtractedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return publishWork{}, fmt.Errorf("decode payload: %w", err)
	}
	return publishWork{
		kbID:          payload.KBID,
		docs:          payload.Documents,
		fileResults:   payload.FileResults,
		documentCount: payload.DocumentCount,
		opts:          payload.Options,
		sourceEventID: payload.SourceEventID,
		graphResult:   payload.GraphResult,
	}, nil
}

func embeddedToPlainDocuments(docs []EmbeddedDocument) []Document {
	plainDocs := make([]Document, 0, len(docs))
	for _, doc := range docs {
		plainDocs = append(
			plainDocs,
			Document{
				ID:        doc.ID,
				Text:      doc.Text,
				MediaIDs:  doc.MediaIDs,
				MediaRefs: doc.MediaRefs,
				Metadata:  doc.Metadata,
			},
		)
	}
	return plainDocs
}

func (w *DocumentPublishWorker) publishedResult(event *KBEvent, work publishWork, plainDocs []Document) WorkerResult {
	mediaIDs := collectMediaIDs(plainDocs)
	pubPayload, _ := json.Marshal(
		KBPublishedPayload{
			KBID:          work.kbID,
			DocumentCount: work.documentCount,
			ChunkCount:    len(plainDocs),
			MediaIDs:      mediaIDs,
			FileResults:   work.fileResults,
			SourceEventID: work.sourceEventID,
		},
	)
	pub := w.KB.newChildDoneEvent(
		event,
		EventKBPublished,
		"kb.published/v1",
		work.sourceEventID+"|kb.published",
		pubPayload,
	)
	return WorkerResult{FollowUps: []KBEvent{pub}, Commit: func(ctx context.Context) error {
		if err := w.KB.PromoteReferencedMedia(ctx, work.kbID, mediaIDs); err != nil {
			return fmt.Errorf("promote media: %w", err)
		}
		return nil
	}}
}
