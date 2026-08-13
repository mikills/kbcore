package kb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mikills/minnow/kb/fileingest"
)

// Recommended visibility timeouts per ingest stage. Exposed so operators can
// tune them. the default WorkerPoolConfig value (kb/worker.go) is too short
// for embedding / graph / publish work.
const (
	DocumentUpsertWorkerVisibilityTimeout  = 5 * time.Minute
	EmbedWorkerVisibilityTimeout           = 10 * time.Minute
	GraphExtractWorkerVisibilityTimeout    = 15 * time.Minute
	DocumentPublishWorkerVisibilityTimeout = 5 * time.Minute
)

type FileIngestSource = fileingest.Source
type FileIngestResult = fileingest.Result
type FileIngestUpload = fileingest.Upload

// DocumentUpsertPayload is the payload carried by a document.upsert command.
type DocumentUpsertPayload struct {
	KBID        string             `json:"kb_id"`
	Documents   []Document         `json:"documents"`
	FileSources []FileIngestSource `json:"file_sources,omitempty"`
	ChunkSize   int                `json:"chunk_size,omitempty"`
	PreChunked  bool               `json:"pre_chunked,omitempty"`
	Options     UpsertDocsOptions  `json:"options"`
}

type DocumentChunkedPayload struct {
	KBID          string             `json:"kb_id"`
	DocumentCount int                `json:"document_count"`
	Documents     []Document         `json:"documents"`
	FileResults   []FileIngestResult `json:"file_results,omitempty"`
	Options       UpsertDocsOptions  `json:"options"`
	SourceEventID string             `json:"source_event_id"`
}

type DocumentEmbeddedPayload struct {
	KBID          string             `json:"kb_id"`
	DocumentCount int                `json:"document_count"`
	Documents     []EmbeddedDocument `json:"documents"`
	FileResults   []FileIngestResult `json:"file_results,omitempty"`
	Options       UpsertDocsOptions  `json:"options"`
	SourceEventID string             `json:"source_event_id"`
}

type DocumentGraphExtractedPayload struct {
	KBID          string             `json:"kb_id"`
	DocumentCount int                `json:"document_count"`
	Documents     []EmbeddedDocument `json:"documents"`
	FileResults   []FileIngestResult `json:"file_results,omitempty"`
	GraphResult   *GraphBuildResult  `json:"graph_result,omitempty"`
	Options       UpsertDocsOptions  `json:"options"`
	SourceEventID string             `json:"source_event_id"`
}

// KBPublishedPayload is the payload carried by kb.published events.
type KBPublishedPayload struct {
	KBID          string             `json:"kb_id"`
	DocumentCount int                `json:"document_count"`
	ChunkCount    int                `json:"chunk_count"`
	MediaIDs      []string           `json:"media_ids,omitempty"`
	FileResults   []FileIngestResult `json:"file_results,omitempty"`
	SourceEventID string             `json:"source_event_id,omitempty"`
}

// WorkerFailedPayload is the payload carried by worker.failed events.
type StagingCleanupPayload struct {
	Keys []string `json:"keys"`
}

type WorkerFailedPayload struct {
	Stage         string             `json:"stage"`
	SourceEventID string             `json:"source_event_id"`
	Attempt       int                `json:"attempt"`
	Error         string             `json:"error"`
	WillRetry     bool               `json:"will_retry"`
	FileResults   []FileIngestResult `json:"file_results,omitempty"`
}

type FileIngestInput struct {
	KBID      string
	Documents []Document
	Files     []FileIngestUpload
	ChunkSize int
	Options   UpsertDocsOptions
}

// NewULIDLike returns a short sortable id usable for event/correlation ids,
// using real wall-clock time. Call NewULIDLikeAt from a KB method when a
// FakeClock is in play so IDs remain deterministic under simulation.
func NewULIDLike(prefix string) string {
	return newULIDLikeAt(prefix, RealClock.Now())
}

// NewULIDLikeAt returns an id using the supplied Clock for the timestamp
// component. Intended for internal callers that already hold a Clock (e.g.
// KB methods). External users can stick with NewULIDLike.
func NewULIDLikeAt(prefix string, c Clock) string {
	return newULIDLikeAt(prefix, nowFrom(c))
}

func newULIDLikeAt(prefix string, now time.Time) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s_%013d_%s", prefix, now.UnixMilli(), hex.EncodeToString(b))
}

type pendingEventInput struct {
	kind           EventKind
	kbID           string
	schema         string
	correlationID  string
	causationID    string
	idempotencyKey string
	payload        []byte
}

func (l *KB) newPendingEvent(input pendingEventInput) KBEvent {
	now := l.Clock.Now()
	return KBEvent{
		EventID:        newULIDLikeAt("evt", now),
		KBID:           input.kbID,
		Kind:           input.kind,
		Payload:        input.payload,
		PayloadSchema:  input.schema,
		CorrelationID:  input.correlationID,
		CausationID:    input.causationID,
		IdempotencyKey: input.idempotencyKey,
		Status:         EventStatusPending,
		CreatedAt:      now,
	}
}

func (l *KB) newRootPendingEvent(input pendingEventInput) KBEvent {
	input.causationID = ""
	return l.newPendingEvent(input)
}

func (l *KB) newChildPendingEvent(
	parent *KBEvent,
	kind EventKind,
	schema, idempotencyKey string,
	payload []byte,
) KBEvent {
	return l.newChildEvent(parent, kind, schema, idempotencyKey, payload, EventStatusPending)
}

func (l *KB) newChildDoneEvent(
	parent *KBEvent,
	kind EventKind,
	schema, idempotencyKey string,
	payload []byte,
) KBEvent {
	return l.newChildEvent(parent, kind, schema, idempotencyKey, payload, EventStatusDone)
}

func (l *KB) newChildEvent(
	parent *KBEvent,
	kind EventKind,
	schema, idempotencyKey string,
	payload []byte,
	status EventStatus,
) KBEvent {
	event := l.newPendingEvent(pendingEventInput{
		kind:           kind,
		kbID:           parent.KBID,
		schema:         schema,
		correlationID:  parent.CorrelationID,
		causationID:    parent.EventID,
		idempotencyKey: idempotencyKey,
		payload:        payload,
	})
	event.Status = status
	return event
}

// NewULIDLike on *KB uses the KB's Clock. Prefer this over the package-level
// NewULIDLike inside any code that already has a *KB in scope.
func (l *KB) NewULIDLike(prefix string) string {
	return newULIDLikeAt(prefix, l.Clock.Now())
}

// AppendDocumentUpsertDetailed appends a document.upsert command event.
// The caller supplies an idempotency key (client-provided or API-generated)
// so retried submissions collapse to a single event.
//
// Returns the new event's id and the effective idempotency key.
func (l *KB) AppendDocumentUpsertDetailed(
	ctx context.Context,
	p DocumentUpsertPayload,
	idempotencyKey, correlationID string,
) (string, string, error) {
	eventID, effectiveKey, _, err := l.appendDocumentUpsertDetailed(ctx, p, idempotencyKey, correlationID)
	return eventID, effectiveKey, err
}

// appendDocumentUpsertDetailed additionally reports whether this call created
// the event. File ingest uses that distinction to reclaim request-local staged
// blobs when a concurrent idempotent submission wins the append race.
func (l *KB) appendDocumentUpsertDetailed(
	ctx context.Context,
	p DocumentUpsertPayload,
	idempotencyKey, correlationID string,
) (string, string, bool, error) {
	if l.EventStore == nil {
		return "", "", false, errors.New("ingest: EventStore not configured")
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return "", "", false, fmt.Errorf("marshal payload: %w", err)
	}
	idempotencyKey, correlationID = l.ensureEventKeys(idempotencyKey, correlationID)
	if err := l.validateDocumentUpsertRequest(ctx, p); err != nil {
		return "", "", false, err
	}
	if err := l.ValidateDocumentReferences(ctx, p.KBID, p.Documents); err != nil {
		return "", "", false, err
	}
	event := l.newRootPendingEvent(
		pendingEventInput{
			kind:           EventDocumentUpsert,
			kbID:           p.KBID,
			schema:         "document.upsert/v1",
			correlationID:  correlationID,
			idempotencyKey: idempotencyKey,
			payload:        payload,
		},
	)
	return l.appendDocumentUpsertEvent(ctx, event, p.KBID, idempotencyKey)
}

func (l *KB) ensureEventKeys(idempotencyKey string, correlationID string) (string, string) {
	if idempotencyKey == "" {
		idempotencyKey = l.NewULIDLike("idem")
	}
	if correlationID == "" {
		correlationID = l.NewULIDLike("corr")
	}
	return idempotencyKey, correlationID
}

func (l *KB) validateDocumentUpsertRequest(ctx context.Context, p DocumentUpsertPayload) error {
	if p.Options.GraphEnabled != nil && *p.Options.GraphEnabled && l.GraphBuilder == nil {
		return ErrGraphUnavailable
	}
	return nil
}

func (l *KB) appendDocumentUpsertEvent(
	ctx context.Context,
	event KBEvent,
	kbID string,
	idempotencyKey string,
) (string, string, bool, error) {
	if err := l.EventStore.Append(ctx, event); err != nil {
		existingID, effectiveKey, handleErr := l.handleDocumentUpsertAppendError(ctx, err, kbID, idempotencyKey)
		return existingID, effectiveKey, false, handleErr
	}
	return event.EventID, idempotencyKey, true, nil
}

func (l *KB) handleDocumentUpsertAppendError(
	ctx context.Context,
	err error,
	kbID string,
	idempotencyKey string,
) (string, string, error) {
	if !errors.Is(err, ErrEventDuplicateKey) {
		return "", "", err
	}
	existing, findErr := l.EventStore.FindByIdempotency(ctx, EventDocumentUpsert, kbID, idempotencyKey)
	if findErr != nil {
		return "", "", findErr
	}
	if existing != nil {
		return existing.EventID, idempotencyKey, nil
	}
	return "", "", err
}

func (l *KB) AppendDocumentUpsert(
	ctx context.Context,
	p DocumentUpsertPayload,
	idempotencyKey, correlationID string,
) (string, error) {
	eventID, _, err := l.AppendDocumentUpsertDetailed(ctx, p, idempotencyKey, correlationID)
	return eventID, err
}

type OperationStageSnapshot struct {
	Event   *KBEvent
	Failure *KBEvent
}

func (l *KB) FindOperationTerminal(ctx context.Context, sourceEventID string) (*KBEvent, error) {
	for _, kind := range []EventKind{EventKBPublished, EventMediaUploaded} {
		ev, err := l.findOperationEvent(ctx, sourceEventID, kind)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			return ev, nil
		}
	}
	stages, err := l.OperationStages(ctx, sourceEventID)
	if err != nil {
		return nil, err
	}
	for i := len(stages) - 1; i >= 0; i-- {
		if stages[i].Event.Status != EventStatusDead {
			continue
		}
		if stages[i].Failure != nil {
			return stages[i].Failure, nil
		}
		// Fail notification persistence is best-effort relative to the Dead
		// transition. Synthesize a stable terminal view from the source so a
		// timestamp tie or transient notification failure cannot hide it.
		payload, _ := json.Marshal(WorkerFailedPayload{
			Stage: string(stages[i].Event.Kind), SourceEventID: stages[i].Event.EventID,
			Attempt: stages[i].Event.Attempt, Error: stages[i].Event.LastError, WillRetry: false,
		})
		return &KBEvent{
			EventID: stages[i].Event.EventID + "|terminal-failure", KBID: stages[i].Event.KBID,
			Kind: EventWorkerFailed, Payload: payload, PayloadSchema: "worker.failed/v1",
			CorrelationID: stages[i].Event.CorrelationID, CausationID: stages[i].Event.EventID,
			Status: EventStatusDone, CreatedAt: stages[i].Event.CreatedAt,
		}, nil
	}
	return nil, nil
}

const operationStageCapacity = 1 << 2

func workerFailureWillRetry(payload []byte) bool {
	var failure WorkerFailedPayload
	return json.Unmarshal(payload, &failure) == nil && failure.WillRetry
}

func (l *KB) OperationStages(ctx context.Context, sourceEventID string) ([]OperationStageSnapshot, error) {
	if l.EventStore == nil {
		return nil, errors.New("event: EventStore not configured")
	}
	current, err := l.EventStore.Get(ctx, sourceEventID)
	if err != nil {
		return nil, err
	}
	stages := make([]OperationStageSnapshot, 0, operationStageCapacity)
	seen := map[string]struct{}{}
	for current != nil {
		if _, ok := seen[current.EventID]; ok {
			break
		}
		seen[current.EventID] = struct{}{}
		failure, err := l.EventStore.FindByCausation(ctx, EventWorkerFailed, current.EventID)
		if err != nil {
			return nil, err
		}
		if current.Status != EventStatusDead || (failure != nil && workerFailureWillRetry(failure.Payload)) {
			failure = nil
		}
		stages = append(stages, OperationStageSnapshot{Event: current, Failure: failure})
		next, err := l.nextOperationStage(ctx, current)
		if err != nil {
			return nil, err
		}
		current = next
	}
	return stages, nil
}

func (l *KB) nextOperationStage(ctx context.Context, current *KBEvent) (*KBEvent, error) {
	if current == nil {
		return nil, nil
	}
	for _, kind := range operationNextKinds(current.Kind) {
		child, err := l.EventStore.FindByCausation(ctx, kind, current.EventID)
		if err != nil {
			return nil, err
		}
		if child != nil {
			return child, nil
		}
	}
	return nil, nil
}

func operationNextKinds(kind EventKind) []EventKind {
	switch kind {
	case EventDocumentUpsert:
		return []EventKind{EventDocumentChunked}
	case EventDocumentChunked:
		return []EventKind{EventDocumentEmbedded, EventDocumentGraphExtracted}
	case EventDocumentEmbedded, EventDocumentGraphExtracted:
		return []EventKind{EventKBPublished}
	case EventMediaUpload:
		return []EventKind{EventMediaUploaded}
	default:
		return nil
	}
}

func (l *KB) findOperationEvent(ctx context.Context, rootEventID string, targetKind EventKind) (*KBEvent, error) {
	if l.EventStore == nil {
		return nil, errors.New("event: EventStore not configured")
	}
	return l.findOperationEventFrom(ctx, rootEventID, targetKind, map[string]struct{}{})
}

func (l *KB) findOperationEventFrom(
	ctx context.Context,
	currentEventID string,
	targetKind EventKind,
	seen map[string]struct{},
) (*KBEvent, error) {
	if _, ok := seen[currentEventID]; ok {
		return nil, nil
	}
	seen[currentEventID] = struct{}{}
	if ev, err := l.EventStore.FindByCausation(ctx, targetKind, currentEventID); err != nil {
		return nil, err
	} else if ev != nil {
		return ev, nil
	}
	for _, childKind := range []EventKind{EventDocumentChunked, EventDocumentEmbedded, EventDocumentGraphExtracted} {
		child, err := l.EventStore.FindByCausation(ctx, childKind, currentEventID)
		if err != nil {
			return nil, err
		}
		if child == nil {
			continue
		}
		resolved, err := l.findOperationEventFrom(ctx, child.EventID, targetKind, seen)
		if err != nil {
			return nil, err
		}
		if resolved != nil {
			return resolved, nil
		}
	}
	return nil, nil
}

func collectMediaIDs(docs []Document) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	visit := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, d := range docs {
		for _, id := range d.MediaIDs {
			visit(id)
		}
		for _, r := range d.MediaRefs {
			visit(r.MediaID)
		}
	}
	return out
}
