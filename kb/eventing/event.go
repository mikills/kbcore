package eventing

import (
	"context"
	"errors"
	"time"
)

type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

var RealClock Clock = realClock{}

func nowFrom(c Clock) time.Time {
	if c == nil {
		return time.Now().UTC()
	}
	return c.Now()
}

const (
	mongoKeyID      = "_id"
	mongoKeyKind    = "kind"
	mongoKeyStatus  = "status"
	mongoKeyCreated = "created_at"
	mongoKeyClaimed = "claimed_until"
	mongoKeyKBID    = "kb_id"
)

// EventKind enumerates the small set of event kinds the system uses.
//
// The taxonomy intentionally avoids `.requested`/`.completed` pairing for
// internal events: the prior stage's completion event is the next stage's
// trigger.
type EventKind string

const (
	// External boundary commands (imperative. addressed to one handler.
	// may be rejected).
	EventMediaUpload    EventKind = "media.upload"
	EventDocumentUpsert EventKind = "document.upsert"

	// Domain events (facts, past tense. trigger downstream stages).
	EventMediaUploaded          EventKind = "media.uploaded"
	EventDocumentChunked        EventKind = "document.chunked"
	EventDocumentEmbedded       EventKind = "document.embedded"
	EventDocumentGraphExtracted EventKind = "document.graph_extracted"
	EventKBPublished            EventKind = "kb.published"

	// Single failure event shared across all stages.
	EventWorkerFailed EventKind = "worker.failed"
	// Durable cleanup work emitted after staged blobs are no longer live inputs.
	EventStagingCleanup EventKind = "staging.cleanup"
)

// EventStatus tracks event lifecycle in the outbox.
type EventStatus string

const (
	EventStatusPending EventStatus = "pending"
	EventStatusClaimed EventStatus = "claimed"
	EventStatusDone    EventStatus = "done"
	EventStatusFailed  EventStatus = "failed"
	EventStatusDead    EventStatus = "dead"
)

// Event is a single entry in the event log / outbox.
//
// Every event carries an IdempotencyKey, producer-generated and stable across
// retries, so consumer-side inbox dedup is possible. External clients provide
// the key via Idempotency-Key header. internal producers derive it from
// (parent_event_id, child_kind).
//
// MaxAttempts is stored on the event at Append time so all workers agree on
// the retry ceiling for a given event, even if WorkerPoolConfig drifts across
// replicas. Migration: events appended before this field existed decode as
// zero. callers must fall back to DefaultEventMaxAttempts when event.MaxAttempts
// <= 0.
type Event struct {
	EventID        string
	KBID           string
	Kind           EventKind
	Payload        []byte // opaque, kind-specific
	PayloadSchema  string // e.g. "media.uploaded/v1"
	CorrelationID  string
	CausationID    string
	IdempotencyKey string

	Status       EventStatus
	Attempt      int
	MaxAttempts  int
	ClaimedBy    string
	ClaimedUntil time.Time
	CreatedAt    time.Time
	LastError    string
}

// DefaultEventMaxAttempts is the fallback retry ceiling applied when an event
// is read without MaxAttempts set (e.g. events written before the field was
// added, or callers that passed zero).
const DefaultEventMaxAttempts = 3

// EffectiveMaxAttempts returns the retry ceiling to apply for this event,
// falling back to DefaultEventMaxAttempts when MaxAttempts is unset.
func (e Event) EffectiveMaxAttempts() int {
	if e.MaxAttempts > 0 {
		return e.MaxAttempts
	}
	return DefaultEventMaxAttempts
}

// Store is the append-only event log abstraction.
//
// Implementations must commit event appends and caller-supplied state mutations
// in a single atomic transaction, which rules out pure message-queue backings.
// InMemoryStore covers tests and single-process deployments. the Mongo
// store is the production target and relies on multi-document transactions.
type Store interface {
	// Append inserts a new event. Returns ErrDuplicateKey if an event
	// with the same IdempotencyKey + Kind + KBID already exists.
	Append(ctx context.Context, event Event) error

	// Claim atomically transitions one Pending event of the given Kind to
	// Claimed and returns it. Returns ErrNoneAvailable if no event is
	// ready. The visibility timeout is set to now+visibilityTimeout. if the
	// worker does not Ack/Fail within that window, the event-reaper sweep
	// returns it to Pending.
	Claim(ctx context.Context, kind EventKind, workerID string, visibilityTimeout time.Duration) (*Event, error)

	// Ack marks an event Done. Should be called inside the worker's
	// inbox-protected transaction.
	Ack(ctx context.Context, eventID string) error

	// Fail transitions a Claimed event back to Pending, or Dead when the
	// stored MaxAttempts has been reached. The caller passes the attempt
	// counter value observed at Claim time. if the stored attempt has
	// advanced since then, implementations return ErrStateChanged so
	// the caller can re-read and decide what to do. The target status
	// (Pending vs Dead) is decided server-side from the stored
	// MaxAttempts (falling back to DefaultEventMaxAttempts).
	Fail(ctx context.Context, eventID string, observedAttempt int, errMessage string) error

	// Requeue returns Claimed events whose ClaimedUntil < now back to
	// Pending. Returns the number of events returned. Driven by the
	// event-reaper sweep.
	Requeue(ctx context.Context, now time.Time) (int, error)

	// Get returns an event by id.
	Get(ctx context.Context, eventID string) (*Event, error)

	// FindByIdempotency returns the first event matching the logical producer
	// key, or nil when none exists.
	FindByIdempotency(ctx context.Context, kind EventKind, kbID, idempotencyKey string) (*Event, error)

	// FindByCausation returns the first event of kind whose causation/source id matches.
	FindByCausation(ctx context.Context, kind EventKind, sourceEventID string) (*Event, error)

	// ListUnfinishedBefore returns at most limit events of a kind older
	// than before whose lifecycle is not terminal yet (default 1000 when
	// limit <= 0), sorted by created_at ascending. When more events
	// remain, the returned continuation token is non-empty. pass it back
	// as `after` on the next call. Callers iterate until the token is
	// empty.
	ListUnfinishedBefore(
		ctx context.Context,
		kind EventKind,
		before time.Time,
		after string,
		limit int,
	) (events []Event, next string, err error)

	// Cleanup removes events in terminal state (done, dead) older than the
	// cutoff. Returns the count removed. Driven by a periodic sweep.
	Cleanup(ctx context.Context, olderThan time.Time) (int, error)
}

// Sentinel errors for Store implementations.
var (
	ErrDuplicateKey  = errors.New("event: duplicate idempotency key")
	ErrNoneAvailable = errors.New("event: none available")
	ErrNotFound      = errors.New("event: not found")
	// ErrStateChanged signals that a conditional update saw the stored
	// attempt counter advance past the observed value. Callers should re-read
	// the event and decide whether to retry.
	ErrStateChanged = errors.New("event: state changed since observation")
)

// TransactionRunner is an optional capability on Store implementations
// that can execute a closure inside a single transaction. Mongo-backed
// stores use a multi-document transaction. the in-memory store executes
// the closure directly and is best-effort.
//
// Workers use this to commit the (append child events → mark inbox →
// Ack source) sequence atomically after their side-effects have run.
type TransactionRunner interface {
	InTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}
