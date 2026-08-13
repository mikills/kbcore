package eventing

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MaxReasonableVisibilityFuture is the outer edge of any believable
// ClaimedUntil. Anything beyond this is almost certainly clock skew or a
// bogus write. the reaper treats it as an expired claim.
const MaxReasonableVisibilityFuture = 1 * time.Hour

// InMemoryStore stores events in process memory.
type InMemoryStore struct {
	mu        sync.Mutex
	txMu      sync.Mutex
	events    map[string]*Event // event_id → event
	idempKeys map[string]string // idempotency_key|kind|kbid → event_id
	clock     Clock
}

// NewInMemoryStore constructs an empty in-memory event store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		events:    make(map[string]*Event),
		idempKeys: make(map[string]string),
		clock:     RealClock,
	}
}

// SetClock replaces the event store's Clock. Safe to call at any point. the
// swap is serialised against other store operations via the primary mutex.
func (s *InMemoryStore) SetClock(c Clock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c == nil {
		s.clock = RealClock
		return
	}
	s.clock = c
}

func (s *InMemoryStore) now() time.Time { return nowFrom(s.clock) }

func idempKey(e Event) string {
	return e.IdempotencyKey + "|" + string(e.Kind) + "|" + e.KBID
}

func eventBefore(left, right *Event) bool {
	if left.CreatedAt.Equal(right.CreatedAt) {
		return left.EventID < right.EventID
	}
	return left.CreatedAt.Before(right.CreatedAt)
}

// Append inserts the event. Returns ErrDuplicateKey on idempotency
// collision.
func (s *InMemoryStore) Append(_ context.Context, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.EventID == "" {
		return ErrInvalidEvent("event_id required")
	}
	if event.IdempotencyKey != "" {
		key := idempKey(event)
		if _, exists := s.idempKeys[key]; exists {
			return ErrDuplicateKey
		}
		s.idempKeys[key] = event.EventID
	}

	if event.Status == "" {
		event.Status = EventStatusPending
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}
	if event.MaxAttempts <= 0 {
		event.MaxAttempts = DefaultEventMaxAttempts
	}

	cp := event
	s.events[event.EventID] = &cp
	return nil
}

// Claim returns one pending event of the kind, transitioning it to claimed.
func (s *InMemoryStore) Claim(
	_ context.Context,
	kind EventKind,
	workerID string,
	visibility time.Duration,
) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	// Pick the deterministic first pending event without materializing and
	// sorting every candidate. Older events go first, then EventID breaks ties.
	var e *Event
	for _, candidate := range s.events {
		if candidate.Kind != kind || candidate.Status != EventStatusPending {
			continue
		}
		if e == nil || eventBefore(candidate, e) {
			e = candidate
		}
	}
	if e == nil {
		return nil, ErrNoneAvailable
	}
	e.Status = EventStatusClaimed
	e.ClaimedBy = workerID
	e.ClaimedUntil = now.Add(visibility)
	e.Attempt++

	out := *e
	return &out, nil
}

// Ack marks an event done.
func (s *InMemoryStore) Ack(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.events[eventID]
	if !ok {
		return ErrNotFound
	}
	e.Status = EventStatusDone
	e.LastError = ""
	// Acked events have already produced their follow-up event, so their often
	// large document/embedding payload is no longer needed. Keep the envelope
	// for operation lineage and idempotency while releasing the bulk data.
	e.Payload = nil
	return nil
}

// Fail transitions a Claimed event back to Pending (or Dead past stored
// MaxAttempts) only if the stored attempt still equals observedAttempt. A
// mismatch means a concurrent Claim/Requeue advanced the counter. the caller
// should re-read.
func (s *InMemoryStore) Fail(_ context.Context, eventID string, observedAttempt int, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.events[eventID]
	if !ok {
		return ErrNotFound
	}
	if observedAttempt > 0 && e.Attempt != observedAttempt {
		return ErrStateChanged
	}
	e.LastError = errMsg
	max := e.MaxAttempts
	if max <= 0 {
		max = DefaultEventMaxAttempts
	}
	if e.Attempt >= max {
		e.Status = EventStatusDead
		// The companion worker.failed event retains the diagnostic details;
		// release the source document/embedding payload at terminal failure.
		e.Payload = nil
		return nil
	}
	e.Status = EventStatusPending
	e.ClaimedBy = ""
	e.ClaimedUntil = time.Time{}
	return nil
}

// Requeue returns claimed events with expired visibility (or an absurdly
// far-future ClaimedUntil that can only be the product of clock skew) back
// to pending.
func (s *InMemoryStore) Requeue(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.IsZero() {
		now = s.now()
	}
	skewCutoff := now.Add(MaxReasonableVisibilityFuture)
	count := 0
	for _, e := range s.events {
		if e.Status != EventStatusClaimed {
			continue
		}
		if now.After(e.ClaimedUntil) || e.ClaimedUntil.After(skewCutoff) {
			e.Status = EventStatusPending
			e.ClaimedBy = ""
			e.ClaimedUntil = time.Time{}
			count++
		}
	}
	return count, nil
}

// Get returns the event by id.
func (s *InMemoryStore) Get(_ context.Context, eventID string) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.events[eventID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *e
	return &cp, nil
}

func (s *InMemoryStore) FindByIdempotency(
	_ context.Context,
	kind EventKind,
	kbID, idempotencyKey string,
) (*Event, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	eventID, ok := s.idempKeys[idempotencyKey+"|"+string(kind)+"|"+kbID]
	if !ok {
		return nil, nil
	}
	e, ok := s.events[eventID]
	if !ok {
		return nil, nil
	}
	cp := *e
	return &cp, nil
}

func (s *InMemoryStore) ListUnfinishedBefore(
	_ context.Context,
	kind EventKind,
	before time.Time,
	after string,
	limit int,
) ([]Event, string, error) {
	if limit <= 0 {
		limit = 1000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	matches := s.unfinishedEventsBeforeLocked(kind, before)
	sortEventPage(matches)
	return eventPageAfter(matches, after, limit), eventNextToken(matches, after, limit), nil
}

func (s *InMemoryStore) unfinishedEventsBeforeLocked(kind EventKind, before time.Time) []Event {
	matches := make([]Event, 0)
	for _, event := range s.events {
		if unfinishedEventMatches(event, kind, before) {
			matches = append(matches, *event)
		}
	}
	return matches
}

func unfinishedEventMatches(event *Event, kind EventKind, before time.Time) bool {
	if event.Kind != kind {
		return false
	}
	if !before.IsZero() && event.CreatedAt.After(before) {
		return false
	}
	return event.Status != EventStatusDone && event.Status != EventStatusDead
}

func sortEventPage(events []Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].CreatedAt.Equal(events[j].CreatedAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})
}

func eventPageAfter(events []Event, after string, limit int) []Event {
	start, end := eventPageBounds(events, after, limit)
	return append([]Event(nil), events[start:end]...)
}

func eventNextToken(events []Event, after string, limit int) string {
	_, end := eventPageBounds(events, after, limit)
	if end < len(events) {
		return events[end-1].EventID
	}
	return ""
}

func eventPageBounds(events []Event, after string, limit int) (int, int) {
	start := eventPageStart(events, after)
	end := min(start+limit, len(events))
	return start, end
}

func eventPageStart(events []Event, after string) int {
	if after == "" {
		return 0
	}
	for idx, event := range events {
		if event.EventID == after {
			return idx + 1
		}
	}
	return 0
}

// Cleanup removes terminal-state events whose CreatedAt is before olderThan
// and prunes the companion idempotency-key index. Returns the count of events
// removed.
func (s *InMemoryStore) Cleanup(_ context.Context, olderThan time.Time) (int, error) {
	if olderThan.IsZero() {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, e := range s.events {
		if e.Status != EventStatusDone && e.Status != EventStatusDead {
			continue
		}
		if !e.CreatedAt.Before(olderThan) {
			continue
		}
		delete(s.events, id)
		if e.IdempotencyKey != "" {
			delete(s.idempKeys, idempKey(*e))
		}
		removed++
	}
	return removed, nil
}

// FindByCausation returns the latest event of the given kind whose
// CausationID equals sourceEventID, or nil when none exists.
func (s *InMemoryStore) FindByCausation(
	_ context.Context,
	kind EventKind,
	sourceEventID string,
) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest *Event
	for _, e := range s.events {
		if e.Kind != kind || e.CausationID != sourceEventID {
			continue
		}
		if latest == nil || e.CreatedAt.After(latest.CreatedAt) ||
			(e.CreatedAt.Equal(latest.CreatedAt) && e.EventID > latest.EventID) {
			cp := *e
			latest = &cp
		}
	}
	return latest, nil
}

// InTransaction serializes fn against other store operations via txMu so the
// worker's append+ack+mark-inbox sequence cannot interleave with other
// callers. The in-memory store still cannot provide true rollback. if fn
// returns an error the caller is responsible for not having mutated external
// state. This matches the Mongo store's externally visible contract closely
// enough for tests and single-process deployments.
func (s *InMemoryStore) InTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	return fn(ctx)
}

// Snapshot returns a copy of all events. For tests and operator queries.
func (s *InMemoryStore) Snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, 0, len(s.events))
	for _, e := range s.events {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

// ErrInvalidEvent describes an invalid-event-input error.
type ErrInvalidEvent string

func (e ErrInvalidEvent) Error() string { return "event: invalid input: " + string(e) }
