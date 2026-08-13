package eventing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time  { return c.now }
func (c *testClock) Set(t time.Time) { c.now = t }

func mkEvent(id, kbID string, kind EventKind, idem string) Event {
	return Event{EventID: id, KBID: kbID, Kind: kind, IdempotencyKey: idem, CorrelationID: "corr-" + id}
}

func TestInMemoryEventStore(t *testing.T) {
	t.Run("append rejects duplicate idempotency key per (kb_id, kind)", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()

		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb1", EventDocumentUpsert, "idem-1")))
		err := s.Append(ctx, mkEvent("e2", "kb1", EventDocumentUpsert, "idem-1"))
		require.ErrorIs(t, err, ErrDuplicateKey)

		require.NoError(
			t,
			s.Append(ctx, mkEvent("e3", "kb2", EventDocumentUpsert, "idem-1")),
			"different kb_id is not a duplicate",
		)
		require.NoError(
			t,
			s.Append(ctx, mkEvent("e4", "kb1", EventMediaUpload, "idem-1")),
			"different kind is not a duplicate",
		)
	})

	t.Run("claim then ack moves event through pending -> claimed -> done", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		firstEvent := mkEvent("e1", "kb1", EventDocumentChunked, "k1")
		firstEvent.Payload = []byte("large transient payload")
		require.NoError(t, s.Append(ctx, firstEvent))
		require.NoError(t, s.Append(ctx, mkEvent("e2", "kb1", EventDocumentChunked, "k2")))

		first, err := s.Claim(ctx, EventDocumentChunked, "worker-1", time.Minute)
		require.NoError(t, err)
		require.Equal(t, "e1", first.EventID, "older event claimed first")
		require.Equal(t, EventStatusClaimed, first.Status)
		require.Equal(t, 1, first.Attempt)

		require.NoError(t, s.Ack(ctx, first.EventID))
		got, err := s.Get(ctx, first.EventID)
		require.NoError(t, err)
		require.Equal(t, EventStatusDone, got.Status)
		require.Empty(t, got.Payload, "acked payload should be released")

		second, err := s.Claim(ctx, EventDocumentChunked, "worker-1", time.Minute)
		require.NoError(t, err)
		require.Equal(t, "e2", second.EventID)

		_, err = s.Claim(ctx, EventDocumentChunked, "worker-1", time.Minute)
		require.ErrorIs(t, err, ErrNoneAvailable)
	})

	t.Run("fail past max attempts dead-letters the event", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		ev := mkEvent("e1", "kb1", EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		ev.Payload = []byte("large failed payload")
		require.NoError(t, s.Append(ctx, ev))

		for attempt := 1; attempt <= 3; attempt++ {
			claimed, err := s.Claim(ctx, EventDocumentEmbedded, "w", time.Minute)
			require.NoError(t, err)
			require.Equal(t, attempt, claimed.Attempt)
			require.NoError(t, s.Fail(ctx, "e1", claimed.Attempt, "bang"))
		}

		got, err := s.Get(ctx, "e1")
		require.NoError(t, err)
		require.Equal(t, EventStatusDead, got.Status)
		require.Equal(t, "bang", got.LastError)
		require.Empty(t, got.Payload, "dead event payload should be released")
	})

	t.Run("reaper returns expired claims to pending", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb1", EventDocumentUpsert, "k")))

		claimed, err := s.Claim(ctx, EventDocumentUpsert, "w", 10*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, EventStatusClaimed, claimed.Status)

		// Simulate worker death: visibility timeout passes, reaper runs.
		count, err := s.Requeue(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, count)

		got, err := s.Get(ctx, "e1")
		require.NoError(t, err)
		require.Equal(t, EventStatusPending, got.Status)
		require.Empty(t, got.ClaimedBy)
	})

	t.Run("reaper requeues absurdly far-future ClaimedUntil (M1 clock skew)", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentUpsert, "k")))
		c, err := s.Claim(ctx, EventDocumentUpsert, "w", 365*24*time.Hour)
		require.NoError(t, err)

		// Simulate clock skew: some peer wrote a ClaimedUntil far past the
		// believable future. Without the skew guard, this event is trapped.
		n, err := s.Requeue(ctx, time.Now())
		require.NoError(t, err)
		require.Equal(t, 1, n)
		got, _ := s.Get(ctx, c.EventID)
		require.Equal(t, EventStatusPending, got.Status)
	})

	t.Run("Fail rejects stale observed attempt (H6)", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentEmbedded, "k")))

		c1, err := s.Claim(ctx, EventDocumentEmbedded, "w1", 10*time.Millisecond)
		require.NoError(t, err)

		// Reaper comes in and requeues. another worker re-claims, advancing attempt.
		_, err = s.Requeue(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		_, err = s.Claim(ctx, EventDocumentEmbedded, "w2", time.Minute)
		require.NoError(t, err)

		// First worker tries to Fail with its stale attempt observation.
		err = s.Fail(ctx, "e1", c1.Attempt, "boom")
		require.ErrorIs(t, err, ErrStateChanged)
	})

	t.Run("Cleanup removes terminal events and idempotency index (M2)", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		clock := &testClock{now: time.Now().Add(-48 * time.Hour)}
		s.SetClock(clock)
		require.NoError(t, s.Append(ctx, mkEvent("e-old", "kb", EventDocumentUpsert, "k1")))
		clock.Set(time.Now())
		require.NoError(t, s.Append(ctx, mkEvent("e-new", "kb", EventDocumentUpsert, "k2")))

		// mark old as done. backdate it so Cleanup's cutoff matches
		require.NoError(t, s.Ack(ctx, "e-old"))

		n, err := s.Cleanup(ctx, time.Now().Add(-24*time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, n)

		_, err = s.Get(ctx, "e-old")
		require.ErrorIs(t, err, ErrNotFound)
		// k1 should now be reusable because we pruned the idempotency index.
		require.NoError(t, s.Append(ctx, mkEvent("e-fresh", "kb", EventDocumentUpsert, "k1")))
	})

	t.Run("ListUnfinishedBefore paginates and emits continuation token", func(t *testing.T) {
		s := NewInMemoryStore()
		ctx := context.Background()
		for i := range 5 {
			ev := mkEvent(fmt.Sprintf("e-%d", i), "kb", EventDocumentUpsert, fmt.Sprintf("k-%d", i))
			ev.CreatedAt = time.Now().Add(time.Duration(i) * time.Millisecond)
			require.NoError(t, s.Append(ctx, ev))
		}
		page, next, err := s.ListUnfinishedBefore(ctx, EventDocumentUpsert, time.Time{}, "", 2)
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.NotEmpty(t, next)

		page2, next2, err := s.ListUnfinishedBefore(ctx, EventDocumentUpsert, time.Time{}, next, 2)
		require.NoError(t, err)
		require.Len(t, page2, 2)
		require.NotEmpty(t, next2)

		page3, next3, err := s.ListUnfinishedBefore(ctx, EventDocumentUpsert, time.Time{}, next2, 2)
		require.NoError(t, err)
		require.Len(t, page3, 1)
		require.Empty(t, next3)
	})
}

func TestInMemoryEventInbox(t *testing.T) {
	t.Run("mark processed dedups per (worker, idempotency key)", func(t *testing.T) {
		in := NewInMemoryEventInbox()
		ctx := context.Background()
		require.NoError(t, in.MarkProcessed(ctx, "worker-1", "idem-1", "e1"))
		processed, err := in.Processed(ctx, "worker-1", "idem-1")
		require.NoError(t, err)
		require.True(t, processed)
		err = in.MarkProcessed(ctx, "worker-1", "idem-1", "e1")
		require.True(t, IsInboxDuplicate(err))

		// Same idempotency key on a different worker is not a dup.
		require.NoError(t, in.MarkProcessed(ctx, "worker-2", "idem-1", "e1"))
	})

	t.Run("cleanup evicts records older than cutoff", func(t *testing.T) {
		in := NewInMemoryEventInbox()
		ctx := context.Background()
		clock := &testClock{now: time.Now().Add(-30 * 24 * time.Hour)}
		in.SetClock(clock)
		require.NoError(t, in.MarkProcessed(ctx, "w", "old", "e1"))
		clock.Set(time.Now())
		require.NoError(t, in.MarkProcessed(ctx, "w", "new", "e2"))

		count, err := in.Cleanup(ctx, time.Now().Add(-7*24*time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Equal(t, 1, in.Size())
	})
}
