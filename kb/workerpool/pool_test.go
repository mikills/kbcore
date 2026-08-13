package workerpool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/eventing"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	t.Run("pending event is handled once", func(t *testing.T) {
		store := eventing.NewInMemoryStore()
		inbox := eventing.NewInMemoryEventInbox()
		evt := eventing.Event{
			EventID:   "e1",
			KBID:      "kb",
			Kind:      eventing.EventDocumentUpsert,
			Status:    eventing.EventStatusPending,
			CreatedAt: time.Now(),
		}
		require.NoError(t, store.Append(context.Background(), evt))
		worker := &stubWorker{kind: eventing.EventDocumentUpsert, id: "w1"}
		pool, err := NewWorkerPool(worker, store, inbox, WorkerPoolConfig{Concurrency: 1})
		require.NoError(t, err)

		id, err := pool.HandleOnce(context.Background())

		require.NoError(t, err)
		require.Equal(t, "e1", id)
		require.True(t, worker.called)
	})

	t.Run("after terminal runs after done transition", func(t *testing.T) {
		store := eventing.NewInMemoryStore()
		evt := eventing.Event{
			EventID: "e1", KBID: "kb", Kind: eventing.EventDocumentUpsert,
			Status: eventing.EventStatusPending, CreatedAt: time.Now(),
		}
		require.NoError(t, store.Append(context.Background(), evt))
		worker := &stubWorker{kind: eventing.EventDocumentUpsert, id: "w1"}
		worker.result.AfterTerminal = func(ctx context.Context) error {
			stored, err := store.Get(ctx, "e1")
			require.NoError(t, err)
			require.Equal(t, eventing.EventStatusDone, stored.Status)
			return nil
		}
		pool, err := NewWorkerPool(worker, store, eventing.NewInMemoryEventInbox(), WorkerPoolConfig{})
		require.NoError(t, err)
		_, err = pool.HandleOnce(context.Background())
		require.NoError(t, err)
	})

	t.Run("after terminal waits through retry and runs after dead", func(t *testing.T) {
		store := eventing.NewInMemoryStore()
		evt := eventing.Event{
			EventID: "e1", KBID: "kb", Kind: eventing.EventDocumentUpsert,
			Status: eventing.EventStatusPending, CreatedAt: time.Now(), MaxAttempts: 2,
		}
		require.NoError(t, store.Append(context.Background(), evt))
		cleanupCalls := 0
		worker := &stubWorker{
			kind: eventing.EventDocumentUpsert, id: "w1", handleErr: errors.New("planned"),
			result: WorkerResult{AfterTerminal: func(context.Context) error { cleanupCalls++; return nil }},
		}
		pool, err := NewWorkerPool(worker, store, eventing.NewInMemoryEventInbox(), WorkerPoolConfig{})
		require.NoError(t, err)
		_, err = pool.HandleOnce(context.Background())
		require.Error(t, err)
		require.Zero(t, cleanupCalls)
		_, err = pool.HandleOnce(context.Background())
		require.Error(t, err)
		require.Equal(t, 1, cleanupCalls)
		stored, err := store.Get(context.Background(), "e1")
		require.NoError(t, err)
		require.Equal(t, eventing.EventStatusDead, stored.Status)
	})
}

type stubWorker struct {
	kind      eventing.EventKind
	id        string
	called    bool
	result    WorkerResult
	handleErr error
}

func (w *stubWorker) Kind() eventing.EventKind { return w.kind }
func (w *stubWorker) WorkerID() string         { return w.id }
func (w *stubWorker) Handle(context.Context, *eventing.Event) (WorkerResult, error) {
	w.called = true
	return w.result, w.handleErr
}
