package workerpool

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/eventing"
	"github.com/stretchr/testify/require"
)

type logCapture struct {
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, record slog.Record) error {
	c.records = append(c.records, record)
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *logCapture) WithGroup(string) slog.Handler { return c }

func (c *logCapture) messages() []string {
	out := make([]string, 0, len(c.records))
	for _, record := range c.records {
		out = append(out, record.Message)
	}
	return out
}

func (c *logCapture) levels() []slog.Level {
	out := make([]slog.Level, 0, len(c.records))
	for _, record := range c.records {
		out = append(out, record.Level)
	}
	return out
}

func captureWorkerLogs(t *testing.T) *logCapture {
	t.Helper()
	capture := &logCapture{}
	previous := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return capture
}

func runFailingHandler(t *testing.T, maxAttempts int) *logCapture {
	t.Helper()
	store := eventing.NewInMemoryStore()
	evt := eventing.Event{
		EventID: "e1", KBID: "kb-under-test", Kind: eventing.EventDocumentUpsert,
		Status: eventing.EventStatusPending, CreatedAt: time.Now(), MaxAttempts: maxAttempts,
	}
	require.NoError(t, store.Append(context.Background(), evt))
	worker := &stubWorker{kind: eventing.EventDocumentUpsert, id: "w1", handleErr: errors.New("boom")}
	pool, err := NewWorkerPool(worker, store, eventing.NewInMemoryEventInbox(), WorkerPoolConfig{})
	require.NoError(t, err)

	capture := captureWorkerLogs(t)
	_, err = pool.HandleOnce(context.Background())
	require.Error(t, err)
	return capture
}

func TestWorkerFailureLogging(t *testing.T) {
	t.Run("a retryable failure logs once at warn", func(t *testing.T) {
		capture := runFailingHandler(t, 3)

		require.Equal(t, []string{"worker event failed"}, capture.messages())
		require.Equal(t, []slog.Level{slog.LevelWarn}, capture.levels())

		attrs := map[string]string{}
		capture.records[0].Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		require.Equal(t, "kb-under-test", attrs[logKeyKBID])
		require.Equal(t, "e1", attrs[logKeyEventID])
		require.Contains(t, attrs[logKeyError], "boom")
		require.Equal(t, "3", attrs["max_attempts"])
	})

	t.Run("the final attempt also logs a terminal error", func(t *testing.T) {
		capture := runFailingHandler(t, 1)

		require.Equal(t,
			[]string{"worker event failed", "worker event failed; giving up"},
			capture.messages(),
		)
		require.Equal(t, []slog.Level{slog.LevelWarn, slog.LevelError}, capture.levels())
	})
}
