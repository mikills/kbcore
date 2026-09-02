package duckdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
	kb "github.com/mikills/minnow/kb"
)

func TestFormatUsesBudget(t *testing.T) {
	t.Run("embedTexts draws on the process budget", func(t *testing.T) {
		// One slot, so a second batch can only run if the budget is consulted.
		m := budget.New(memlimit.Plan{}, false)
		m.SetEmbedBudgetForTest(1)

		var inFlight, peak atomic.Int64
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
			Budget:           m,
			EmbedParallelism: 8,
			EmbedBatch: func(_ context.Context, in []string) ([][]float32, error) {
				current := inFlight.Add(1)
				defer inFlight.Add(-1)
				for {
					high := peak.Load()
					if current <= high || peak.CompareAndSwap(high, current) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				out := make([][]float32, len(in))
				for i := range out {
					out[i] = []float32{1}
				}
				return out, nil
			},
		}}
		docs := make([]kb.Document, 320)
		idx := make([]int, len(docs))
		for i := range docs {
			docs[i] = kb.Document{ID: "d", Text: "t"}
			idx[i] = i
		}
		vectors, err := f.embedTexts(context.Background(), docs, idx)
		require.NoError(t, err)
		require.Len(t, vectors, len(docs))
		require.Equal(t, int64(1), peak.Load(), "the budget was not consulted")
	})

	t.Run("a cancelled context is reported, not silently skipped", func(t *testing.T) {
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
			EmbedBatch: func(context.Context, []string) ([][]float32, error) {
				return nil, errors.New("should not be called")
			},
		}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		vectors, err := f.embedTexts(ctx, []kb.Document{{ID: "a", Text: "a"}}, []int{0})
		require.ErrorIs(t, err, context.Canceled, "cancellation must not read as a bad embedder")
		require.Nil(t, vectors)
	})

	t.Run("an open database takes a share and gives it back", func(t *testing.T) {
		plan, err := memlimit.Limit{Ceiling: 16 << 30}.Divide(memlimit.Shape{Rows: 75000, Dimensions: 512}, budget.CachedReaders, 0)
		require.NoError(t, err)
		m := budget.New(plan, true)
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{Budget: m, MemoryLimit: "256MB"}}

		require.Zero(t, m.LiveDatabases())
		db, err := f.openConfiguredDB(t.Context(), t.TempDir()+"/a.duckdb")
		require.NoError(t, err)
		require.Equal(t, int64(1), m.LiveDatabases())

		var limit string
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT current_setting('memory_limit')`).Scan(&limit))
		require.NotEqual(t, "256MB", limit, "the budget's share never reached duckdb")

		require.NoError(t, db.Close())
		require.Zero(t, m.LiveDatabases(), "closing did not return the share")
	})
}
