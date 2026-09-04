package duckdb_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
)

type countingEmbedder struct {
	dim     int
	single  atomic.Int64
	batches atomic.Int64
	vectors atomic.Int64

	// inFlight tracks concurrency; peak keeps the high-water mark.
	inFlight atomic.Int64
	peak     atomic.Int64

	// gate, when set, blocks every batch until it is closed.
	gate chan struct{}
	// failOn returns an error for a batch containing this text.
	failOn string
	// slowOn delays a batch containing this text.
	slowOn string
	delay  time.Duration
}

// vecFor encodes the input so a misrouted vector is detectable.
func vecFor(dim int, input string) []float32 {
	v := make([]float32, dim)
	n, err := strconv.Atoi(strings.TrimPrefix(input, "doc-"))
	if err != nil {
		n = len(input)
	}
	v[0] = float32(n)
	return v
}

func (e *countingEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	e.single.Add(1)
	return vecFor(e.dim, input), nil
}

func (e *countingEmbedder) EmbedBatch(ctx context.Context, in []string) ([][]float32, error) {
	current := e.inFlight.Add(1)
	for {
		peak := e.peak.Load()
		if current <= peak || e.peak.CompareAndSwap(peak, current) {
			break
		}
	}
	defer e.inFlight.Add(-1)

	e.batches.Add(1)
	e.vectors.Add(int64(len(in)))

	if e.gate != nil {
		select {
		case <-e.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	for _, input := range in {
		if e.failOn != "" && input == e.failOn {
			return nil, errors.New("embedder refused this batch")
		}
		if e.slowOn != "" && input == e.slowOn {
			select {
			case <-time.After(e.delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	out := make([][]float32, len(in))
	for i, input := range in {
		out[i] = vecFor(e.dim, input)
	}
	return out, nil
}

func newBatchKB(t *testing.T, emb kb.Embedder, opts ...duckdb.DepOption) *kb.KB {
	t.Helper()
	k := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
		kb.WithEmbedder(emb),
		kb.WithManifestStore(&kb.BlobManifestStore{Store: &kb.LocalBlobStore{Root: t.TempDir()}}),
	)
	opts = append([]duckdb.DepOption{duckdb.WithMemoryLimit("256MB")}, opts...)
	af, err := duckdb.NewArtifactFormat(duckdb.NewDepsFromKB(k, opts...))
	require.NoError(t, err)
	require.NoError(t, k.RegisterFormat(af))
	return k
}

func numberedDocs(n int) []kb.Document {
	docs := make([]kb.Document, n)
	for i := range docs {
		text := fmt.Sprintf("doc-%06d", i)
		docs[i] = kb.Document{ID: text, Text: text}
	}
	return docs
}

func TestUpsertEmbedBatching(t *testing.T) {
	t.Run("batches instead of one call per document", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8}
		k := newBatchKB(t, emb)
		require.NoError(t, k.UpsertDocsAndUpload(t.Context(), "kb", numberedDocs(100)))

		require.Equal(t, int64(100), emb.vectors.Load())
		require.Equal(t, int64(0), emb.single.Load(), "no per-document round trips")
		require.Equal(t, int64(4), emb.batches.Load(), "100 documents in batches of 32")
	})

	t.Run("a byte cap splits a batch below the count", func(t *testing.T) {
		const size = (256 << 10) / 3
		emb := &countingEmbedder{dim: 8}
		k := newBatchKB(t, emb)
		docs := make([]kb.Document, 9)
		for i := range docs {
			docs[i] = kb.Document{ID: fmt.Sprintf("d-%03d", i), Text: strings.Repeat("x", size)}
		}
		require.NoError(t, k.UpsertDocsAndUpload(t.Context(), "kb", docs))
		require.Equal(t, int64(3), emb.batches.Load())
	})

	t.Run("every document keeps its own vector", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8}
		k := newBatchKB(t, emb)
		docs := numberedDocs(200)
		require.NoError(t, k.UpsertDocsAndUpload(t.Context(), "kb", docs))

		// A vector encodes its input, so searching for one input's vector must
		// land on that document. A cross-batch mix-up moves the nearest hit.
		for _, want := range []int{0, 31, 32, 99, 199} {
			hits, err := k.SearchRaw(t.Context(), "kb", vecFor(8, docs[want].Text), 1, nil)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			require.Equal(t, docs[want].ID, hits[0].ID)
			require.Zero(t, hits[0].Distance)
		}
	})

	t.Run("a pre-set embedding is not re-embedded", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8}
		k := newBatchKB(t, emb)
		docs := numberedDocs(40)
		for i := range docs[:10] {
			docs[i].Embedding = vecFor(8, docs[i].Text)
		}
		require.NoError(t, k.UpsertDocsAndUpload(t.Context(), "kb", docs))
		require.Equal(t, int64(30), emb.vectors.Load(), "only the 30 without embeddings")
	})

	t.Run("an empty document is rejected before any call", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8}
		k := newBatchKB(t, emb)
		docs := numberedDocs(5)
		docs[2].Text = "   "
		err := k.UpsertDocsAndUpload(t.Context(), "kb", docs)
		require.ErrorContains(t, err, "text or embedding is required")
		require.Equal(t, int64(0), emb.batches.Load(), "validation precedes embedding")
	})
}

func TestUpsertEmbedConcurrency(t *testing.T) {
	t.Run("batches run at the same time", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8, gate: make(chan struct{})}
		k := newBatchKB(t, emb, duckdb.WithEmbedParallelism(4))

		done := make(chan error, 1)
		go func() { done <- k.UpsertDocsAndUpload(t.Context(), "kb", numberedDocs(128)) }()

		require.Eventually(t, func() bool { return emb.inFlight.Load() > 1 },
			10*time.Second, 5*time.Millisecond, "no two batches were ever in flight together")
		close(emb.gate)
		require.NoError(t, <-done)
		require.Greater(t, emb.peak.Load(), int64(1))
	})

	t.Run("never exceeds the configured parallelism", func(t *testing.T) {
		// Every batch blocks, so without a limit all ten would be in flight at
		// once. Holding them is what makes the assertion independent of timing.
		const limit = 2
		emb := &countingEmbedder{dim: 8, gate: make(chan struct{})}
		k := newBatchKB(t, emb, duckdb.WithEmbedParallelism(limit))

		done := make(chan error, 1)
		go func() { done <- k.UpsertDocsAndUpload(t.Context(), "kb", numberedDocs(320)) }()

		require.Eventually(t, func() bool { return emb.inFlight.Load() == limit },
			10*time.Second, 5*time.Millisecond, "never reached the limit")
		// Give any unbounded goroutine room to pile in before checking.
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, int64(limit), emb.inFlight.Load(), "more batches in flight than allowed")

		close(emb.gate)
		require.NoError(t, <-done)
		require.Equal(t, int64(limit), emb.peak.Load())
	})

	t.Run("parallelism of one stays sequential", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8, gate: make(chan struct{})}
		k := newBatchKB(t, emb, duckdb.WithEmbedParallelism(1))

		done := make(chan error, 1)
		go func() { done <- k.UpsertDocsAndUpload(t.Context(), "kb", numberedDocs(160)) }()

		require.Eventually(t, func() bool { return emb.inFlight.Load() == 1 },
			10*time.Second, 5*time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, int64(1), emb.inFlight.Load())

		close(emb.gate)
		require.NoError(t, <-done)
		require.Equal(t, int64(1), emb.peak.Load())
	})

	t.Run("one failed batch fails the upsert", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8, failOn: "doc-000100"}
		k := newBatchKB(t, emb, duckdb.WithEmbedParallelism(4))
		err := k.UpsertDocsAndUpload(t.Context(), "kb", numberedDocs(320))
		require.ErrorContains(t, err, "embedder refused this batch")
	})

	t.Run("a cancelled context stops the batches", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8, gate: make(chan struct{})}
		k := newBatchKB(t, emb, duckdb.WithEmbedParallelism(4))
		ctx, cancel := context.WithCancel(t.Context())

		done := make(chan error, 1)
		go func() { done <- k.UpsertDocsAndUpload(ctx, "kb", numberedDocs(320)) }()
		require.Eventually(t, func() bool { return emb.batches.Load() > 0 },
			10*time.Second, 5*time.Millisecond)
		cancel()

		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("upsert did not return after cancellation")
		}
		close(emb.gate)
	})

	t.Run("concurrent upserts to different knowledge bases", func(t *testing.T) {
		emb := &countingEmbedder{dim: 8}
		k := newBatchKB(t, emb, duckdb.WithEmbedParallelism(4))
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = k.UpsertDocsAndUpload(t.Context(), fmt.Sprintf("kb-%d", i), numberedDocs(64))
			}()
		}
		wg.Wait()
		for i, err := range errs {
			require.NoErrorf(t, err, "knowledge base %d", i)
		}
		require.Equal(t, int64(4*64), emb.vectors.Load())
	})
}
