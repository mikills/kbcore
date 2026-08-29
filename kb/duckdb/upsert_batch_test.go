package duckdb_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
)

type countingEmbedder struct {
	dim     int
	single  atomic.Int64
	batches atomic.Int64
	vectors atomic.Int64
}

func (e *countingEmbedder) vec() []float32 {
	v := make([]float32, e.dim)
	v[0] = 1
	return v
}

func (e *countingEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.single.Add(1)
	return e.vec(), nil
}

func (e *countingEmbedder) EmbedBatch(_ context.Context, in []string) ([][]float32, error) {
	e.batches.Add(1)
	e.vectors.Add(int64(len(in)))
	out := make([][]float32, len(in))
	for i := range out {
		out[i] = e.vec()
	}
	return out, nil
}

func TestUpsertBatchesEmbeddings(t *testing.T) {
	const docCount = 100
	emb := &countingEmbedder{dim: 8}
	k := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
		kb.WithEmbedder(emb),
		kb.WithManifestStore(&kb.BlobManifestStore{Store: &kb.LocalBlobStore{Root: t.TempDir()}}),
	)
	af, err := duckdb.NewArtifactFormat(duckdb.NewDepsFromKB(k,
		duckdb.WithMemoryLimit("256MB"),
	))
	require.NoError(t, err)
	require.NoError(t, k.RegisterFormat(af))

	docs := make([]kb.Document, docCount)
	for i := range docs {
		docs[i] = kb.Document{ID: fmt.Sprintf("d-%03d", i), Text: fmt.Sprintf("text %d", i)}
	}
	require.NoError(t, k.UpsertDocsAndUpload(t.Context(), "batch-kb", docs))

	require.Equal(t, int64(docCount), emb.vectors.Load(), "every document embedded once")
	require.Equal(t, int64(0), emb.single.Load(), "no per-document round trips")
	require.Equal(t, int64(4), emb.batches.Load(), "100 docs in batches of 32")
}

func TestUpsertBatchBytes(t *testing.T) {
	// Each doc is a third of the byte cap, so batches cap at 3 by size.
	const docCount, size = 9, (256 << 10) / 3
	emb := &countingEmbedder{dim: 8}
	k := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
		kb.WithEmbedder(emb),
		kb.WithManifestStore(&kb.BlobManifestStore{Store: &kb.LocalBlobStore{Root: t.TempDir()}}),
	)
	af, err := duckdb.NewArtifactFormat(duckdb.NewDepsFromKB(k, duckdb.WithMemoryLimit("256MB")))
	require.NoError(t, err)
	require.NoError(t, k.RegisterFormat(af))

	docs := make([]kb.Document, docCount)
	for i := range docs {
		docs[i] = kb.Document{ID: fmt.Sprintf("d-%03d", i), Text: strings.Repeat("x", size)}
	}
	require.NoError(t, k.UpsertDocsAndUpload(t.Context(), "bytes-kb", docs))

	require.Equal(t, int64(docCount), emb.vectors.Load())
	require.Equal(t, int64(0), emb.single.Load())
	require.Equal(t, int64(3), emb.batches.Load(), "byte cap splits 9 docs into 3 batches, not 1")
}
