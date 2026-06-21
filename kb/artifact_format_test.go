package kb_test

import (
	"context"
	"testing"

	. "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestArtifactFormatRegistration(t *testing.T) {
	t.Run("rejects invalid formats", func(t *testing.T) {
		kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir())
		require.ErrorIs(t, kb.RegisterFormat(nil), ErrInvalidArtifactFormat)
		require.False(t, kb.HasFormat())
		require.ErrorIs(t, kb.RegisterFormat(&stubArtifactFormat{}), ErrInvalidArtifactFormat)
		require.False(t, kb.HasFormat())
	})

	t.Run("search uses registered artifact format", func(t *testing.T) {
		format := &stubArtifactFormat{kind: "mock", resultID: "from-a"}
		kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithArtifactFormat(format))
		seedManifest(
			t,
			context.Background(),
			kb.BlobStore,
			"kb-a",
			[]SnapshotShardMetadata{{ShardID: "s1", Key: "snapshots/kb-a/s1.duckdb"}},
		)
		results, err := kb.Search(context.Background(), "kb-a", []float32{1}, &SearchOptions{TopK: 1})
		require.NoError(t, err)
		require.Equal(t, "from-a", results[0].ID)
	})
}

type stubArtifactFormat struct {
	kind     string
	resultID string
}

func (f *stubArtifactFormat) Kind() string    { return f.kind }
func (f *stubArtifactFormat) Version() int    { return 1 }
func (f *stubArtifactFormat) FileExt() string { return ".stub" }
func (f *stubArtifactFormat) BuildArtifacts(context.Context, string, string, int64) ([]SnapshotShardMetadata, error) {
	return nil, nil
}
func (f *stubArtifactFormat) QueryRag(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
	return []ExpandedResult{{ID: f.resultID}}, nil
}
func (f *stubArtifactFormat) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (f *stubArtifactFormat) QueryBM25(context.Context, BM25QueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (f *stubArtifactFormat) Ingest(context.Context, IngestUpsertRequest) (IngestResult, error) {
	return IngestResult{}, nil
}
func (f *stubArtifactFormat) Delete(context.Context, IngestDeleteRequest) (IngestResult, error) {
	return IngestResult{}, nil
}
