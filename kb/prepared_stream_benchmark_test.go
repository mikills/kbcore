package kb

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkPreparedStreamPublish(b *testing.B) {
	ctx := context.Background()
	for _, count := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("embedded_docs=%d", count), func(b *testing.B) {
			docs := benchEmbeddedDocs(count, 64)
			b.ReportAllocs()
			for b.Loop() {
				loader := NewKB(
					&LocalBlobStore{Root: b.TempDir()},
					b.TempDir(),
					WithArtifactFormat(&benchPreparedStreamer{}),
				)
				pos := 0
				err := loader.publishPreparedStream(ctx, PreparedStreamRequest{
					KBID: "kb",
					Next: func(context.Context) ([]EmbeddedDocument, error) {
						if pos >= len(docs) {
							return nil, nil
						}
						end := min(pos+128, len(docs))
						batch := docs[pos:end]
						pos = end
						return batch, nil
					},
				})
				require.NoError(b, err)
			}
		})
	}
}

type benchPreparedStreamer struct {
	published int
}

func (s *benchPreparedStreamer) Kind() string    { return "bench" }
func (s *benchPreparedStreamer) Version() int    { return 1 }
func (s *benchPreparedStreamer) FileExt() string { return ".bench" }

func (s *benchPreparedStreamer) BuildArtifacts(
	context.Context,
	string,
	string,
	int64,
) ([]SnapshotShardMetadata, error) {
	return nil, nil
}
func (s *benchPreparedStreamer) Ingest(context.Context, IngestUpsertRequest) (IngestResult, error) {
	return IngestResult{}, nil
}
func (s *benchPreparedStreamer) Delete(context.Context, IngestDeleteRequest) (IngestResult, error) {
	return IngestResult{}, nil
}
func (s *benchPreparedStreamer) QueryRag(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (s *benchPreparedStreamer) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (s *benchPreparedStreamer) QueryBM25(context.Context, BM25QueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}

func (s *benchPreparedStreamer) PublishPreparedStream(
	ctx context.Context,
	req PreparedStreamRequest,
) (IngestResult, error) {
	for {
		batch, err := req.Next(ctx)
		if err != nil || len(batch) == 0 {
			return IngestResult{MutatedCount: s.published}, err
		}
		s.published += len(batch)
	}
}

func benchEmbeddedDocs(count int, dim int) []EmbeddedDocument {
	docs := make([]EmbeddedDocument, 0, count)
	for i := range count {
		vec := make([]float32, dim)
		vec[i%dim] = 1
		docs = append(
			docs,
			EmbeddedDocument{ID: fmt.Sprintf("doc-%04d", i), Text: "normal document text", Embedding: vec},
		)
	}
	return docs
}
