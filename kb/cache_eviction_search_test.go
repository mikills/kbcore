package kb_test

import (
	"context"
	"hash/fnv"
	"math"
	"path/filepath"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCacheEvictionViaSearch(t *testing.T) {
	makeMock := func() *cacheSearchMockFormat {
		upserted := map[string]string{}
		return &cacheSearchMockFormat{
			ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
				for _, doc := range req.Docs {
					upserted[doc.ID] = doc.Text
				}
				return IngestResult{MutatedCount: len(req.Docs)}, nil
			},
			queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
				var results []ExpandedResult
				for id, text := range upserted {
					results = append(results, ExpandedResult{ID: id, Content: text, Distance: 0.01})
				}
				if req.Options.TopK > 0 && len(results) > req.Options.TopK {
					results = results[:req.Options.TopK]
				}
				return results, nil
			},
		}
	}

	t.Run("budget_enforcement", func(t *testing.T) {
		ctx := context.Background()
		embedder := newCacheSearchEmbedder(32)
		mock := makeMock()
		loader := newCacheSearchKB(t, mock, WithEmbedder(embedder))

		tenantID := "tenant-budget"
		require.NoError(
			t,
			loader.UpsertDocsAndUpload(
				ctx,
				tenantID,
				[]Document{{ID: tenantID + "-a", Text: "alpha"}, {ID: tenantID + "-b", Text: "bravo"}},
			),
		)
		q, err := loader.Embed(ctx, "alpha")
		require.NoError(t, err)

		loader.SetMaxCacheBytes(1)
		mock.queryRagFn = func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
			return nil, ErrCacheBudgetExceeded
		}
		_, err = loader.Search(ctx, tenantID, q, &SearchOptions{TopK: 2})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCacheBudgetExceeded)

		loader.SetMaxCacheBytes(16 * 1024 * 1024)
		mock.queryRagFn = func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
			return []ExpandedResult{{ID: tenantID + "-a", Distance: 0.01}}, nil
		}
		res, err := loader.Search(ctx, tenantID, q, &SearchOptions{TopK: 2})
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("eviction_behavior", func(t *testing.T) {
		ctx := context.Background()
		embedder := newCacheSearchEmbedder(32)
		mock := makeMock()
		loader := newCacheSearchKB(t, mock, WithEmbedder(embedder))

		tenantIDs := []string{"tenant-evict-a", "tenant-evict-b"}
		for _, tenantID := range tenantIDs {
			require.NoError(t, loader.UpsertDocsAndUpload(ctx, tenantID, []Document{{
				ID:   tenantID + "-1",
				Text: "payload for " + tenantID,
			}}))
			q, err := loader.Embed(ctx, "payload for "+tenantID)
			require.NoError(t, err)
			_, err = loader.Search(ctx, tenantID, q, &SearchOptions{TopK: 1})
			require.NoError(t, err)
		}

		loader.SetCacheEntryTTL(20 * time.Millisecond)
		time.Sleep(60 * time.Millisecond)
		require.NoError(t, loader.SweepCache(ctx))

		for _, tenantID := range tenantIDs {
			q, err := loader.Embed(ctx, "payload for "+tenantID)
			require.NoError(t, err)
			res, err := loader.Search(ctx, tenantID, q, &SearchOptions{TopK: 1})
			require.NoError(t, err)
			require.NotEmpty(t, res, "search should succeed after cache eviction for %s", tenantID)
		}
	})
}

type cacheSearchMockFormat struct {
	queryRagFn func(context.Context, RagQueryRequest) ([]ExpandedResult, error)
	ingestFn   func(context.Context, IngestUpsertRequest) (IngestResult, error)
}

func (m *cacheSearchMockFormat) Kind() string    { return "mock" }
func (m *cacheSearchMockFormat) Version() int    { return 1 }
func (m *cacheSearchMockFormat) FileExt() string { return ".mock" }

func (m *cacheSearchMockFormat) BuildArtifacts(
	context.Context,
	string,
	string,
	int64,
) ([]SnapshotShardMetadata, error) {
	return nil, nil
}
func (m *cacheSearchMockFormat) QueryRag(ctx context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
	return m.queryRagFn(ctx, req)
}
func (m *cacheSearchMockFormat) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (m *cacheSearchMockFormat) QueryBM25(context.Context, BM25QueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (m *cacheSearchMockFormat) Ingest(ctx context.Context, req IngestUpsertRequest) (IngestResult, error) {
	return m.ingestFn(ctx, req)
}
func (m *cacheSearchMockFormat) Delete(context.Context, IngestDeleteRequest) (IngestResult, error) {
	return IngestResult{}, nil
}

func newCacheSearchKB(t *testing.T, format ArtifactFormat, opts ...KBOption) *KB {
	t.Helper()
	all := []KBOption{WithArtifactFormat(format)}
	all = append(all, opts...)
	return NewKB(&LocalBlobStore{Root: filepath.Join(t.TempDir(), "blob")}, filepath.Join(t.TempDir(), "cache"), all...)
}

type cacheSearchEmbedder struct{ dim int }

func newCacheSearchEmbedder(dim int) Embedder { return cacheSearchEmbedder{dim: dim} }
func (e cacheSearchEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(input))
	seed := h.Sum32()
	vec := make([]float32, e.dim)
	for i := range vec {
		vec[i] = float32((seed>>uint(i%24))&0xff) + 1
	}
	var sum float64
	for _, v := range vec {
		sum += float64(v * v)
	}
	n := float32(math.Sqrt(sum))
	if n == 0 {
		return vec, nil
	}
	for i := range vec {
		vec[i] /= n
	}
	return vec, nil
}
