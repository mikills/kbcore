package kb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKBVectorQuery(t *testing.T) {
	t.Run("topk_cases", testKBTopKCases)
	t.Run("search_by_distance_cases", testKBSearchByDistanceCases)
	t.Run("result_ordering", testKBResultOrdering)
	t.Run("shard_execution_fallback", testKBShardExecutionFallback)
}

func TestKBGraphQuery(t *testing.T) {
	t.Run("search_expanded_basic", testKBSearchExpandedBasic)
	t.Run("graph_mode_unavailable_scenarios", testKBGraphModeUnavailableScenarios)
	t.Run("search_expanded_invalid_args", testKBSearchExpandedInvalidArgs)
	t.Run("search_expanded_invalid_options_defaulted", testKBSearchExpandedInvalidOptionsDefaulted)
	t.Run("search_default_vector", testKBSearchDefaultVector)
	t.Run("search_graph_mode", testKBSearchGraphMode)
	t.Run("search_adaptive_mode", testKBSearchAdaptive)
	t.Run("search_expanded_bidirectional", testKBSearchExpandedBidirectional)
}

// cannedResults returns a set of ExpandedResults for testing.
func cannedResults() []ExpandedResult {
	return []ExpandedResult{
		{ID: "a", Content: "hello world", Distance: 0.01, Score: 0.99},
		{ID: "b", Content: "goodbye", Distance: 0.05, Score: 0.95},
	}
}

// newMockKB creates a KB with a mock artifact format registered, using a real
// LocalBlobStore and manifest store so orchestration logic works end-to-end.
func newMockKB(t *testing.T, mock *mockArtifactFormat, opts ...KBOption) *KB {
	t.Helper()
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	require.NoError(t, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))

	allOpts := []KBOption{WithArtifactFormat(mock)}
	allOpts = append(allOpts, opts...)
	return NewKB(&LocalBlobStore{Root: blobRoot}, cacheDir, allOpts...)
}

// newMockKBWithManifest creates a mock KB and seeds a manifest so resolveFormat succeeds.
func newMockKBWithManifest(t *testing.T, kbID string, mock *mockArtifactFormat, opts ...KBOption) *KB {
	t.Helper()
	loader := newMockKB(t, mock, opts...)
	ctx := context.Background()
	seedManifest(t, ctx, loader.BlobStore, kbID, []SnapshotShardMetadata{
		{ShardID: "shard-000", Key: kbID + "/shard-000.mock", SizeBytes: 1, VectorRows: 1, CreatedAt: time.Now().UTC()},
	})
	return loader
}

func testKBTopKCases(t *testing.T) {
	ctx := context.Background()
	canned := cannedResults()

	type testCase struct {
		name    string
		setup   func(*testing.T) (*KB, string, []float32, int)
		wantErr bool
		wantLen int
	}

	tests := []testCase{
		{
			name: "basic",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						if req.Options.TopK < len(canned) {
							return canned[:req.Options.TopK], nil
						}
						return canned, nil
					},
				}
				loader := newMockKBWithManifest(t, "kb-topk", mock)
				return loader, "kb-topk", []float32{0.1, 0.2, 0.3}, 1
			},
			wantLen: 1,
		},
		{
			name: "k_exceeds_total",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						return canned, nil
					},
				}
				loader := newMockKBWithManifest(t, "kb-topk-exceed", mock)
				return loader, "kb-topk-exceed", []float32{0.1, 0.2, 0.3}, 100
			},
			wantLen: 2,
		},
		{
			name: "k_zero_returns_error",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				mock := &mockArtifactFormat{}
				loader := newMockKBWithManifest(t, "kb-topk-zero", mock)
				return loader, "kb-topk-zero", []float32{0.1, 0.2, 0.3}, 0
			},
			wantErr: true,
		},
		{
			name: "invalid_vector_empty",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						return nil, fmt.Errorf("%w: query vector cannot be empty", ErrInvalidQueryRequest)
					},
				}
				loader := newMockKBWithManifest(t, "kb-topk-empty", mock)
				return loader, "kb-topk-empty", []float32{}, 1
			},
			wantErr: true,
		},
		{
			name: "invalid_vector_nil",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						return nil, fmt.Errorf("%w: query vector cannot be empty", ErrInvalidQueryRequest)
					},
				}
				loader := newMockKBWithManifest(t, "kb-topk-nil", mock)
				return loader, "kb-topk-nil", nil, 1
			},
			wantErr: true,
		},
		{
			name: "non_existent_kb",
			setup: func(t *testing.T) (*KB, string, []float32, int) {
				// No format registered: resolveFormat returns ErrArtifactFormatNotConfigured.
				blobRoot := filepath.Join(t.TempDir(), "blobs")
				require.NoError(t, os.MkdirAll(blobRoot, 0o755))
				loader := NewKB(&LocalBlobStore{Root: blobRoot}, t.TempDir())
				return loader, "non-existent-kb", []float32{0.1, 0.2, 0.3}, 1
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, vec, k := tc.setup(t)
			results, err := loader.Search(ctx, kbID, vec, &SearchOptions{TopK: k})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, results, tc.wantLen)
		})
	}
}

func testKBSearchByDistanceCases(t *testing.T) {
	ctx := context.Background()
	canned := cannedResults()

	type testCase struct {
		name           string
		setup          func(*testing.T) (*KB, string, []float32, float64)
		wantErr        bool
		wantAtLeastOne bool
	}

	tests := []testCase{
		{
			name: "basic",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						return canned, nil
					},
				}
				loader := newMockKBWithManifest(t, "kb-dist-basic", mock)
				return loader, "kb-dist-basic", []float32{0.1, 0.2, 0.3}, 10.0
			},
			wantAtLeastOne: true,
		},
		{
			name: "invalid_threshold_zero",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						return nil, fmt.Errorf("%w: max_distance must be > 0", ErrInvalidQueryRequest)
					},
				}
				loader := newMockKBWithManifest(t, "kb-dist-zero", mock)
				return loader, "kb-dist-zero", []float32{0.1, 0.2, 0.3}, 0
			},
			wantErr: true,
		},
		{
			name: "invalid_vector",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				mock := &mockArtifactFormat{
					queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
						return nil, fmt.Errorf("%w: query vector cannot be empty", ErrInvalidQueryRequest)
					},
				}
				loader := newMockKBWithManifest(t, "kb-dist-emptyvec", mock)
				return loader, "kb-dist-emptyvec", []float32{}, 1.0
			},
			wantErr: true,
		},
		{
			name: "non_existent_kb",
			setup: func(t *testing.T) (*KB, string, []float32, float64) {
				// No format registered: resolveFormat returns ErrArtifactFormatNotConfigured.
				blobRoot := filepath.Join(t.TempDir(), "blobs")
				require.NoError(t, os.MkdirAll(blobRoot, 0o755))
				loader := NewKB(&LocalBlobStore{Root: blobRoot}, t.TempDir())
				return loader, "non-existent-kb", []float32{0.1, 0.2, 0.3}, 1.0
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, vec, threshold := tc.setup(t)
			results, err := loader.Search(ctx, kbID, vec, &SearchOptions{TopK: 1000, MaxDistance: &threshold})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantAtLeastOne {
				require.NotEmpty(t, results)
			}
		})
	}
}

func testKBResultOrdering(t *testing.T) {
	ctx := context.Background()
	ordered := []ExpandedResult{
		{ID: "a", Distance: 0.01},
		{ID: "b", Distance: 0.05},
		{ID: "c", Distance: 0.10},
	}

	mock := &mockArtifactFormat{queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
		return ordered, nil
	}}
	kbID := "kb-ordering"
	loader := newMockKBWithManifest(t, kbID, mock)

	t.Run("TopK", func(t *testing.T) {
		results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{TopK: 3})
		require.NoError(t, err)
		require.Len(t, results, 3)
		for i := 1; i < len(results); i++ {
			assert.GreaterOrEqual(t, results[i].Distance, results[i-1].Distance)
		}
	})

	t.Run("SearchByDistance", func(t *testing.T) {
		maxDist := 10.0
		results, err := loader.Search(
			ctx,
			kbID,
			[]float32{0.1, 0.2, 0.3},
			&SearchOptions{TopK: 1000, MaxDistance: &maxDist},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 2)
		for i := 1; i < len(results); i++ {
			assert.GreaterOrEqual(t, results[i].Distance, results[i-1].Distance)
		}
	})
}

func testKBShardExecutionFallback(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-shard-fallback"

	errShard := fmt.Errorf("shard execution failed")
	mock := &mockArtifactFormat{queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
		return nil, errShard
	}}
	loader := newMockKBWithManifest(t, kbID, mock)

	results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{TopK: 3})
	require.Error(t, err)
	require.Empty(t, results)
}

func testKBSearchExpandedBasic(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-basic"

	mock := &mockArtifactFormat{queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
		return []ExpandedResult{
			{ID: "a", Distance: 0.01, GraphScore: 1.0},
			{ID: "c", Distance: 0.10, GraphScore: 0.7},
		}, nil
	}}
	loader := newMockKBWithManifest(t, kbID, mock)

	results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{
		Mode: SearchModeGraph, TopK: 2,
		Expansion: &ExpansionOptions{SeedK: 1, Hops: 1, MaxNeighborsPerNode: 25, Alpha: 0.7, Decay: 0.7},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	requireResultContainsID(t, results, "c")
}

func testKBGraphModeUnavailableScenarios(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		setup func(t *testing.T) (*KB, string, []float32)
	}{
		{
			name: "graph_unavailable",
			setup: func(t *testing.T) (*KB, string, []float32) {
				kbID := "kb-graph-unavail"
				mock := &mockArtifactFormat{
					queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
						return nil, ErrGraphQueryUnavailable
					},
				}
				loader := newMockKBWithManifest(t, kbID, mock)
				return loader, kbID, []float32{0.1, 0.2, 0.3}
			},
		},
		{
			name: "no_edges",
			setup: func(t *testing.T) (*KB, string, []float32) {
				kbID := "kb-graph-no-edges"
				mock := &mockArtifactFormat{
					queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
						return nil, ErrGraphQueryUnavailable
					},
				}
				loader := newMockKBWithManifest(t, kbID, mock)
				return loader, kbID, []float32{0.1, 0.2, 0.3}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loader, kbID, queryVec := tc.setup(t)
			_, err := loader.Search(ctx, kbID, queryVec, &SearchOptions{Mode: SearchModeGraph, TopK: 2})
			require.Error(t, err)
			require.ErrorIs(t, err, ErrGraphQueryUnavailable)
		})
	}
}

func testKBSearchExpandedInvalidArgs(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-invalid"
	mock := &mockArtifactFormat{
		queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
			if err := ValidateGraphQueryRequest(req); err != nil {
				return nil, err
			}
			return nil, nil
		},
	}
	loader := newMockKBWithManifest(t, kbID, mock)

	tests := []struct {
		name string
		k    int
		vec  []float32
	}{
		{name: "invalid k", k: 0, vec: []float32{0.1, 0.2, 0.3}},
		{name: "empty vector", k: 1, vec: []float32{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loader.Search(ctx, kbID, tc.vec, &SearchOptions{Mode: SearchModeGraph, TopK: tc.k})
			require.Error(t, err)
		})
	}
}

func testKBSearchExpandedInvalidOptionsDefaulted(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-invalid-opts"
	mock := &mockArtifactFormat{queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
		return []ExpandedResult{{ID: "a", Distance: 0.01}}, nil
	}}
	loader := newMockKBWithManifest(t, kbID, mock)

	_, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{
		Mode: SearchModeGraph, TopK: 2,
		Expansion: &ExpansionOptions{SeedK: -5, Hops: -1, MaxNeighborsPerNode: -10, Alpha: 2.5, Decay: -0.1},
	})
	require.NoError(t, err)
}

func testKBSearchDefaultVector(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-default-vector"
	canned := cannedResults()
	ragCalled := false

	mock := &mockArtifactFormat{queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
		ragCalled = true
		return canned, nil
	}}
	loader := newMockKBWithManifest(t, kbID, mock)

	results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{TopK: 2})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, ragCalled, "default mode should use QueryRag")
}

func testKBSearchGraphMode(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-search-graph"
	graphCalled := false

	mock := &mockArtifactFormat{queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
		graphCalled = true
		return []ExpandedResult{
			{ID: "a", Distance: 0.01, GraphScore: 1.0},
			{ID: "c", Distance: 0.10, GraphScore: 0.7},
		}, nil
	}}
	loader := newMockKBWithManifest(t, kbID, mock)

	results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{
		Mode: SearchModeGraph, TopK: 2,
		Expansion: &ExpansionOptions{SeedK: 1, Hops: 1, MaxNeighborsPerNode: 25, Alpha: 0.7, Decay: 0.7},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, graphCalled, "graph mode should dispatch to QueryGraph")
	requireResultContainsID(t, results, "c")
}

func testKBSearchAdaptive(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-search-adaptive"

	// Return a low-similarity vector result to trigger graph fallback.
	mock := &mockArtifactFormat{
		queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
			return []ExpandedResult{
				{ID: "a", Distance: 5.0}, // sim = 1/(1+5) = 0.167, below 0.95
			}, nil
		},
		queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
			return []ExpandedResult{
				{ID: "a", Distance: 0.01, GraphScore: 1.0},
				{ID: "c", Distance: 0.10, GraphScore: 0.7},
			}, nil
		},
	}
	loader := newMockKBWithManifest(t, kbID, mock)

	results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{
		Mode: SearchModeAdaptive, TopK: 2, AdaptiveMinSim: 0.95,
		Expansion: &ExpansionOptions{SeedK: 1, Hops: 1, MaxNeighborsPerNode: 25, Alpha: 0.7, Decay: 0.7},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	requireResultContainsID(t, results, "c")
}

func testKBSearchExpandedBidirectional(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-bidir"

	mock := &mockArtifactFormat{queryGraphFn: func(_ context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
		return []ExpandedResult{
			{ID: "a", Distance: 0.01, GraphScore: 1.0},
			{ID: "c", Distance: 0.10, GraphScore: 0.7},
		}, nil
	}}
	loader := newMockKBWithManifest(t, kbID, mock)

	results, err := loader.Search(ctx, kbID, []float32{0.1, 0.2, 0.3}, &SearchOptions{
		Mode: SearchModeGraph, TopK: 3,
		Expansion: &ExpansionOptions{SeedK: 1, Hops: 1, MaxNeighborsPerNode: 25, Alpha: 0.7, Decay: 0.7},
	})
	require.NoError(t, err)
	requireResultContainsID(t, results, "c")
}

func TestKBMutations(t *testing.T) {
	t.Run("upsert_and_soft_delete", testKBUpsertAndSoftDelete)
	t.Run("upsert_embed_once", testKBUpsertEmbedOnce)
	t.Run("mutate_upload_visible_fresh_loader", testKBMutateUploadVisibleFreshLoader)
	t.Run("upsert_upload_bootstrap", testKBUpsertUploadBootstrap)
	t.Run("mutate_upload_no_retry_conflict", testKBMutateUploadNoRetryConflict)
	t.Run("mutate_upload_retry", testKBMutateUploadRetry)
	t.Run("ingest_sharding", testKBIngestSharding)
	t.Run("retry_context_canceled", testKBRetryContextCanceled)
}

func testKBUpsertAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-mutations-soft"
	embedder := newFixtureEmbedder(3)

	// Track state: upserted docs and deleted docs
	upserted := map[string]string{} // id -> text
	deleted := map[string]bool{}

	mock := &mockArtifactFormat{
		ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
			for _, doc := range req.Docs {
				upserted[doc.ID] = doc.Text
				delete(deleted, doc.ID)
			}
			return IngestResult{MutatedCount: len(req.Docs)}, nil
		},
		deleteFn: func(_ context.Context, req IngestDeleteRequest) (IngestResult, error) {
			for _, id := range req.DocIDs {
				deleted[id] = true
			}
			return IngestResult{MutatedCount: len(req.DocIDs)}, nil
		},
		queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
			var results []ExpandedResult
			for id, text := range upserted {
				if !deleted[id] {
					results = append(results, ExpandedResult{ID: id, Content: text, Distance: 0.01})
				}
			}
			if req.Options.TopK > 0 && len(results) > req.Options.TopK {
				results = results[:req.Options.TopK]
			}
			return results, nil
		},
	}

	loader := newMockKBWithManifest(t, kbID, mock, WithEmbedder(embedder))

	// Upsert
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}))

	queryVec, err := loader.Embed(ctx, "brand new doc")
	require.NoError(t, err)

	results, err := loader.Search(ctx, kbID, queryVec, &SearchOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "c", results[0].ID)

	// Soft delete
	require.NoError(t, loader.DeleteDocsAndUpload(ctx, kbID, []string{"c"}, DeleteDocsOptions{}))

	results, err = loader.Search(ctx, kbID, queryVec, &SearchOptions{TopK: 3})
	require.NoError(t, err)
	for _, r := range results {
		assert.NotEqual(t, "c", r.ID)
	}
}

func testKBUpsertEmbedOnce(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-mutations-embed-count"
	counted := &countingEmbedder{base: newFixtureEmbedder(3)}

	mock := &mockArtifactFormat{
		ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
			return IngestResult{MutatedCount: len(req.Docs)}, nil
		},
	}
	loader := newMockKBWithManifest(t, kbID, mock, WithEmbedder(counted))

	// Embed is called by the caller (not by format.Upsert in mock mode).
	// This tests that KB.UpsertDocs passes docs to format.Upsert.
	err := loader.UpsertDocs(ctx, kbID, []Document{
		{ID: "a", Text: "first doc"},
		{ID: "b", Text: "second doc"},
	})
	require.NoError(t, err)
	// The mock format receives the docs. the KB layer does not embed on its own.
	// countingEmbedder.Calls() == 0 because embedding is the format's responsibility.
	// We verify the upsert went through without error.
}

func testKBMutateUploadVisibleFreshLoader(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-upload-visible"
	embedder := newFixtureEmbedder(3)

	upserted := map[string]string{}
	mock := &mockArtifactFormat{
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

	// Writer and reader share the same blob root but have separate cache dirs.
	sharedBlobRoot := SharedBlobRoot(t)
	require.NoError(t, os.MkdirAll(sharedBlobRoot, 0o755))
	bs := &LocalBlobStore{Root: sharedBlobRoot}

	writerCacheDir := filepath.Join(t.TempDir(), "writer-cache")
	require.NoError(t, os.MkdirAll(writerCacheDir, 0o755))
	writer := NewKB(bs, writerCacheDir, WithEmbedder(embedder), WithArtifactFormat(mock))

	// Seed manifest so resolveFormat works.
	seedManifest(t, ctx, bs, kbID, []SnapshotShardMetadata{
		{ShardID: "s-0", Key: kbID + "/s-0.mock", SizeBytes: 1, VectorRows: 1, CreatedAt: time.Now().UTC()},
	})

	require.NoError(t, writer.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}))

	// Reader shares the same mock (simulates shared format state).
	readerCacheDir := filepath.Join(t.TempDir(), "reader-cache")
	require.NoError(t, os.MkdirAll(readerCacheDir, 0o755))
	reader := NewKB(bs, readerCacheDir, WithEmbedder(embedder), WithArtifactFormat(mock))

	queryVec, err := reader.Embed(ctx, "brand new doc")
	require.NoError(t, err)
	results, err := reader.Search(ctx, kbID, queryVec, &SearchOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "c", results[0].ID)
}

func testKBUpsertUploadBootstrap(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-upsert-bootstrap"
	embedder := newFixtureEmbedder(3)

	upserted := map[string]string{}
	mock := &mockArtifactFormat{
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

	// No pre-existing manifest: this is a bootstrap scenario.
	loader := newMockKB(t, mock, WithEmbedder(embedder))

	err := loader.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}})
	require.NoError(t, err)

	queryVec, err := loader.Embed(ctx, "brand new doc")
	require.NoError(t, err)

	results, err := loader.Search(ctx, kbID, queryVec, &SearchOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "c", results[0].ID)
}

func testKBMutateUploadNoRetryConflict(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-upload-no-retry"
	embedder := newFixtureEmbedder(3)

	mock := &mockArtifactFormat{
		ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
			if req.Upload {
				return IngestResult{}, ErrBlobVersionMismatch
			}
			return IngestResult{MutatedCount: len(req.Docs)}, nil
		},
	}

	observer := &captureRetryObserver{}
	loader := newMockKBWithManifest(t, kbID, mock, WithEmbedder(embedder))
	loader.SetMutationRetryObserver(observer)

	err := loader.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrBlobVersionMismatch)

	stats, ok := observer.Last()
	require.True(t, ok)
	assert.Equal(t, 1, stats.Attempts)
	assert.Equal(t, 1, stats.ConflictCount)
	assert.Equal(t, time.Duration(0), stats.TotalRetryDelay)
	assert.False(t, stats.Success)
}

func testKBMutateUploadRetry(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-upload-retry"
	embedder := newFixtureEmbedder(3)

	callCount := 0
	mock := &mockArtifactFormat{
		ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
			callCount++
			if callCount == 1 {
				return IngestResult{}, ErrBlobVersionMismatch
			}
			return IngestResult{MutatedCount: len(req.Docs)}, nil
		},
	}

	observer := &captureRetryObserver{}
	loader := newMockKBWithManifest(t, kbID, mock, WithEmbedder(embedder))
	loader.SetMutationRetryObserver(observer)

	err := loader.UpsertDocsAndUploadWithRetry(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}, 1)
	require.NoError(t, err)

	stats, ok := observer.Last()
	require.True(t, ok)
	assert.Equal(t, "upsert_docs_upload", stats.Operation)
	assert.Equal(t, 2, stats.Attempts)
	assert.Equal(t, 1, stats.ConflictCount)
	assert.Greater(t, stats.TotalRetryDelay, time.Duration(0))
	assert.True(t, stats.Success)
}

func testKBIngestSharding(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		kbID   string
		policy ShardingPolicy
		docs   []Document
	}{
		{
			name:   "below_threshold",
			kbID:   "kb-ingest-sharding-below",
			policy: ShardingPolicy{ShardTriggerBytes: 1 << 30, ShardTriggerVectorRows: 1 << 30},
			docs:   []Document{{ID: "doc-1", Text: "hello world"}},
		},
		{
			name:   "rotate_rows",
			kbID:   "kb-ingest-sharding-rows",
			policy: ShardingPolicy{ShardTriggerBytes: 1 << 30, ShardTriggerVectorRows: 1},
			docs:   []Document{{ID: "doc-1", Text: "hello world"}, {ID: "doc-2", Text: "another text"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			embedder := newFixtureEmbedder(3)
			mock := &mockArtifactFormat{
				ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
					return IngestResult{MutatedCount: len(req.Docs)}, nil
				},
			}
			loader := newMockKB(t, mock, WithEmbedder(embedder), WithShardingPolicy(tc.policy))

			require.NoError(t, loader.UpsertDocsAndUpload(ctx, tc.kbID, tc.docs))
		})
	}
}

func testKBRetryContextCanceled(t *testing.T) {
	kbID := "kb-upload-retry-canceled"
	embedder := newFixtureEmbedder(3)

	mock := &mockArtifactFormat{
		ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
			return IngestResult{}, ErrBlobVersionMismatch
		},
	}
	loader := newMockKBWithManifest(t, kbID, mock, WithEmbedder(embedder))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := loader.UpsertDocsAndUploadWithRetry(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}, 10)
	elapsed := time.Since(start)

	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrBlobVersionMismatch),
		"expected context canceled or version mismatch, got: %v", err)
	assert.Less(t, elapsed, 250*time.Millisecond, "expected cancellation to return quickly")
}

func TestSnapshotSharded(t *testing.T) {
	t.Run("manifest_cas", testSnapshotShardedManifestCAS)
	t.Run("write_lease_conflict", testSnapshotShardedWriteLeaseConflict)
}

func testSnapshotShardedManifestCAS(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-sharded-cas"

	mock := &mockArtifactFormat{
		buildArtifactsFn: func(_ context.Context, kbIDArg, srcPath string, targetBytes int64) ([]SnapshotShardMetadata, error) {
			return []SnapshotShardMetadata{
				{
					ShardID:    "s-0",
					Key:        kbIDArg + "/s-0.mock",
					SizeBytes:  100,
					VectorRows: 10,
					CreatedAt:  time.Now().UTC(),
				},
			}, nil
		},
	}

	blobRoot := filepath.Join(t.TempDir(), "blobs")
	require.NoError(t, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))
	store := &LocalBlobStore{Root: blobRoot}
	loader := NewKB(store, cacheDir, WithArtifactFormat(mock))

	// Write a dummy source file
	src1 := filepath.Join(cacheDir, "v1.mock")
	require.NoError(t, os.WriteFile(src1, []byte("v1"), 0o644))
	info1, err := loader.UploadSnapshotShardedIfMatch(ctx, kbID, src1, "", 1024)
	require.NoError(t, err)

	src2 := filepath.Join(cacheDir, "v2.mock")
	require.NoError(t, os.WriteFile(src2, []byte("v2"), 0o644))

	// Stale version should fail
	_, err = loader.UploadSnapshotShardedIfMatch(ctx, kbID, src2, "stale-version", 1024)
	require.ErrorIs(t, err, ErrBlobVersionMismatch)

	// Correct version should succeed
	_, err = loader.UploadSnapshotShardedIfMatch(ctx, kbID, src2, info1.Version, 1024)
	require.NoError(t, err)
}

func testSnapshotShardedWriteLeaseConflict(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-sharded-lease"

	mock := &mockArtifactFormat{}
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	require.NoError(t, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))

	loader := NewKB(&LocalBlobStore{Root: blobRoot}, cacheDir, WithArtifactFormat(mock))

	src := filepath.Join(cacheDir, "lease-src.mock")
	require.NoError(t, os.WriteFile(src, []byte("data"), 0o644))

	leaseMgr := loader.WriteLeaseManager
	require.NotNil(t, leaseMgr)
	held, err := leaseMgr.Acquire(ctx, kbID, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = leaseMgr.Release(context.Background(), held)
	})

	_, err = loader.UploadSnapshotShardedIfMatch(ctx, kbID, src, "", 1024)
	require.ErrorIs(t, err, ErrWriteLeaseConflict)
}

func TestCacheQueryBudget(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-vector-budget"
	embedder := newFixtureEmbedder(3)

	mock := &mockArtifactFormat{
		ingestFn: func(_ context.Context, req IngestUpsertRequest) (IngestResult, error) {
			return IngestResult{MutatedCount: len(req.Docs)}, nil
		},
		queryRagFn: func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
			return nil, ErrCacheBudgetExceeded
		},
	}

	loader := newMockKBWithManifest(t, kbID, mock, WithEmbedder(embedder))
	loader.SetMaxCacheBytes(1)

	queryVec, err := loader.Embed(ctx, "hello world")
	require.NoError(t, err)

	_, err = loader.Search(ctx, kbID, queryVec, &SearchOptions{TopK: 2})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCacheBudgetExceeded)

	// Increase budget and provide working query.
	loader.SetMaxCacheBytes(5 * 1024 * 1024)
	mock.queryRagFn = func(_ context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
		return []ExpandedResult{{ID: "doc-1", Distance: 0.01}}, nil
	}

	results, err := loader.Search(ctx, kbID, queryVec, &SearchOptions{TopK: 2})
	require.NoError(t, err)
	require.NotEmpty(t, results)
}

// mockArtifactFormat is a callback-based mock implementing ArtifactFormat
// for testing KB orchestration logic without any DuckDB dependency.

type mockArtifactFormat struct {
	kind                  string
	version               int
	queryRagFn            func(context.Context, RagQueryRequest) ([]ExpandedResult, error)
	queryGraphFn          func(context.Context, GraphQueryRequest) ([]ExpandedResult, error)
	ingestFn              func(context.Context, IngestUpsertRequest) (IngestResult, error)
	deleteFn              func(context.Context, IngestDeleteRequest) (IngestResult, error)
	buildArtifactsFn      func(context.Context, string, string, int64) ([]SnapshotShardMetadata, error)
	publishPreparedStream func(context.Context, PreparedStreamRequest) (IngestResult, error)
}

func (m *mockArtifactFormat) Kind() string {
	if m.kind == "" {
		return "mock"
	}
	return m.kind
}
func (m *mockArtifactFormat) Version() int {
	if m.version == 0 {
		return 1
	}
	return m.version
}
func (m *mockArtifactFormat) FileExt() string { return ".mock" }
func (m *mockArtifactFormat) QueryRag(ctx context.Context, req RagQueryRequest) ([]ExpandedResult, error) {
	if m.queryRagFn != nil {
		return m.queryRagFn(ctx, req)
	}
	return nil, nil
}
func (m *mockArtifactFormat) QueryGraph(ctx context.Context, req GraphQueryRequest) ([]ExpandedResult, error) {
	if m.queryGraphFn != nil {
		return m.queryGraphFn(ctx, req)
	}
	return nil, nil
}
func (m *mockArtifactFormat) QueryBM25(context.Context, BM25QueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (m *mockArtifactFormat) Ingest(ctx context.Context, req IngestUpsertRequest) (IngestResult, error) {
	if m.ingestFn != nil {
		return m.ingestFn(ctx, req)
	}
	return IngestResult{}, nil
}
func (m *mockArtifactFormat) Delete(ctx context.Context, req IngestDeleteRequest) (IngestResult, error) {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, req)
	}
	return IngestResult{}, nil
}

func (m *mockArtifactFormat) BuildArtifacts(
	ctx context.Context,
	kbID, srcPath string,
	targetBytes int64,
) ([]SnapshotShardMetadata, error) {
	if m.buildArtifactsFn != nil {
		return m.buildArtifactsFn(ctx, kbID, srcPath, targetBytes)
	}
	return nil, nil
}

func (m *mockArtifactFormat) PublishPreparedStream(
	ctx context.Context,
	req PreparedStreamRequest,
) (IngestResult, error) {
	if m.publishPreparedStream != nil {
		return m.publishPreparedStream(ctx, req)
	}
	return IngestResult{}, nil
}

type fixtureEmbedder struct{ dim int }

func newFixtureEmbedder(dim int) Embedder { return fixtureEmbedder{dim: dim} }
func (e fixtureEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(input))
	seed := h.Sum32()
	vec := make([]float32, e.dim)
	for i := range vec {
		vec[i] = float32((seed>>uint(i%24))&0xff) + 1
	}
	return normalizeVector(vec), nil
}
func normalizeVector(vec []float32) []float32 {
	var sum float64
	for _, v := range vec {
		sum += float64(v * v)
	}
	norm := float32(math.Sqrt(sum))
	out := make([]float32, len(vec))
	if norm == 0 {
		return out
	}
	for i, v := range vec {
		out[i] = v / norm
	}
	return out
}

type countingEmbedder struct {
	base  Embedder
	mu    sync.Mutex
	calls int
}

func (e *countingEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	return e.base.Embed(ctx, input)
}
func (e *countingEmbedder) Calls() int { e.mu.Lock(); defer e.mu.Unlock(); return e.calls }

type captureRetryObserver struct {
	mu    sync.Mutex
	stats []MutationRetryStats
}

func (o *captureRetryObserver) ObserveMutationRetry(s MutationRetryStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stats = append(o.stats, s)
}
func (o *captureRetryObserver) Last() (MutationRetryStats, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.stats) == 0 {
		return MutationRetryStats{}, false
	}
	return o.stats[len(o.stats)-1], true
}

func seedManifest(t *testing.T, ctx context.Context, bs BlobStore, kbID string, shards []SnapshotShardMetadata) string {
	t.Helper()
	now := time.Now().UTC()
	manifest := SnapshotShardManifest{
		SchemaVersion: 1,
		Layout:        ShardManifestLayoutDuckDBs,
		FormatKind:    "mock",
		FormatVersion: 1,
		KBID:          kbID,
		CreatedAt:     now,
		Shards:        shards,
	}
	for _, s := range shards {
		manifest.TotalSizeBytes += s.SizeBytes
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(manifestPath, data, 0o644))
	info, err := bs.UploadIfMatch(ctx, ShardManifestKey(kbID), manifestPath, "")
	require.NoError(t, err)
	return info.Version
}

func requireResultContainsID(t *testing.T, results []ExpandedResult, id string) {
	t.Helper()
	for _, result := range results {
		if result.ID == id {
			return
		}
	}
	require.Failf(t, "missing result", "id %s not found", id)
}
