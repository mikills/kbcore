package kb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestFakeClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("starts_at_provided_time", func(t *testing.T) {
		c := kb.NewFakeClock(start)
		require.Equal(t, start, c.Now())
	})

	t.Run("advance_moves_forward", func(t *testing.T) {
		c := kb.NewFakeClock(start)
		c.Advance(5 * time.Minute)
		require.Equal(t, start.Add(5*time.Minute), c.Now())
		c.Advance(1 * time.Hour)
		require.Equal(t, start.Add(5*time.Minute+time.Hour), c.Now())
	})

	t.Run("set_jumps_to_time", func(t *testing.T) {
		c := kb.NewFakeClock(start)
		target := start.Add(24 * time.Hour)
		c.Set(target)
		require.Equal(t, target, c.Now())
	})

	t.Run("zero_initial_time_defaults_to_epoch", func(t *testing.T) {
		c := kb.NewFakeClock(time.Time{})
		require.Equal(t, time.Unix(0, 0).UTC(), c.Now())
	})
}

func TestKBDefaultsToRealClock(t *testing.T) {
	loader := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir())
	require.NotNil(t, loader.Clock)
	require.Equal(t, kb.RealClock, loader.Clock)
}

func TestKBWithClockOverride(t *testing.T) {
	fake := kb.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	loader := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir(), kb.WithClock(fake))
	require.Same(t, fake, loader.Clock)
}

func TestNormalizeShardingPolicy(t *testing.T) {
	t.Run("defaults_to_enabled", func(t *testing.T) {
		policy := kb.NormalizeShardingPolicy(kb.ShardingPolicy{})
		require.True(t, policy.CompactionEnabled, "expected default compaction to be enabled")
	})

	t.Run("honors_explicit_disable", func(t *testing.T) {
		policy := kb.NormalizeShardingPolicy(kb.ShardingPolicy{CompactionEnabled: false, CompactionEnabledSet: true})
		require.False(t, policy.CompactionEnabled, "expected compaction to be disabled")
	})

	t.Run("with_compaction_enabled_option_disables", func(t *testing.T) {
		loader := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir(), kb.WithCompactionEnabled(false))
		resolved := kb.NormalizeShardingPolicy(loader.ShardingPolicy)
		require.False(t, resolved.CompactionEnabled, "expected normalized policy to preserve explicit disable")
	})
}

type searchStubFormat struct {
	queryRagFn   func(context.Context, kb.RagQueryRequest) ([]kb.ExpandedResult, error)
	queryGraphFn func(context.Context, kb.GraphQueryRequest) ([]kb.ExpandedResult, error)
}

func (f *searchStubFormat) Kind() string    { return "search-stub" }
func (f *searchStubFormat) Version() int    { return 1 }
func (f *searchStubFormat) FileExt() string { return ".stub" }
func (f *searchStubFormat) BuildArtifacts(context.Context, string, string, int64) ([]kb.SnapshotShardMetadata, error) {
	return nil, nil
}
func (f *searchStubFormat) QueryRag(ctx context.Context, req kb.RagQueryRequest) ([]kb.ExpandedResult, error) {
	return f.queryRagFn(ctx, req)
}
func (f *searchStubFormat) QueryGraph(ctx context.Context, req kb.GraphQueryRequest) ([]kb.ExpandedResult, error) {
	return f.queryGraphFn(ctx, req)
}
func (f *searchStubFormat) QueryBM25(context.Context, kb.BM25QueryRequest) ([]kb.ExpandedResult, error) {
	return nil, nil
}
func (f *searchStubFormat) FetchVectors(context.Context, string, []string) ([]kb.VectorRecord, error) {
	return nil, nil
}
func (f *searchStubFormat) Ingest(context.Context, kb.IngestUpsertRequest) (kb.IngestResult, error) {
	return kb.IngestResult{}, nil
}
func (f *searchStubFormat) Delete(context.Context, kb.IngestDeleteRequest) (kb.IngestResult, error) {
	return kb.IngestResult{}, nil
}

func TestSearch(t *testing.T) {
	calledErr := errors.New("backend should not be called")
	mock := &searchStubFormat{
		queryRagFn: func(context.Context, kb.RagQueryRequest) ([]kb.ExpandedResult, error) {
			return nil, calledErr
		},
		queryGraphFn: func(context.Context, kb.GraphQueryRequest) ([]kb.ExpandedResult, error) {
			return nil, calledErr
		},
	}

	loader := kb.NewKB(&kb.LocalBlobStore{Root: t.TempDir()}, t.TempDir(), kb.WithArtifactFormat(mock))

	t.Run("vector_mode_empty_query_vec", func(t *testing.T) {
		_, err := loader.Search(context.Background(), "kb", nil, &kb.SearchOptions{TopK: 1})
		require.ErrorIs(t, err, kb.ErrInvalidQueryRequest)
	})

	t.Run("graph_mode_invalid_max_distance", func(t *testing.T) {
		zero := 0.0
		_, err := loader.Search(
			context.Background(),
			"kb",
			[]float32{0.1},
			&kb.SearchOptions{Mode: kb.SearchModeGraph, TopK: 1, MaxDistance: &zero},
		)
		require.ErrorIs(t, err, kb.ErrInvalidQueryRequest)
	})
}
