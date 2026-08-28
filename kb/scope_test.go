package kb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type slowDeleteStore struct {
	BlobStore
	started chan struct{}
	release chan struct{}
}

func (s *slowDeleteStore) Delete(ctx context.Context, key string) error {
	close(s.started)
	select {
	case <-s.release:
		return s.BlobStore.Delete(ctx, key)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type scopeFormat struct {
	ids     map[string]struct{}
	deleted *[][]string
}

func (scopeFormat) Kind() string    { return "scope" }
func (scopeFormat) Version() int    { return 1 }
func (scopeFormat) FileExt() string { return ".scope" }
func (scopeFormat) BuildArtifacts(context.Context, string, string, int64) ([]SnapshotShardMetadata, error) {
	return nil, nil
}
func (scopeFormat) QueryRag(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (scopeFormat) QueryBM25(context.Context, BM25QueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (scopeFormat) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (f scopeFormat) FetchVectors(_ context.Context, _ string, ids []string) ([]VectorRecord, error) {
	records := make([]VectorRecord, 0, len(ids))
	for _, id := range ids {
		if _, ok := f.ids[id]; ok {
			records = append(records, VectorRecord{ID: id})
		}
	}
	return records, nil
}
func (scopeFormat) Ingest(context.Context, IngestUpsertRequest) (IngestResult, error) {
	return IngestResult{}, nil
}

func (f scopeFormat) Delete(_ context.Context, req IngestDeleteRequest) (IngestResult, error) {
	if f.deleted != nil {
		*f.deleted = append(*f.deleted, append([]string(nil), req.DocIDs...))
	}
	return IngestResult{}, nil
}

func newScopeKB(t *testing.T) *KB {
	t.Helper()
	return NewKB(
		&LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
		WithArtifactFormat(scopeFormat{ids: map[string]struct{}{"a": {}, "b": {}, "c": {}}}),
	)
}

func TestScopes(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)

	main, err := loader.ReplaceScope(ctx, "kb", "main", []string{"b", "a", "a", ""}, "")
	require.NoError(t, err)
	_, err = loader.ReplaceScope(ctx, "kb", "feature", []string{"b", "c"}, "")
	require.NoError(t, err)

	main, err = loader.GetScope(ctx, "kb", "main")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, main.DocumentIDs)

	scopes, err := loader.ListScopes(ctx, "kb")
	require.NoError(t, err)
	require.Equal(t, []string{"feature", "main"}, []string{scopes[0].ScopeID, scopes[1].ScopeID})

	_, err = loader.ReplaceScope(ctx, "kb", "main", []string{"c"}, main.Revision)
	require.NoError(t, err)
	feature, err := loader.GetScope(ctx, "kb", "feature")
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, feature.DocumentIDs)
}

func TestFinalizeSessionScope(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)
	desired := SessionCommitScope{ScopeID: "main", DocumentIDs: []string{"b", "a"}}

	require.NoError(t, loader.FinalizeSessionScope(ctx, "kb", desired))
	require.NoError(t, loader.FinalizeSessionScope(ctx, "kb", desired))

	scope, err := loader.GetScope(ctx, "kb", "main")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, scope.DocumentIDs)

	err = loader.FinalizeSessionScope(ctx, "kb", SessionCommitScope{
		ScopeID: "main", DocumentIDs: []string{"c"},
	})
	require.ErrorIs(t, err, ErrBlobVersionMismatch)
}

func TestScopeCleanup(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)
	_, err := loader.ReplaceScope(ctx, "kb", "main", []string{"a"}, "")
	require.NoError(t, err)

	errs := loader.deleteKBScopes(ctx, "kb")
	require.Empty(t, errs)
	_, err = loader.GetScope(ctx, "kb", "main")
	require.ErrorIs(t, err, ErrScopeNotFound)
}

func TestDeleteScope(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)
	_, err := loader.ReplaceScope(ctx, "kb", "branch", []string{"a"}, "")
	require.NoError(t, err)
	require.NoError(t, loader.DeleteScope(ctx, "kb", "branch"))
	require.NoError(t, loader.DeleteScope(ctx, "kb", "branch"))
	_, err = loader.GetScope(ctx, "kb", "branch")
	require.ErrorIs(t, err, ErrScopeNotFound)
}

func TestScopeDropsAreCollected(t *testing.T) {
	ctx := context.Background()
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	deleted := make([][]string, 0)
	loader := NewKB(
		&LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
		WithArtifactFormat(scopeFormat{
			ids: map[string]struct{}{"a": {}, "b": {}}, deleted: &deleted,
		}),
	)
	loader.Clock = clock
	created, err := loader.ReplaceScope(ctx, "kb", "branch", []string{"a"}, "")
	require.NoError(t, err)
	_, err = loader.ReplaceScope(ctx, "kb", "branch", []string{"b"}, created.Revision)
	require.NoError(t, err)

	count, err := loader.SweepScopeGC(ctx, clock.Now().Add(ScopeGCGrace))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, [][]string{{"a"}}, deleted)
}

func TestDeletedScopeIsCollected(t *testing.T) {
	ctx := context.Background()
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	deleted := make([][]string, 0)
	loader := NewKB(
		&LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
		WithArtifactFormat(scopeFormat{ids: map[string]struct{}{"a": {}}, deleted: &deleted}),
	)
	loader.Clock = clock
	created, err := loader.ReplaceScope(ctx, "kb", "branch", []string{"a"}, "")
	require.NoError(t, err)
	require.NoError(t, loader.DeleteScopeIfRevision(ctx, "kb", "branch", created.Revision))

	count, err := loader.SweepScopeGC(ctx, clock.Now().Add(ScopeGCGrace))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, [][]string{{"a"}}, deleted)
}

func TestScopeLeaseRenewal(t *testing.T) {
	store := &slowDeleteStore{
		BlobStore: &LocalBlobStore{Root: t.TempDir()},
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	manager := NewInMemoryWriteLeaseManager()
	loader := NewKB(
		store, t.TempDir(), WithWriteLeaseManager(manager), WithWriteLeaseTTL(30*time.Millisecond),
		WithArtifactFormat(scopeFormat{ids: map[string]struct{}{"a": {}}}),
	)
	_, err := loader.ReplaceScope(context.Background(), "kb", "branch", []string{"a"}, "")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- loader.DeleteScope(context.Background(), "kb", "branch") }()
	<-store.started
	time.Sleep(50 * time.Millisecond)
	_, err = manager.Acquire(context.Background(), "scope:kb", 30*time.Millisecond)
	require.True(t, errors.Is(err, ErrWriteLeaseConflict))
	close(store.release)
	require.NoError(t, <-done)
}

func TestScopeConflict(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)
	created, err := loader.ReplaceScope(ctx, "kb", "main", []string{"a"}, "")
	require.NoError(t, err)
	updated, err := loader.ReplaceScope(ctx, "kb", "main", []string{"b"}, created.Revision)
	require.NoError(t, err)
	require.ErrorIs(t, loader.DeleteScopeIfRevision(ctx, "kb", "main", created.Revision), ErrBlobVersionMismatch)
	_, err = loader.GetScope(ctx, "kb", "main")
	require.NoError(t, err)
	_, err = loader.ReplaceScope(ctx, "kb", "main", []string{"c"}, created.Revision)
	require.ErrorIs(t, err, ErrBlobVersionMismatch)
	require.NotEqual(t, created.Revision, updated.Revision)
	require.NoError(t, loader.DeleteScopeIfRevision(ctx, "kb", "main", updated.Revision))
	_, err = loader.ReplaceScope(ctx, "kb", "missing", []string{"missing"}, "")
	require.ErrorIs(t, err, ErrScopeDocumentsMissing)
}

func TestScopeGCRefresh(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	loader.Clock = clock
	_, err := loader.ScheduleScopeGC(ctx, "kb", []string{"c"})
	require.NoError(t, err)
	clock.Advance(14 * time.Minute)
	_, err = loader.ScheduleScopeGC(ctx, "kb", []string{"c"})
	require.NoError(t, err)
	count, err := loader.SweepScopeGC(ctx, clock.Now().Add(2*time.Minute))
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = loader.SweepScopeGC(ctx, clock.Now().Add(ScopeGCGrace))
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestScopeGCScale(t *testing.T) {
	ctx := context.Background()
	loader := newScopeKB(t)
	ids := make([]string, 36_816)
	for i := range ids {
		ids[i] = fmt.Sprintf("chunk-%05d", i)
	}
	for start := 0; start < len(ids); start += 32 {
		end := min(start+32, len(ids))
		_, err := loader.ScheduleScopeGC(ctx, "kb", ids[start:end])
		require.NoError(t, err)
	}
	_, err := loader.ScheduleScopeGC(ctx, "kb", ids)
	require.NoError(t, err)
	objects, err := loader.BlobStore.List(ctx, scopeGCKBPrefix("kb"))
	require.NoError(t, err)
	require.Len(t, objects, (len(ids)+31)/32+1)
	var bytes int64
	for _, object := range objects {
		bytes += object.Size
	}
	require.Less(t, bytes, int64(12<<20))
}

// Close cancels the mutation context, and a renew tick racing that must not
// fail a mutation that already finished.
func TestScopeMutationRenewIgnoresShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mutation := &scopeMutation{
		ctx: ctx, cancel: cancel, ttl: 3 * time.Millisecond,
		lease: &WriteLease{}, done: make(chan struct{}),
	}
	mutation.manager = cancelDuringRenew{cancel: cancel}

	mutation.wg.Add(1)
	mutation.renew()

	require.NoError(t, mutation.err)
}

// cancelDuringRenew cancels the mutation from inside Renew, reproducing a tick
// that wins the select and only then observes the shutdown.
type cancelDuringRenew struct {
	WriteLeaseManager
	cancel context.CancelFunc
}

func (c cancelDuringRenew) Renew(context.Context, *WriteLease, time.Duration) (*WriteLease, error) {
	c.cancel()
	return nil, context.Canceled
}
