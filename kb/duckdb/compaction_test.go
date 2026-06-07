package duckdb_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
)

func startCompactionAttempt(
	wg *sync.WaitGroup,
	af *duckdb.DuckDBArtifactFormat,
	kbID string,
	i int,
	results []*kb.CompactionPublishResult,
	errs []error,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[i], errs[i] = af.CompactIfNeeded(context.Background(), kbID)
	}()
}

func TestDuckDBCompaction(t *testing.T) {
	t.Run("kb_uninitialized", func(t *testing.T) {
		h := kb.NewTestHarness(t, "kb-compact-missing").Setup()
		registerDuckDBFormatOnHarness(t, h)
		t.Cleanup(h.Cleanup)

		af := requireDuckDBFormat(t, h.KB())
		_, err := af.CompactIfNeeded(context.Background(), "kb-compact-missing")
		require.Error(t, err)
		require.True(t, errors.Is(err, kb.ErrKBUninitialized))
	})

	t.Run("no_compaction_debt", func(t *testing.T) {
		kbID := "kb-compact-nodebt"
		h := kb.NewTestHarness(t, kbID).Setup()
		registerDuckDBFormatOnHarness(t, h)
		t.Cleanup(h.Cleanup)

		uploadTestShardManifest(t, h.KB(), kbID, 1)

		af := requireDuckDBFormat(t, h.KB())
		result, err := af.CompactIfNeeded(context.Background(), kbID)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.False(t, result.Performed)
	})

	t.Run("cas_conflict_aborts_compaction", func(t *testing.T) {
		kbID := "kb-compact-cas"
		bs := &kb.LocalBlobStore{Root: t.TempDir()}
		conflictStore := &casConflictManifestStore{
			inner:    &kb.BlobManifestStore{Store: bs},
			failOnce: true,
		}
		cacheDir := t.TempDir()
		embDim := 4

		var mu sync.Mutex
		locks := map[string]*sync.Mutex{}
		lockFor := func(id string) *sync.Mutex {
			mu.Lock()
			defer mu.Unlock()
			if locks[id] == nil {
				locks[id] = &sync.Mutex{}
			}
			return locks[id]
		}

		af, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
			BlobStore:     bs,
			ManifestStore: conflictStore,
			CacheDir:      cacheDir,
			MemoryLimit:   "128MB",
			ShardingPolicy: kb.ShardingPolicy{
				CompactionEnabled:       true,
				CompactionEnabledSet:    true,
				CompactionMinShardCount: 2,
			},
			Embed:              func(_ context.Context, s string) ([]float32, error) { return make([]float32, embDim), nil },
			GraphBuilder:       func() *kb.GraphBuilder { return nil },
			EvictCacheIfNeeded: func(_ context.Context, _ string) error { return nil },
			LockFor:            lockFor,
			AcquireWriteLease:  noopAcquireWriteLease(),
			Metrics:            &noopMetrics{},
		})
		require.NoError(t, err)

		// Upload real DuckDB shard blobs so compaction can merge them.
		uploadRealShardManifest(t, bs, kbID, 3, embDim)

		_, err = af.CompactIfNeeded(context.Background(), kbID)
		require.Error(t, err)
		require.True(t, errors.Is(err, kb.ErrBlobVersionMismatch), "expected ErrBlobVersionMismatch, got: %v", err)

		// Original manifest should be unchanged.
		doc, err := conflictStore.inner.Get(context.Background(), kbID)
		require.NoError(t, err)
		require.Len(t, doc.Manifest.Shards, 3)
	})

	t.Run("concurrent_compaction_serialized", func(t *testing.T) {
		kbID := "kb-compact-concurrent"
		bs := &kb.LocalBlobStore{Root: t.TempDir()}
		ms := &kb.BlobManifestStore{Store: bs}
		cacheDir := t.TempDir()
		embDim := 4

		var mu sync.Mutex
		locks := map[string]*sync.Mutex{}
		lockFor := func(id string) *sync.Mutex {
			mu.Lock()
			defer mu.Unlock()
			if locks[id] == nil {
				locks[id] = &sync.Mutex{}
			}
			return locks[id]
		}

		af, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
			BlobStore:     bs,
			ManifestStore: ms,
			CacheDir:      cacheDir,
			MemoryLimit:   "128MB",
			ShardingPolicy: kb.ShardingPolicy{
				CompactionEnabled:       true,
				CompactionEnabledSet:    true,
				CompactionMinShardCount: 2,
			},
			Embed:              func(_ context.Context, s string) ([]float32, error) { return make([]float32, embDim), nil },
			GraphBuilder:       func() *kb.GraphBuilder { return nil },
			EvictCacheIfNeeded: func(_ context.Context, _ string) error { return nil },
			LockFor:            lockFor,
			AcquireWriteLease:  noopAcquireWriteLease(),
			Metrics:            &noopMetrics{},
		})
		require.NoError(t, err)

		uploadRealShardManifest(t, bs, kbID, 3, embDim)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		results := make([]*kb.CompactionPublishResult, 2)
		for i := range 2 {
			startCompactionAttempt(&wg, af, kbID, i, results, errs)
		}
		wg.Wait()

		// One should succeed with compaction, the other either succeeds
		// (finding no debt) or fails with CAS conflict.
		successCount := 0
		for i := range 2 {
			if errs[i] == nil && results[i] != nil {
				successCount++
			}
		}
		assert.GreaterOrEqual(t, successCount, 1, "at least one compaction should succeed")

		// Final manifest should be consistent: no duplicate shard IDs.
		doc, err := ms.Get(context.Background(), kbID)
		require.NoError(t, err)
		seen := map[string]struct{}{}
		for _, s := range doc.Manifest.Shards {
			_, dup := seen[s.ShardID]
			assert.False(t, dup, "duplicate shard ID: %s", s.ShardID)
			seen[s.ShardID] = struct{}{}
		}
	})

	t.Run("gc_enqueue_after_compaction", func(t *testing.T) {
		kbID := "kb-compact-gc"
		bs := &kb.LocalBlobStore{Root: t.TempDir()}
		ms := &kb.BlobManifestStore{Store: bs}
		cacheDir := t.TempDir()
		embDim := 4

		var mu sync.Mutex
		locks := map[string]*sync.Mutex{}
		lockFor := func(id string) *sync.Mutex {
			mu.Lock()
			defer mu.Unlock()
			if locks[id] == nil {
				locks[id] = &sync.Mutex{}
			}
			return locks[id]
		}

		var gcCalls []gcEnqueueCall
		var gcMu sync.Mutex

		af, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
			BlobStore:     bs,
			ManifestStore: ms,
			CacheDir:      cacheDir,
			MemoryLimit:   "128MB",
			ShardingPolicy: kb.ShardingPolicy{
				CompactionEnabled:       true,
				CompactionEnabledSet:    true,
				CompactionMinShardCount: 2,
			},
			Embed:              func(_ context.Context, s string) ([]float32, error) { return make([]float32, embDim), nil },
			GraphBuilder:       func() *kb.GraphBuilder { return nil },
			EvictCacheIfNeeded: func(_ context.Context, _ string) error { return nil },
			LockFor:            lockFor,
			AcquireWriteLease:  noopAcquireWriteLease(),
			EnqueueReplacedShardsForGC: func(id string, shards []kb.SnapshotShardMetadata, now time.Time) {
				gcMu.Lock()
				defer gcMu.Unlock()
				gcCalls = append(gcCalls, gcEnqueueCall{kbID: id, shards: shards, now: now})
			},
			Metrics: &noopMetrics{},
		})
		require.NoError(t, err)

		uploadRealShardManifest(t, bs, kbID, 3, embDim)

		result, err := af.CompactIfNeeded(context.Background(), kbID)
		require.NoError(t, err)
		require.True(t, result.Performed)

		// GC callback should have been called with the replaced shards.
		gcMu.Lock()
		defer gcMu.Unlock()
		require.Len(t, gcCalls, 1)
		assert.Equal(t, kbID, gcCalls[0].kbID)
		assert.Len(t, gcCalls[0].shards, len(result.ReplacedShards))

		// Replacement shard should be in the new manifest.
		doc, err := ms.Get(context.Background(), kbID)
		require.NoError(t, err)
		replacementIDs := map[string]struct{}{}
		for _, s := range result.ReplacementShards {
			replacementIDs[s.ShardID] = struct{}{}
		}
		found := false
		for _, s := range doc.Manifest.Shards {
			if _, ok := replacementIDs[s.ShardID]; ok {
				found = true
				break
			}
		}
		assert.True(t, found, "replacement shard should be in the new manifest")
	})
}

// --- test helpers ---

type gcEnqueueCall struct {
	kbID   string
	shards []kb.SnapshotShardMetadata
	now    time.Time
}

// casConflictManifestStore wraps a ManifestStore and injects ErrBlobVersionMismatch
// on the first UpsertIfMatch call.
type casConflictManifestStore struct {
	inner    kb.ManifestStore
	failOnce bool
	failed   atomic.Bool
}

func (s *casConflictManifestStore) Get(ctx context.Context, kbID string) (*kb.ManifestDocument, error) {
	return s.inner.Get(ctx, kbID)
}

func (s *casConflictManifestStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	return s.inner.HeadVersion(ctx, kbID)
}

func (s *casConflictManifestStore) UpsertIfMatch(
	ctx context.Context,
	kbID string,
	manifest kb.SnapshotShardManifest,
	expectedVersion string,
) (string, error) {
	if s.failOnce && s.failed.CompareAndSwap(false, true) {
		return "", kb.ErrBlobVersionMismatch
	}
	return s.inner.UpsertIfMatch(ctx, kbID, manifest, expectedVersion)
}

func (s *casConflictManifestStore) Delete(ctx context.Context, kbID string) error {
	return s.inner.Delete(ctx, kbID)
}

// uploadRealShardManifest creates real DuckDB shard files with the docs table
// and uploads them alongside a manifest to the blob store.
func uploadRealShardManifest(t *testing.T, bs kb.BlobStore, kbID string, shardCount, embDim int) {
	t.Helper()
	ctx := context.Background()

	manifest := kb.SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         kb.ShardManifestLayoutDuckDBs,
		FormatKind:     duckdb.DuckDBFormatKind,
		FormatVersion:  duckdb.DuckDBFormatVersion,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: int64(shardCount),
		Shards:         make([]kb.SnapshotShardMetadata, 0, shardCount),
	}

	tmpDir := t.TempDir()
	for i := range shardCount {
		shardID := fmt.Sprintf("shard-%03d", i)
		key := fmt.Sprintf("%s/%s.duckdb", kbID, shardID)

		// Create a real DuckDB file with the expected schema.
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("shard-%03d.duckdb", i))
		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[%d],
			media_refs TEXT,
			metadata TEXT
		)`, embDim))
		require.NoError(t, err)

		vec := make([]float32, embDim)
		for d := range embDim {
			vec[d] = float32(i*embDim+d) * 0.1
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			`INSERT INTO docs VALUES ('doc-%d', 'content %d', %s::FLOAT[%d], NULL, '{"source":"compaction-test"}')`,
			i, i, duckdb.FormatVectorForSQL(vec), embDim,
		))
		require.NoError(t, err)

		require.NoError(t, duckdb.EnsureGraphTables(ctx, db))
		require.NoError(t, duckdb.CheckpointAndCloseDB(ctx, db, "test shard"))

		// Upload to blob store.
		info, err := bs.UploadIfMatch(ctx, key, dbPath, "")
		require.NoError(t, err)
		_ = info

		fi, err := os.Stat(dbPath)
		require.NoError(t, err)

		manifest.Shards = append(manifest.Shards, kb.SnapshotShardMetadata{
			ShardID:    shardID,
			Key:        key,
			SizeBytes:  fi.Size(),
			VectorRows: 1,
			CreatedAt:  time.Now().UTC(),
		})
	}

	// Upload the manifest.
	data, err := json.Marshal(manifest)
	require.NoError(t, err)

	manifestPath := filepath.Join(tmpDir, "manifest.json")
	require.NoError(t, os.WriteFile(manifestPath, data, 0o644))

	_, err = bs.UploadIfMatch(ctx, kb.ShardManifestKey(kbID), manifestPath, "")
	require.NoError(t, err)
}
