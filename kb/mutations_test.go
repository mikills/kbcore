package kb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testChunker struct{}

func (testChunker) Chunk(ctx context.Context, docID string, text string) ([]Chunk, error) {
	_ = ctx
	return []Chunk{{
		DocID:   docID,
		ChunkID: docID + ":chunk",
		Text:    text,
		Start:   0,
		End:     len(text),
	}}, nil
}

type testGrapher struct{}

func (testGrapher) Extract(ctx context.Context, chunks []Chunk) (*GraphExtraction, error) {
	_ = ctx
	entities := make([]EntityCandidate, 0, len(chunks)*2)
	edges := make([]EdgeCandidate, 0, len(chunks))
	for _, chunk := range chunks {
		cleanText := strings.TrimSpace(chunk.Text)
		entityName := fmt.Sprintf("ent:%s:%s", chunk.DocID, cleanText)
		entities = append(entities,
			EntityCandidate{Name: entityName, ChunkID: chunk.ChunkID},
			EntityCandidate{Name: "shared", ChunkID: chunk.ChunkID},
		)
		edges = append(edges, EdgeCandidate{
			Src:     entityName,
			Dst:     "shared",
			RelType: "mentions",
			Weight:  1.0,
			ChunkID: chunk.ChunkID,
		})
	}
	return &GraphExtraction{Entities: entities, Edges: edges}, nil
}

type identityCanonicalizer struct{}

func (identityCanonicalizer) Canonicalize(ctx context.Context, name string) (string, error) {
	_ = ctx
	return name, nil
}

type flakyUploadBlobStore struct {
	base               BlobStore
	failUploadAttempts int
	mu                 sync.Mutex
}

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

func (f *flakyUploadBlobStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	return f.base.Head(ctx, key)
}

func (f *flakyUploadBlobStore) Download(ctx context.Context, key string, dest string) error {
	return f.base.Download(ctx, key, dest)
}

func (f *flakyUploadBlobStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error) {
	f.mu.Lock()
	if f.failUploadAttempts > 0 {
		f.failUploadAttempts--
		f.mu.Unlock()
		return nil, ErrBlobVersionMismatch
	}
	f.mu.Unlock()
	return f.base.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (f *flakyUploadBlobStore) Delete(ctx context.Context, key string) error {
	return f.base.Delete(ctx, key)
}

func (f *flakyUploadBlobStore) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	return f.base.List(ctx, prefix)
}

func TestKBMutations(t *testing.T) {
	t.Run("upsert_and_soft_delete", testKBUpsertAndSoftDelete)
	t.Run("hard_delete_prunes_graph_links", testKBHardDeletePrunesGraphLinks)
	t.Run("mutate_upload_visible_to_fresh_loader", testKBMutateUploadVisibleFreshLoader)
	t.Run("upsert_upload_bootstrap", testKBUpsertUploadBootstrap)
	t.Run("mutate_upload_no_retry_conflict", testKBMutateUploadNoRetryConflict)
	t.Run("mutate_upload_retry", testKBMutateUploadRetry)
	t.Run("ingest_sharding", testKBIngestSharding)
	t.Run("retry_context_canceled", testKBRetryContextCanceled)
	t.Run("graph_pipeline_upsert_tables", testKBGraphPipelineUpsertTables)
	t.Run("hard_delete_cleanup_graph_orphans", testKBHardDeleteCleanupGraphOrphans)
	t.Run("upsert_upload_graph_toggle", testKBUpsertUploadGraphToggle)
}

func testKBUpsertAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-mutations-soft"

	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, kbID).
		WithEmbedder(embedder).
		Setup()
	defer harness.Cleanup()

	if err := buildDemoKB(kbID, harness.BlobRoot(), embedder); err != nil {
		require.FailNowf(t, "test failed", "build kb: %v", err)
	}

	kb := harness.KB()

	if err := kb.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}); err != nil {
		require.FailNowf(t, "test failed", "upsert docs: %v", err)
	}

	queryVec, err := kb.Embed(ctx, "brand new doc")
	if err != nil {
		require.FailNowf(t, "test failed", "embed query: %v", err)
	}

	results, err := kb.TopK(ctx, kbID, queryVec, 1)
	if err != nil {
		require.FailNowf(t, "test failed", "topk after upsert: %v", err)
	}
	if len(results) != 1 || results[0].ID != "c" {
		require.FailNowf(t, "test failed", "expected top result c after upsert, got: %+v", results)
	}

	if err := kb.DeleteDocsAndUpload(ctx, kbID, []string{"c"}, DeleteDocsOptions{}); err != nil {
		require.FailNowf(t, "test failed", "soft delete docs: %v", err)
	}

	results, err = kb.TopK(ctx, kbID, queryVec, 3)
	if err != nil {
		require.FailNowf(t, "test failed", "topk after soft delete: %v", err)
	}
	for _, r := range results {
		if r.ID == "c" {
			require.FailNowf(t, "test failed", "soft-deleted doc c should not appear in results")
		}
	}
}

func testKBHardDeletePrunesGraphLinks(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-mutations-hard"

	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
	defer harness.Cleanup()
	if err := buildGraphKB(kbID, harness.BlobRoot(), embedder, []graphEdge{{src: "ent-a", dst: "ent-b", weight: 1.0, relType: "rel"}}); err != nil {
		require.FailNowf(t, "test failed", "build graph kb: %v", err)
	}

	kb := harness.KB()

	if err := kb.DeleteDocsAndUpload(ctx, kbID, []string{"a"}, DeleteDocsOptions{HardDelete: true}); err != nil {
		require.FailNowf(t, "test failed", "hard delete docs: %v", err)
	}

	db, err := kb.Load(ctx, kbID)
	if err != nil {
		require.FailNowf(t, "test failed", "load kb after delete: %v", err)
	}
	defer db.Close()

	var docsCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs WHERE id = 'a'`).Scan(&docsCount); err != nil {
		require.FailNowf(t, "test failed", "count docs: %v", err)
	}
	if docsCount != 0 {
		require.FailNowf(t, "test failed", "expected doc a to be hard deleted, found %d rows", docsCount)
	}

	var linksCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities WHERE doc_id = 'a'`).Scan(&linksCount); err != nil {
		require.FailNowf(t, "test failed", "count doc_entities: %v", err)
	}
	if linksCount != 0 {
		require.FailNowf(t, "test failed", "expected doc_entities for doc a to be pruned, found %d rows", linksCount)
	}

	var tombstoneCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_tombstones WHERE doc_id = 'a'`).Scan(&tombstoneCount); err != nil {
		require.FailNowf(t, "test failed", "count tombstones: %v", err)
	}
	if tombstoneCount != 0 {
		require.FailNowf(t, "test failed", "hard delete should remove tombstones, found %d rows", tombstoneCount)
	}
}

func testKBMutateUploadVisibleFreshLoader(t *testing.T) {
	tests := []struct {
		name     string
		kbID     string
		embedder Embedder
		mutate   func(context.Context, *KB, string) error
		verify   func(*testing.T, context.Context, *KB, string)
	}{
		{
			name:     "upsert",
			kbID:     "kb-upload-upsert",
			embedder: newFixtureEmbedder(3),
			mutate: func(ctx context.Context, kbWriter *KB, kbID string) error {
				return kbWriter.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}})
			},
			verify: func(t *testing.T, ctx context.Context, kbReader *KB, kbID string) {
				queryVec, err := kbReader.Embed(ctx, "brand new doc")
				require.NoError(t, err)
				results, err := kbReader.TopK(ctx, kbID, queryVec, 1)
				require.NoError(t, err)
				require.Len(t, results, 1)
				assert.Equal(t, "c", results[0].ID)
			},
		},
		{
			name:     "delete",
			kbID:     "kb-upload-delete",
			embedder: newDemoEmbedder(),
			mutate: func(ctx context.Context, kbWriter *KB, kbID string) error {
				return kbWriter.DeleteDocsAndUpload(ctx, kbID, []string{"b"}, DeleteDocsOptions{})
			},
			verify: func(t *testing.T, ctx context.Context, kbReader *KB, kbID string) {
				queryVec, err := kbReader.Embed(ctx, "goodbye")
				require.NoError(t, err)
				results, err := kbReader.TopK(ctx, kbID, queryVec, 2)
				require.NoError(t, err)
				for _, r := range results {
					assert.NotEqual(t, "b", r.ID)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sharedBlobRoot := SharedBlobRoot(t)
			writerHarness := NewTestHarness(t, tc.kbID).WithBlobRoot(sharedBlobRoot).WithEmbedder(tc.embedder).Setup()
			defer writerHarness.Cleanup()
			readerHarness := NewTestHarness(t, tc.kbID).WithBlobRoot(sharedBlobRoot).WithEmbedder(tc.embedder).Setup()
			defer readerHarness.Cleanup()

			require.NoError(t, buildDemoKB(tc.kbID, writerHarness.BlobRoot(), tc.embedder))
			require.NoError(t, tc.mutate(ctx, writerHarness.KB(), tc.kbID))
			tc.verify(t, ctx, readerHarness.KB(), tc.kbID)
		})
	}
}

func testKBUpsertUploadBootstrap(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-upsert-bootstrap"
	embedder := newFixtureEmbedder(3)
	sharedBlobRoot := SharedBlobRoot(t)

	writerHarness := NewTestHarness(t, kbID).WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
	defer writerHarness.Cleanup()
	readerHarness := NewTestHarness(t, kbID).WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
	defer readerHarness.Cleanup()

	err := writerHarness.KB().UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}})
	require.NoError(t, err)

	queryVec, err := readerHarness.KB().Embed(ctx, "brand new doc")
	require.NoError(t, err)

	results, err := readerHarness.KB().TopK(ctx, kbID, queryVec, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "c", results[0].ID)
}

func testKBMutateUploadNoRetryConflict(t *testing.T) {
	tests := []struct {
		name             string
		kbID             string
		embedder         Embedder
		mutate           func(context.Context, *KB, string) error
		verifyRetryStats bool
	}{
		{
			name:     "upsert",
			kbID:     "kb-upload-no-retry",
			embedder: newFixtureEmbedder(3),
			mutate: func(ctx context.Context, kbWriter *KB, kbID string) error {
				return kbWriter.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}})
			},
			verifyRetryStats: true,
		},
		{
			name:     "delete",
			kbID:     "kb-delete-no-retry",
			embedder: newDemoEmbedder(),
			mutate: func(ctx context.Context, kbWriter *KB, kbID string) error {
				return kbWriter.DeleteDocsAndUpload(ctx, kbID, []string{"b"}, DeleteDocsOptions{})
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			harness := NewTestHarness(t, tc.kbID).WithEmbedder(tc.embedder).Setup()
			defer harness.Cleanup()

			require.NoError(t, buildDemoKB(tc.kbID, harness.BlobRoot(), tc.embedder))

			localStore := &LocalBlobStore{Root: harness.BlobRoot()}
			flakyStore := &flakyUploadBlobStore{base: localStore, failUploadAttempts: 1}
			observer := &captureRetryObserver{}
			kbWriter := NewKBWithEmbedder(flakyStore, harness.CacheDir(), "128MB", tc.embedder)
			kbWriter.SetMutationRetryObserver(observer)

			err := tc.mutate(ctx, kbWriter, tc.kbID)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrBlobVersionMismatch)

			if tc.verifyRetryStats {
				stats, ok := observer.Last()
				require.True(t, ok)
				assert.Equal(t, 1, stats.Attempts)
				assert.Equal(t, 1, stats.ConflictCount)
				assert.Equal(t, time.Duration(0), stats.TotalRetryDelay)
				assert.False(t, stats.Success)
			}
		})
	}
}

func testKBMutateUploadRetry(t *testing.T) {
	tests := []struct {
		name             string
		kbID             string
		embedder         Embedder
		mutate           func(context.Context, *KB, string) error
		verify           func(*testing.T, context.Context, *KB, string)
		verifyRetryStats bool
	}{
		{
			name:     "upsert",
			kbID:     "kb-upload-retry",
			embedder: newFixtureEmbedder(3),
			mutate: func(ctx context.Context, kbWriter *KB, kbID string) error {
				return kbWriter.UpsertDocsAndUploadWithRetry(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}, 1)
			},
			verify: func(t *testing.T, ctx context.Context, kbReader *KB, kbID string) {
				queryVec, err := kbReader.Embed(ctx, "brand new doc")
				require.NoError(t, err)
				results, err := kbReader.TopK(ctx, kbID, queryVec, 1)
				require.NoError(t, err)
				require.Len(t, results, 1)
				assert.Equal(t, "c", results[0].ID)
			},
			verifyRetryStats: true,
		},
		{
			name:     "delete",
			kbID:     "kb-delete-retry",
			embedder: newDemoEmbedder(),
			mutate: func(ctx context.Context, kbWriter *KB, kbID string) error {
				return kbWriter.DeleteDocsAndUploadWithRetry(ctx, kbID, []string{"b"}, DeleteDocsOptions{}, 1)
			},
			verify: func(t *testing.T, ctx context.Context, kbReader *KB, kbID string) {
				queryVec, err := kbReader.Embed(ctx, "goodbye")
				require.NoError(t, err)
				results, err := kbReader.TopK(ctx, kbID, queryVec, 2)
				require.NoError(t, err)
				for _, r := range results {
					assert.NotEqual(t, "b", r.ID)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sharedBlobRoot := SharedBlobRoot(t)
			writerHarness := NewTestHarness(t, tc.kbID).WithBlobRoot(sharedBlobRoot).WithEmbedder(tc.embedder).Setup()
			defer writerHarness.Cleanup()
			readerHarness := NewTestHarness(t, tc.kbID).WithBlobRoot(sharedBlobRoot).WithEmbedder(tc.embedder).Setup()
			defer readerHarness.Cleanup()
			require.NoError(t, buildDemoKB(tc.kbID, writerHarness.BlobRoot(), tc.embedder))

			localStore := &LocalBlobStore{Root: writerHarness.BlobRoot()}
			flakyStore := &flakyUploadBlobStore{base: localStore, failUploadAttempts: 1}
			observer := &captureRetryObserver{}

			kbWriter := NewKBWithEmbedder(flakyStore, writerHarness.CacheDir(), "128MB", tc.embedder)
			kbWriter.SetMutationRetryObserver(observer)
			kbReader := NewKBWithEmbedder(localStore, readerHarness.CacheDir(), "128MB", tc.embedder)

			require.NoError(t, tc.mutate(ctx, kbWriter, tc.kbID))
			tc.verify(t, ctx, kbReader, tc.kbID)

			if tc.verifyRetryStats {
				stats, ok := observer.Last()
				require.True(t, ok)
				assert.Equal(t, "upsert_docs_upload", stats.Operation)
				assert.Equal(t, 2, stats.Attempts)
				assert.Equal(t, 1, stats.ConflictCount)
				assert.Greater(t, stats.TotalRetryDelay, time.Duration(0))
				assert.True(t, stats.Success)
			}
		})
	}
}

func testKBIngestSharding(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name              string
		kbID              string
		policy            ShardingPolicy
		docs              []Document
			wantManifest      bool
			wantMinShardCount int
		}{
		{
			name: "below_threshold",
			kbID: "kb-ingest-sharding-below-threshold",
			policy: ShardingPolicy{
				ShardTriggerBytes:      1 << 30,
				ShardTriggerVectorRows: 1 << 30,
				TargetShardBytes:       128,
				MaxVectorRowsPerShard:  1,
			},
			docs:              []Document{{ID: "doc-1", Text: "hello world"}},
			wantManifest:      true,
			wantMinShardCount: 1,
		},
		{
			name: "rotate_bytes",
			kbID: "kb-ingest-sharding-rotate-bytes",
			policy: ShardingPolicy{
				ShardTriggerBytes:      1,
				ShardTriggerVectorRows: 1,
				TargetShardBytes:       256 * 1024,
				MaxVectorRowsPerShard:  1 << 30,
			},
			docs:              []Document{{ID: "doc-1", Text: "hello world"}},
			wantManifest:      true,
			wantMinShardCount: 1,
		},
		{
			name: "rotate_rows",
			kbID: "kb-ingest-sharding-rotate-vector-rows",
			policy: ShardingPolicy{
				ShardTriggerBytes:      1 << 30,
				ShardTriggerVectorRows: 1,
				TargetShardBytes:       1 << 30,
				MaxVectorRowsPerShard:  1,
			},
			docs: []Document{
				{ID: "doc-1", Text: "hello world"},
				{ID: "doc-2", Text: "another text"},
			},
			wantManifest:      true,
			wantMinShardCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			embedder := newFixtureEmbedder(3)
			harness := NewTestHarness(t, tc.kbID).
				WithEmbedder(embedder).
				WithOptions(WithShardingPolicy(tc.policy)).
				Setup()
			defer harness.Cleanup()

			require.NoError(t, harness.KB().UpsertDocsAndUpload(ctx, tc.kbID, tc.docs))

			manifestHead, err := harness.KB().BlobStore.Head(ctx, shardManifestKey(tc.kbID))
			if !tc.wantManifest {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, manifestHead.Version)

			manifestPath := filepath.Join(harness.CacheDir(), tc.kbID+"-manifest.json")
			manifest, err := harness.KB().DownloadSnapshotFromShards(ctx, tc.kbID, manifestPath)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(manifest.Shards), tc.wantMinShardCount)

			for _, shard := range manifest.Shards {
				assert.NotEmpty(t, shard.ShardID)
				assert.NotEmpty(t, shard.Key)
				assert.NotEmpty(t, shard.Version)
				assert.Greater(t, shard.SizeBytes, int64(0))
				assert.Greater(t, shard.VectorRows, int64(0))
				assert.False(t, shard.CreatedAt.IsZero())
				assert.False(t, shard.SealedAt.IsZero())
			}
		})
	}
}

func testKBRetryContextCanceled(t *testing.T) {
	kbID := "kb-upload-retry-canceled"

	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
	defer harness.Cleanup()
	if err := buildDemoKB(kbID, harness.BlobRoot(), embedder); err != nil {
		require.FailNowf(t, "test failed", "build kb: %v", err)
	}

	localStore := &LocalBlobStore{Root: harness.BlobRoot()}
	flakyStore := &flakyUploadBlobStore{base: localStore, failUploadAttempts: 100}
	kbWriter := NewKBWithEmbedder(flakyStore, harness.CacheDir(), "128MB", embedder)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := kbWriter.UpsertDocsAndUploadWithRetry(ctx, kbID, []Document{{ID: "c", Text: "brand new doc"}}, 10)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		require.FailNowf(t, "test failed", "expected context canceled, got: %v", err)
	}
	if elapsed > 250*time.Millisecond {
		require.FailNowf(t, "test failed", "expected cancellation to return quickly, took %v", elapsed)
	}
}

func testKBGraphPipelineUpsertTables(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-upsert"

	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
	defer harness.Cleanup()
	if err := buildDemoKB(kbID, harness.BlobRoot(), embedder); err != nil {
		require.FailNowf(t, "test failed", "build kb: %v", err)
	}

	graphBuilder := &GraphBuilder{
		Chunker:       testChunker{},
		Grapher:       testGrapher{},
		Canonicalizer: identityCanonicalizer{},
	}

	kb := NewKB(
		&LocalBlobStore{Root: harness.BlobRoot()},
		harness.CacheDir(),
		WithMemoryLimit("128MB"),
		WithEmbedder(embedder),
		WithGraphBuilder(graphBuilder),
	)

	if err := kb.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "alpha"}}); err != nil {
		require.FailNowf(t, "test failed", "upsert docs first pass: %v", err)
	}

	db, err := kb.Load(ctx, kbID)
	if err != nil {
		require.FailNowf(t, "test failed", "load kb first pass: %v", err)
	}

	var firstDocEntities int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities WHERE doc_id = 'c'`).Scan(&firstDocEntities); err != nil {
		db.Close()
		require.FailNowf(t, "test failed", "count doc_entities first pass: %v", err)
	}
	if firstDocEntities == 0 {
		db.Close()
		require.FailNowf(t, "test failed", "expected doc_entities rows for doc c")
	}

	if err := kb.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "beta"}}); err != nil {
		db.Close()
		require.FailNowf(t, "test failed", "upsert docs second pass: %v", err)
	}

	if err := db.Close(); err != nil {
		require.FailNowf(t, "test failed", "close db: %v", err)
	}

	db, err = kb.Load(ctx, kbID)
	if err != nil {
		require.FailNowf(t, "test failed", "reload kb second pass: %v", err)
	}
	defer db.Close()

	var staleEntityLinks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities WHERE doc_id = 'c' AND entity_id = 'ent:c:alpha'`).Scan(&staleEntityLinks); err != nil {
		require.FailNowf(t, "test failed", "count stale entity links: %v", err)
	}
	if staleEntityLinks != 0 {
		require.FailNowf(t, "test failed", "expected stale doc_entities for alpha to be pruned, found %d", staleEntityLinks)
	}

	var currentEntityLinks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities WHERE doc_id = 'c' AND entity_id = 'ent:c:beta'`).Scan(&currentEntityLinks); err != nil {
		require.FailNowf(t, "test failed", "count current entity links: %v", err)
	}
	if currentEntityLinks == 0 {
		require.FailNowf(t, "test failed", "expected current doc_entities for beta")
	}
}

func testKBHardDeleteCleanupGraphOrphans(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-graph-cleanup"

	embedder := newFixtureEmbedder(3)
	harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
	defer harness.Cleanup()
	if err := buildDemoKB(kbID, harness.BlobRoot(), embedder); err != nil {
		require.FailNowf(t, "test failed", "build kb: %v", err)
	}

	graphBuilder := &GraphBuilder{
		Chunker:       testChunker{},
		Grapher:       testGrapher{},
		Canonicalizer: identityCanonicalizer{},
	}

	kb := NewKB(
		&LocalBlobStore{Root: harness.BlobRoot()},
		harness.CacheDir(),
		WithMemoryLimit("128MB"),
		WithEmbedder(embedder),
		WithGraphBuilder(graphBuilder),
	)

	if err := kb.UpsertDocsAndUpload(ctx, kbID, []Document{{ID: "c", Text: "gamma"}}); err != nil {
		require.FailNowf(t, "test failed", "upsert docs with graph: %v", err)
	}

	if err := kb.DeleteDocsAndUpload(ctx, kbID, []string{"c"}, DeleteDocsOptions{HardDelete: true, CleanupGraph: true}); err != nil {
		require.FailNowf(t, "test failed", "hard delete with graph cleanup: %v", err)
	}

	db, err := kb.Load(ctx, kbID)
	if err != nil {
		require.FailNowf(t, "test failed", "load kb: %v", err)
	}
	defer db.Close()

	var docCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs WHERE id = 'c'`).Scan(&docCount); err != nil {
		require.FailNowf(t, "test failed", "count docs: %v", err)
	}
	if docCount != 0 {
		require.FailNowf(t, "test failed", "expected doc c to be deleted, found %d", docCount)
	}

	var edgeCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM edges WHERE chunk_id = 'c:chunk'`).Scan(&edgeCount); err != nil {
		require.FailNowf(t, "test failed", "count edges: %v", err)
	}
	if edgeCount != 0 {
		require.FailNowf(t, "test failed", "expected edges for doc c chunk to be removed, found %d", edgeCount)
	}

	var entityCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entities WHERE id IN ('ent:c:gamma', 'shared')`).Scan(&entityCount); err != nil {
		require.FailNowf(t, "test failed", "count entities: %v", err)
	}
	if entityCount != 0 {
		require.FailNowf(t, "test failed", "expected orphan entities to be removed, found %d", entityCount)
	}
}

func testKBUpsertUploadGraphToggle(t *testing.T) {
	ctx := context.Background()
	boolPtr := func(v bool) *bool { return &v }

	testCases := []struct {
		name             string
		withGraphBuilder bool
		graphEnabled     *bool
		expectedErr      error
		expectGraphRows  bool
	}{
		{
			name:             "graph_enabled_true_without_builder_returns_error",
			withGraphBuilder: false,
			graphEnabled:     boolPtr(true),
			expectedErr:      ErrGraphUnavailable,
			expectGraphRows:  false,
		},
		{
			name:             "graph_enabled_false_with_builder_skips_graph_pipeline",
			withGraphBuilder: true,
			graphEnabled:     boolPtr(false),
			expectedErr:      nil,
			expectGraphRows:  false,
		},
		{
			name:             "graph_enabled_true_with_builder_writes_graph_rows",
			withGraphBuilder: true,
			graphEnabled:     boolPtr(true),
			expectedErr:      nil,
			expectGraphRows:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kbID := "kb-graph-toggle-" + strings.ReplaceAll(tc.name, "_", "-")

			embedder := newFixtureEmbedder(3)
			harness := NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
			defer harness.Cleanup()
			if err := buildDemoKB(kbID, harness.BlobRoot(), embedder); err != nil {
				require.FailNowf(t, "test failed", "build kb: %v", err)
			}

			opts := []KBOption{
				WithMemoryLimit("128MB"),
				WithEmbedder(embedder),
			}
			if tc.withGraphBuilder {
				opts = append(opts, WithGraphBuilder(&GraphBuilder{
					Chunker:       testChunker{},
					Grapher:       testGrapher{},
					Canonicalizer: identityCanonicalizer{},
				}))
			}

			writer := NewKB(
				&LocalBlobStore{Root: harness.BlobRoot()},
				harness.CacheDir(),
				opts...,
			)

			err := writer.UpsertDocsAndUploadWithOptions(ctx, kbID, []Document{{ID: "graph-doc", Text: "theta"}}, UpsertDocsOptions{
				GraphEnabled: tc.graphEnabled,
			})
			if tc.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			db, err := writer.Load(ctx, kbID)
			require.NoError(t, err)
			defer db.Close()

			var tableCount int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'doc_entities'`).Scan(&tableCount))
			if !tc.expectGraphRows && tableCount == 0 {
				return
			}
			require.Equal(t, 1, tableCount, "doc_entities table should exist")

			var count int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities WHERE doc_id = 'graph-doc'`).Scan(&count))
			if tc.expectGraphRows {
				assert.Greater(t, count, 0)
			} else {
				assert.Equal(t, 0, count)
			}
		})
	}
}
