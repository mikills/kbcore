package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/compact"
	"github.com/mikills/minnow/kb/graphbuild"
)

type multigroupStubGrapher struct{}

func (multigroupStubGrapher) Extract(_ context.Context, chunks []graphbuild.Chunk) (*graphbuild.Extraction, error) {
	out := &graphbuild.Extraction{}
	for _, chunk := range chunks {
		a := "a-" + chunk.ChunkID
		b := "b-" + chunk.ChunkID
		out.Entities = append(out.Entities,
			graphbuild.EntityCandidate{Name: a, ChunkID: chunk.ChunkID},
			graphbuild.EntityCandidate{Name: b, ChunkID: chunk.ChunkID},
		)
		out.Edges = append(out.Edges, graphbuild.EdgeCandidate{
			Src: a, Dst: b, RelType: "links", Weight: 1, ChunkID: chunk.ChunkID,
		})
	}
	return out, nil
}

func queryStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	out := make([]string, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		parts := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				parts[i] = "<null>"
				continue
			}
			switch tv := v.(type) {
			case []byte:
				parts[i] = string(tv)
			default:
				parts[i] = fmt.Sprintf("%v", tv)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	require.NoError(t, rows.Err())
	return out
}

func countRows(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRowContext(context.Background(), query).Scan(&n))
	return n
}

func tableExistsInDB(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var n int64
	err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`, table).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

func destHasHNSWIndex(t *testing.T, db *sql.DB) bool {
	t.Helper()
	return countRows(t, db, `SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'docs' AND index_name = 'docs_vec_idx'`) > 0
}

func destFTSQueryErr(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `SELECT COUNT(*) FROM docs WHERE fts_main_docs.match_bm25(id, 'alpha', fields := 'content') IS NOT NULL`)
	return err
}

// TestReconstruct seeds enough shards to force more than one reconstruct
// group, then pins the multi-group contract: full doc/media/graph
// preservation, no corpus-wide HNSW/FTS index on the dest (unlike the
// single-group dest), a present doc_tombstones table, and a follow-up seal
// that re-splits with per-shard indexes.
func TestReconstruct(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-multigroup-reconstruct"
	policy := kb.ShardingPolicy{
		ShardTriggerBytes:      1,
		ShardTriggerVectorRows: 1,
		TargetShardBytes:       512,
		MaxShardBytes:          2048,
	}
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(8)).
		WithOptions(
			kb.WithShardingPolicy(policy),
			kb.WithGraphBuilder(&kb.GraphBuilder{
				Chunker:   kb.TextChunker{ChunkSize: 500},
				Grapher:   multigroupStubGrapher{},
				BatchSize: 32,
			}),
			kb.WithMediaStore(kb.NewInMemoryMediaStore()),
		).
		Setup()
	t.Cleanup(harness.Cleanup)
	af := registerFormatReturning(t, harness)
	loader := harness.KB()

	const docCount = 8
	mediaIDs := make([]string, 0, docCount)
	for i := range 6 {
		mediaIDs = append(mediaIDs, fmt.Sprintf("media-doc-%02d", i))
	}
	for _, id := range append(append([]string{}, mediaIDs...), "media-explicit") {
		require.NoError(t, loader.MediaStore.Put(ctx, kb.MediaObject{
			ID: id, KBID: kbID, State: kb.MediaStatePending, Filename: id + ".bin",
		}))
	}

	docs := make([]kb.Document, 0, docCount)
	for i := range docCount {
		doc := kb.Document{
			ID:   fmt.Sprintf("mg-doc-%02d", i),
			Text: fmt.Sprintf("multigroup document %d with searchable content alpha beta", i),
		}
		switch {
		case i < 6:
			doc.MediaIDs = []string{mediaIDs[i]}
		case i == 6:
			doc.MediaRefs = []kb.ChunkMediaRef{{MediaID: "media-explicit", Role: "illustration"}}
		}
		if i%2 == 0 {
			doc.Metadata = map[string]any{"source": "multigroup-test", "ordinal": i}
		}
		docs = append(docs, doc)
	}
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, docs))

	manifestDoc, err := loader.ManifestStore.Get(ctx, kbID)
	require.NoError(t, err)
	manifest := manifestDoc.Manifest
	require.GreaterOrEqual(t, len(manifest.Shards), 2, "seed must produce multiple shards")

	groups := compact.PartitionForReconstruct(manifest.Shards, af.reconstructMaxBytes())
	require.Greater(t, len(groups), 1, "seed must force a multi-group rebuild under the tiny MaxShardBytes cap")

	// Baseline from the mutable source DB: full corpus before the rebuild.
	sourceDB, err := af.OpenConfiguredDB(ctx, filepath.Join(harness.CacheDir(), kbID, vectorsDuckDBFileName))
	require.NoError(t, err)
	wantDocs := queryStrings(t, sourceDB, `SELECT id, content, COALESCE(media_refs, ''), COALESCE(metadata, '') FROM docs ORDER BY id`)
	wantEntities := queryStrings(t, sourceDB, `SELECT id, name FROM entities ORDER BY id, name`)
	wantDocEntities := queryStrings(t, sourceDB, `SELECT doc_id, entity_id, weight::VARCHAR, chunk_id FROM doc_entities ORDER BY doc_id, entity_id, chunk_id`)
	wantEdges := queryStrings(t, sourceDB, `SELECT src, dst, weight::VARCHAR, rel_type, chunk_id FROM edges ORDER BY src, dst, chunk_id, rel_type`)
	require.Len(t, wantDocs, docCount)
	require.NotEmpty(t, wantEntities, "stub grapher must seed entities or the preservation check is vacuous")
	require.NotEmpty(t, wantEdges, "stub grapher must seed edges or the preservation check is vacuous")
	require.NoError(t, sourceDB.Close())

	multiDest := filepath.Join(t.TempDir(), "multi.duckdb")
	_, err = af.DownloadSnapshotFromShards(ctx, kbID, multiDest)
	require.NoError(t, err)

	t.Run("multi-group preserves corpus", func(t *testing.T) {
		multiDB, err := af.OpenConfiguredDB(ctx, multiDest)
		require.NoError(t, err)
		require.Equal(t, wantDocs, queryStrings(t, multiDB, `SELECT id, content, COALESCE(media_refs, ''), COALESCE(metadata, '') FROM docs ORDER BY id`),
			"multi-group rebuild must preserve every doc, media_refs row, and metadata value")
		require.Equal(t, wantEntities, queryStrings(t, multiDB, `SELECT id, name FROM entities ORDER BY id, name`))
		require.Equal(t, wantDocEntities, queryStrings(t, multiDB, `SELECT doc_id, entity_id, weight::VARCHAR, chunk_id FROM doc_entities ORDER BY doc_id, entity_id, chunk_id`))
		require.Equal(t, wantEdges, queryStrings(t, multiDB, `SELECT src, dst, weight::VARCHAR, rel_type, chunk_id FROM edges ORDER BY src, dst, chunk_id, rel_type`))
		require.True(t, tableExistsInDB(t, multiDB, "doc_tombstones"), "finalize must ensure doc_tombstones on a multi-group dest")
		require.False(t, destHasHNSWIndex(t, multiDB), "multi-group dest must skip the corpus-wide HNSW build")
		require.Error(t, destFTSQueryErr(ctx, multiDB), "multi-group dest must skip the corpus-wide FTS build")
		require.NoError(t, multiDB.Close())
	})

	// Single-group control over the same manifest: same rows, indexes present.
	t.Run("single-group keeps index", func(t *testing.T) {
		bigPolicy := kb.ShardingPolicy{
			ShardTriggerBytes:      1 << 30,
			ShardTriggerVectorRows: 1 << 20,
			TargetShardBytes:       1 << 30,
			MaxShardBytes:          1 << 30,
		}
		bigAF, err := NewArtifactFormat(DuckDBArtifactDeps{
			BlobStore:      loader.BlobStore,
			ManifestStore:  loader.ManifestStore,
			CacheDir:       t.TempDir(),
			MemoryLimit:    "128MB",
			ShardingPolicy: bigPolicy,
			Embed:          loader.Embed,
			GraphBuilder:   func() *kb.GraphBuilder { return loader.GraphBuilder },
			EvictCacheIfNeeded: func(ctx context.Context, protectKBID string) error {
				return loader.EvictCacheIfNeeded(ctx, protectKBID)
			},
			LockFor: loader.LockFor,
			AcquireWriteLease: func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
				return loader.AcquireWriteLease(ctx, kbID)
			},
			EnqueueReplacedShardsForGC: loader.EnqueueReplacedShardsForGC,
			Metrics:                    loader,
		})
		require.NoError(t, err)
		singleGroups := compact.PartitionForReconstruct(manifest.Shards, bigAF.reconstructMaxBytes())
		require.Len(t, singleGroups, 1, "huge cap must collapse the same manifest to one group")
		singleDest := filepath.Join(t.TempDir(), "single.duckdb")
		_, err = bigAF.DownloadSnapshotFromShards(ctx, kbID, singleDest)
		require.NoError(t, err)
		singleDB, err := bigAF.OpenConfiguredDB(ctx, singleDest)
		require.NoError(t, err)
		require.Equal(t, wantDocs, queryStrings(t, singleDB, `SELECT id, content, COALESCE(media_refs, ''), COALESCE(metadata, '') FROM docs ORDER BY id`))
		require.True(t, destHasHNSWIndex(t, singleDB), "single-group dest must keep the direct-open HNSW index")
		require.NoError(t, destFTSQueryErr(ctx, singleDB), "single-group dest must keep the direct-open FTS index")
		require.NoError(t, singleDB.Close())
	})

	// Next seal off the index-less dest: re-split, per-shard indexes, media IDs.
	t.Run("reseal re-splits with per-shard indexes", func(t *testing.T) {
		resealed, err := af.BuildArtifacts(ctx, kbID, multiDest, 512)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resealed), 2, "reseal must re-split the corpus into shards")
		gotMedia := make(map[string]struct{})
		gotDocs := int64(0)
		for _, part := range resealed {
			partPath := filepath.Join(t.TempDir(), part.ShardID+".duckdb")
			require.NoError(t, loader.BlobStore.Download(ctx, part.Key, partPath))
			partDB, err := af.OpenConfiguredDB(ctx, partPath)
			require.NoError(t, err)
			require.True(t, destHasHNSWIndex(t, partDB), "resealed shard %s must carry its own HNSW index", part.ShardID)
			require.NoError(t, destFTSQueryErr(ctx, partDB), "resealed shard %s must carry its own FTS index", part.ShardID)
			gotDocs += countRows(t, partDB, `SELECT COUNT(*) FROM docs`)
			require.NoError(t, partDB.Close())
			for _, id := range part.MediaIDs {
				gotMedia[id] = struct{}{}
			}
			_ = os.Remove(partPath)
		}
		require.Equal(t, int64(docCount), gotDocs, "resealed shards must hold every doc")
		wantMedia := append(append([]string{}, mediaIDs...), "media-explicit")
		sort.Strings(wantMedia)
		gotMediaList := make([]string, 0, len(gotMedia))
		for id := range gotMedia {
			gotMediaList = append(gotMediaList, id)
		}
		sort.Strings(gotMediaList)
		require.Equal(t, wantMedia, gotMediaList, "reseal must recover every referenced media id via collectShardMediaIDs")
	})
}
