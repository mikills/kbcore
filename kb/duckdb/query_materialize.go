package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	kb "github.com/mikills/minnow/kb"
)

type rankedDocRef struct {
	ID       string
	Distance float64
}

type shardRankedDocRef struct {
	ShardIndex int
	Ref        rankedDocRef
}

func queryTopKRefsWithDB(
	ctx context.Context,
	db *sql.DB,
	queryVec []float32,
	k int,
	validateDimension bool,
) ([]rankedDocRef, error) {
	if k <= 0 {
		return []rankedDocRef{}, nil
	}
	if err := validateQueryVectorForDB(ctx, db, queryVec, validateDimension, "query vector dimension is incompatible with stored vectors"); err != nil {
		return nil, err
	}
	vecStr := FormatVectorForSQL(queryVec)
	// See queryTopKWithDB for why the vector literal appears in both SELECT and
	// ORDER BY: DuckDB's VSS optimizer requires the exact expression in ORDER BY
	// and does not support array parameter binding for the HNSW pattern.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, array_distance(embedding, %s::FLOAT[%d]) as distance
		FROM docs
		ORDER BY array_distance(embedding, %s::FLOAT[%d])
		LIMIT %d
	`, vecStr, len(queryVec), vecStr, len(queryVec), k))
	if err != nil {
		return nil, kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("query refs failed: %w", err),
			"vector query dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()
	refs := make([]rankedDocRef, 0, k)
	for rows.Next() {
		var ref rankedDocRef
		if err := rows.Scan(&ref.ID, &ref.Distance); err != nil {
			return nil, fmt.Errorf("failed to scan ranked result: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ranked rows iteration error: %w", err)
	}
	return refs, nil
}

func queryDocPayloadsByID(ctx context.Context, db *sql.DB, ids []string) (map[string]docPayload, error) {
	if len(ids) == 0 {
		return map[string]docPayload{}, nil
	}
	placeholders := kb.BuildInClausePlaceholders(len(ids))
	metadataExpr, err := docsMetadataSelectExpr(ctx, db)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT id, content, media_refs, %s FROM docs WHERE id IN (%s)`, metadataExpr, placeholders)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query doc payloads: %w", err)
	}
	defer rows.Close()
	payloads := make(map[string]docPayload, len(ids))
	for rows.Next() {
		var id string
		var payload docPayload
		var mediaRefsRaw sql.NullString
		var metadataRaw sql.NullString
		if err := rows.Scan(&id, &payload.Content, &mediaRefsRaw, &metadataRaw); err != nil {
			return nil, fmt.Errorf("failed to scan doc payload: %w", err)
		}
		payload.MediaRefs, _ = decodeMediaRefs(mediaRefsRaw)
		payload.Metadata, _ = decodeMetadata(metadataRaw)
		payloads[id] = payload
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doc payload rows iteration error: %w", err)
	}
	return payloads, nil
}

type docPayload struct {
	Content   string
	MediaRefs []kb.ChunkMediaRef
	Metadata  map[string]any
}

func hydrateRankedResults(ctx context.Context, db *sql.DB, refs []rankedDocRef) ([]kb.QueryResult, error) {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.ID)
	}
	payloads, err := queryDocPayloadsByID(ctx, db, ids)
	if err != nil {
		return nil, err
	}
	results := make([]kb.QueryResult, 0, len(refs))
	for _, ref := range refs {
		payload, ok := payloads[ref.ID]
		if !ok {
			continue
		}
		results = append(
			results,
			kb.QueryResult{
				ID:        ref.ID,
				Content:   payload.Content,
				Distance:  ref.Distance,
				MediaRefs: payload.MediaRefs,
				Metadata:  payload.Metadata,
			},
		)
	}
	return results, nil
}

func mergeRankedShardRefs(shards [][]rankedDocRef, k int) []shardRankedDocRef {
	refs := make([]shardRankedDocRef, 0)
	for shardIndex, shard := range shards {
		for _, ref := range shard {
			refs = append(refs, shardRankedDocRef{ShardIndex: shardIndex, Ref: ref})
		}
	}
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].Ref.Distance != refs[j].Ref.Distance {
			return refs[i].Ref.Distance < refs[j].Ref.Distance
		}
		return refs[i].Ref.ID < refs[j].Ref.ID
	})
	if len(refs) > k {
		refs = refs[:k]
	}
	return refs
}
