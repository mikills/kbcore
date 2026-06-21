package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/dimension"
)

func FormatVectorForSQL(vec []float32) string {
	if len(vec) == 0 {
		return "[]"
	}
	buf := make([]byte, 0, 2+len(vec)*10)
	buf = append(buf, '[')
	for i, v := range vec {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendFloat(buf, float64(v), 'f', 6, 32)
	}
	buf = append(buf, ']')
	return string(buf)
}

func tableExists(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, tableName string) (bool, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", tableName))
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found") {
			return false, nil
		}
		return false, err
	}
	return true, rows.Close()
}

func QueryTopKWithDB(ctx context.Context, db *sql.DB, queryVec []float32, k int) ([]kb.QueryResult, error) {
	return queryTopKWithDB(ctx, db, queryVec, k, true)
}

func queryTopKWithDB(
	ctx context.Context,
	db *sql.DB,
	queryVec []float32,
	k int,
	validateDimension bool,
) ([]kb.QueryResult, error) {
	if k <= 0 {
		return []kb.QueryResult{}, nil
	}
	if err := validateQueryVectorForDB(ctx, db, queryVec, validateDimension, "query vector dimension is incompatible with stored vectors"); err != nil {
		return nil, err
	}

	vecStr := FormatVectorForSQL(queryVec)
	metadataExpr, err := docsMetadataSelectExpr(ctx, db)
	if err != nil {
		return nil, err
	}
	// ORDER BY must use the expression directly, not an alias — the VSS optimizer only fires HNSW_INDEX_SCAN on array_distance(...).
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, content, array_distance(embedding, %s::FLOAT[%d]) as distance, media_refs, %s
		FROM docs
		ORDER BY array_distance(embedding, %s::FLOAT[%d])
		LIMIT %d
	`, vecStr, len(queryVec), metadataExpr, vecStr, len(queryVec), k))
	if err != nil {
		return nil, kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("query failed: %w", err),
			"vector query dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()

	results := make([]kb.QueryResult, 0, k)
	for rows.Next() {
		var r kb.QueryResult
		var mediaRefsRaw sql.NullString
		var metadataRaw sql.NullString
		if err := rows.Scan(&r.ID, &r.Content, &r.Distance, &mediaRefsRaw, &metadataRaw); err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		if refs, derr := decodeMediaRefs(mediaRefsRaw); derr == nil {
			r.MediaRefs = refs
		}
		if metadata, derr := decodeMetadata(metadataRaw); derr == nil {
			r.Metadata = metadata
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

func validateQueryVectorForDB(
	ctx context.Context,
	db *sql.DB,
	queryVec []float32,
	validateDimension bool,
	operation string,
) error {
	if len(queryVec) == 0 {
		return fmt.Errorf("query vector cannot be empty")
	}
	if !validateDimension {
		return nil
	}
	expectedDim, err := duckDBEmbeddingDimension(ctx, db)
	if err != nil {
		return err
	}
	return dimension.ValidateVector(queryVec, expectedDim, operation)
}

type docMatch struct {
	Content   string
	Distance  float64
	MediaRefs []kb.ChunkMediaRef
	Metadata  map[string]any
}

func queryDocDistancesForIDs(
	ctx context.Context,
	db *sql.DB,
	queryVec []float32,
	ids []string,
	validateDimension bool,
) (map[string]docMatch, error) {
	if len(ids) == 0 {
		return map[string]docMatch{}, nil
	}
	if err := validateQueryVectorForDB(ctx, db, queryVec, validateDimension, "distance query vector dimension is incompatible with stored vectors"); err != nil {
		return nil, err
	}
	vecStr := FormatVectorForSQL(queryVec)
	placeholders := kb.BuildInClausePlaceholders(len(ids))
	query := fmt.Sprintf(`
		SELECT id, array_distance(embedding, %s::FLOAT[%d]) as distance
		FROM docs
		WHERE id IN (%s)
	`, vecStr, len(queryVec), placeholders)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("distance query failed: %w", err),
			"distance query vector dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()
	results := make(map[string]docMatch, len(ids))
	for rows.Next() {
		var id string
		var distance float64
		if err := rows.Scan(&id, &distance); err != nil {
			return nil, fmt.Errorf("failed to scan query distance: %w", err)
		}
		results[id] = docMatch{Distance: distance}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query distance rows iteration error: %w", err)
	}
	return results, nil
}

func queryDocMatchesForIDs(
	ctx context.Context,
	db *sql.DB,
	queryVec []float32,
	ids []string,
	validateDimension bool,
) (map[string]docMatch, error) {
	if len(ids) == 0 {
		return map[string]docMatch{}, nil
	}
	if err := validateQueryVectorForDB(ctx, db, queryVec, validateDimension, "distance query vector dimension is incompatible with stored vectors"); err != nil {
		return nil, err
	}
	vecStr := FormatVectorForSQL(queryVec)
	placeholders := kb.BuildInClausePlaceholders(len(ids))
	metadataExpr, err := docsMetadataSelectExpr(ctx, db)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
		SELECT id, content, array_distance(embedding, %s::FLOAT[%d]) as distance, media_refs, %s
		FROM docs
		WHERE id IN (%s)
	`, vecStr, len(queryVec), metadataExpr, placeholders)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("distance query failed: %w", err),
			"distance query vector dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()
	results := make(map[string]docMatch, len(ids))
	for rows.Next() {
		var id string
		var content string
		var distance float64
		var mediaRefsRaw sql.NullString
		var metadataRaw sql.NullString
		if err := rows.Scan(&id, &content, &distance, &mediaRefsRaw, &metadataRaw); err != nil {
			return nil, fmt.Errorf("failed to scan query result: %w", err)
		}
		refs, _ := decodeMediaRefs(mediaRefsRaw)
		metadata, _ := decodeMetadata(metadataRaw)
		results[id] = docMatch{Content: content, Distance: distance, MediaRefs: refs, Metadata: metadata}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows iteration error: %w", err)
	}
	return results, nil
}

func docsMetadataSelectExpr(ctx context.Context, db *sql.DB) (string, error) {
	hasMetadata, err := docsColumnExists(ctx, db, "metadata")
	if err != nil {
		return "", err
	}
	if hasMetadata {
		return "metadata", nil
	}
	return "NULL AS metadata", nil
}

func docsColumnExists(ctx context.Context, db *sql.DB, columnName string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_name = 'docs' AND column_name = ?
	`, columnName).Scan(&count); err != nil {
		return false, fmt.Errorf("check docs.%s column: %w", columnName, err)
	}
	return count > 0, nil
}

func ensureGraphQueryReady(ctx context.Context, db *sql.DB) error {
	requiredTables := []string{"edges", "doc_entities"}
	for _, tableName := range requiredTables {
		ok, err := tableExists(ctx, db, tableName)
		if err != nil {
			return err
		}
		if !ok {
			return kb.ErrGraphQueryUnavailable
		}
	}

	var edgeCount int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM edges`).Scan(&edgeCount); err != nil {
		return fmt.Errorf("count edges: %w", err)
	}
	var docEntityCount int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities`).Scan(&docEntityCount); err != nil {
		return fmt.Errorf("count doc_entities: %w", err)
	}
	if edgeCount == 0 || docEntityCount == 0 {
		return kb.ErrGraphQueryUnavailable
	}

	return nil
}

func ActiveDocCount(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM docs d
		WHERE NOT EXISTS (SELECT 1 FROM doc_tombstones t WHERE t.doc_id = d.id)
	`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active docs: %w", err)
	}
	return count, nil
}

func ensureDocTombstonesTable(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS doc_tombstones (
			doc_id TEXT PRIMARY KEY,
			deleted_at TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create doc_tombstones table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_doc_tombstones_doc_id ON doc_tombstones(doc_id)`); err != nil {
		return fmt.Errorf("create doc_tombstones index: %w", err)
	}
	return nil
}

// ValidateSafeIdentifier rejects strings outside [A-Za-z0-9_].
// Used as defense in depth at every call site that interpolates SQL
// identifiers via fmt.Sprintf.
func ValidateSafeIdentifier(s string) error {
	if s == "" {
		return fmt.Errorf("unsafe empty SQL identifier")
	}
	for _, r := range s {
		ok := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if !ok {
			return fmt.Errorf("unsafe SQL identifier: %q", s)
		}
	}
	return nil
}

func CheckpointDB(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CHECKPOINT`); err != nil {
		return fmt.Errorf("checkpoint db: %w", err)
	}
	return nil
}

func CheckpointAndCloseDB(ctx context.Context, db *sql.DB, closeContext string) error {
	if err := CheckpointDB(ctx, db); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return fmt.Errorf("checkpoint db: %w; close db after checkpoint failure: %v", err, closeErr)
		}
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("%s: %w", closeContext, err)
	}
	return nil
}
