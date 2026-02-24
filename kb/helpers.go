package kb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

func formatVectorForSQL(vec []float32) string {
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = fmt.Sprintf("%.6f", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func tableExists(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, tableName string) (bool, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", tableName))
	if err != nil {
		return false, nil
	}
	rows.Close()
	return true, nil
}

func buildInClausePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}

func mapKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func copyFileSync(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}

func replaceFileWithCopy(src, dest string) error {
	tmpDest := fmt.Sprintf("%s.tmp-%d", dest, time.Now().UnixNano())
	if err := copyFileSync(src, tmpDest); err != nil {
		return err
	}
	defer os.Remove(tmpDest)

	if err := os.Rename(tmpDest, dest); err != nil {
		return err
	}

	return nil
}

func fileContentSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ensureGraphQueryReady(ctx context.Context, db *sql.DB) error {
	requiredTables := []string{"edges", "doc_entities"}
	for _, tableName := range requiredTables {
		ok, err := tableExists(ctx, db, tableName)
		if err != nil {
			return err
		}
		if !ok {
			return ErrGraphQueryUnavailable
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
		return ErrGraphQueryUnavailable
	}

	return nil
}

func queryTopKWithDB(ctx context.Context, db *sql.DB, queryVec []float32, k int) ([]QueryResult, error) {
	if k <= 0 {
		return []QueryResult{}, nil
	}
	if len(queryVec) == 0 {
		return nil, fmt.Errorf("query vector cannot be empty")
	}

	vecStr := formatVectorForSQL(queryVec)
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, content, array_distance(embedding, %s::FLOAT[%d]) as distance
		FROM docs
		ORDER BY distance
		LIMIT %d
	`, vecStr, len(queryVec), k))
	if err != nil {
		return nil, wrapEmbeddingDimensionMismatch(
			fmt.Errorf("query failed: %w", err),
			"vector query dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()

	results := make([]QueryResult, 0, k)
	for rows.Next() {
		var r QueryResult
		if err := rows.Scan(&r.ID, &r.Content, &r.Distance); err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

type docMatch struct {
	Content  string
	Distance float64
}

func queryDocMatchesForIDs(ctx context.Context, db *sql.DB, queryVec []float32, ids []string) (map[string]docMatch, error) {
	if len(ids) == 0 {
		return map[string]docMatch{}, nil
	}

	vecStr := formatVectorForSQL(queryVec)
	placeholders := buildInClausePlaceholders(len(ids))
	query := fmt.Sprintf(`
		SELECT id, content, array_distance(embedding, %s::FLOAT[%d]) as distance
		FROM docs
		WHERE id IN (%s)
	`, vecStr, len(queryVec), placeholders)

	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, wrapEmbeddingDimensionMismatch(
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
		if err := rows.Scan(&id, &content, &distance); err != nil {
			return nil, fmt.Errorf("failed to scan query result: %w", err)
		}
		results[id] = docMatch{Content: content, Distance: distance}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows iteration error: %w", err)
	}

	return results, nil
}

func expandedFromVector(results []QueryResult) []ExpandedResult {
	expanded := make([]ExpandedResult, 0, len(results))
	for _, r := range results {
		sim := 1.0 / (1.0 + r.Distance)
		expanded = append(expanded, ExpandedResult{
			ID:         r.ID,
			Content:    r.Content,
			Distance:   r.Distance,
			GraphScore: 0,
			Score:      sim,
		})
	}
	return expanded
}

func topNEntityScores(scores map[string]float64, n int) map[string]float64 {
	if n <= 0 || len(scores) <= n {
		return scores
	}
	type pair struct {
		id    string
		score float64
	}
	pairs := make([]pair, 0, len(scores))
	for id, score := range scores {
		pairs = append(pairs, pair{id: id, score: score})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score == pairs[j].score {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].score > pairs[j].score
	})
	if len(pairs) > n {
		pairs = pairs[:n]
	}
	result := make(map[string]float64, len(pairs))
	for _, p := range pairs {
		result[p.id] = p.score
	}
	return result
}

func activeDocCount(ctx context.Context, db *sql.DB) (int, error) {
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

func checkpointDB(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CHECKPOINT`); err != nil {
		return fmt.Errorf("checkpoint db: %w", err)
	}
	return nil
}

func checkpointAndCloseDB(ctx context.Context, db *sql.DB, closeContext string) error {
	if err := checkpointDB(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("%s: %w", closeContext, err)
	}
	return nil
}
