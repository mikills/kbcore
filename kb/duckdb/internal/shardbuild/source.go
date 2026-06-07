package shardbuild

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/mikills/minnow/kb/duckdb/internal/reconstruct"
)

type OpenDBFunc func(context.Context, string) (*sql.DB, error)
type EnsureGraphTablesFunc func(context.Context, *sql.DB) error

func OpenAttached(ctx context.Context, sourceDBPath string, shardPath string, openDB OpenDBFunc) (*sql.DB, error) {
	shardDB, err := openDB(ctx, shardPath)
	if err != nil {
		return nil, err
	}
	if _, err := shardDB.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS src (READ_ONLY)", quoteSQLString(sourceDBPath))); err != nil {
		if closeErr := shardDB.Close(); closeErr != nil {
			return nil, fmt.Errorf("%w; close shard db after attach failure: %v", err, closeErr)
		}
		return nil, err
	}
	return shardDB, nil
}

func DetachSource(ctx context.Context, db *sql.DB) {
	_, _ = db.ExecContext(context.WithoutCancel(ctx), "DETACH src")
}

func CopyRowRange(ctx context.Context, shardDB *sql.DB, hasTombstones bool, limit int, lastDocID string) error {
	whereClauses := make([]string, 0, 2)
	if hasTombstones {
		whereClauses = append(whereClauses, "NOT EXISTS (SELECT 1 FROM src.doc_tombstones t WHERE t.doc_id = d.id)")
	}
	if lastDocID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("d.id > '%s'", quoteSQLString(lastDocID)))
	}
	where := ""
	if len(whereClauses) > 0 {
		where = "WHERE " + strings.Join(whereClauses, " AND ")
	}
	metadataExpr := "NULL AS metadata"
	if ok, err := attachedColumnExists(ctx, shardDB, "src", "docs", "metadata"); err != nil {
		return err
	} else if ok {
		metadataExpr = "d.metadata AS metadata"
	}
	_, err := shardDB.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE docs AS
		SELECT d.id, d.content, d.embedding, d.media_refs, %s
		FROM src.docs d
		%s
		ORDER BY d.id
		LIMIT %d
	`, metadataExpr, where, limit))
	return err
}

func attachedColumnExists(ctx context.Context, db *sql.DB, alias string, table string, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s.%s LIMIT 0", column, alias, table))
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found") ||
			strings.Contains(msg, "not exist") {
			return false, nil
		}
		return false, err
	}
	return true, rows.Close()
}

func CopyGraphTables(ctx context.Context, shardDB *sql.DB, ensureGraphTables EnsureGraphTablesFunc) error {
	if err := ensureGraphTables(ctx, shardDB); err != nil {
		return err
	}
	ok, err := SourceGraphTablesReady(ctx, shardDB)
	if err != nil || !ok {
		return err
	}
	if _, err := shardDB.ExecContext(ctx, `
		INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id)
		SELECT de.doc_id, de.entity_id, de.weight, de.chunk_id
		FROM src.doc_entities de
		JOIN docs d ON d.id = de.doc_id
	`); err != nil {
		return err
	}
	if _, err := shardDB.ExecContext(ctx, `
		INSERT OR IGNORE INTO entities (id, name)
		SELECT DISTINCT e.id, e.name
		FROM src.entities e
		JOIN doc_entities de ON de.entity_id = e.id
	`); err != nil {
		return err
	}
	_, err = shardDB.ExecContext(ctx, `
		INSERT INTO edges (src, dst, weight, rel_type, chunk_id)
		SELECT DISTINCT e.src, e.dst, e.weight, e.rel_type, e.chunk_id
		FROM src.edges e
		JOIN doc_entities de ON de.chunk_id = e.chunk_id
	`)
	return err
}

func SourceGraphTablesReady(ctx context.Context, db *sql.DB) (bool, error) {
	for _, table := range []string{"entities", "edges", "doc_entities"} {
		ok, err := reconstruct.AttachedTableExists(ctx, db, "src", table)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func CountActiveDocs(ctx context.Context, db *sql.DB, hasTombstones bool) (int64, error) {
	query := `SELECT COUNT(*) FROM docs d`
	if hasTombstones {
		query += ` WHERE NOT EXISTS (SELECT 1 FROM doc_tombstones t WHERE t.doc_id = d.id)`
	}
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func MaxDocID(ctx context.Context, db *sql.DB) (string, error) {
	var maxDocID string
	if err := db.QueryRowContext(ctx, `SELECT MAX(id) FROM docs`).Scan(&maxDocID); err != nil {
		return "", err
	}
	return maxDocID, nil
}

func ValidateLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("limit must be > 0")
	}
	return nil
}

func quoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
