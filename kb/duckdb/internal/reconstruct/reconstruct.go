package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

type EnsureGraphTablesFunc func(context.Context, *sql.DB) error

type MergeOptions struct {
	Alias             string
	PartPath          string
	IsFirst           bool
	EnsureGraphTables EnsureGraphTablesFunc
}

func MergeShardIntoDB(ctx context.Context, db *sql.DB, opts MergeOptions) error {
	if err := attachShardDB(ctx, db, opts.Alias, opts.PartPath); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("DETACH %s", opts.Alias)) }()
	if opts.IsFirst {
		if err := initializeMergedShardTables(ctx, db, opts.Alias, opts.EnsureGraphTables); err != nil {
			return err
		}
	}
	return copyShardTables(ctx, db, opts.Alias)
}

func attachShardDB(ctx context.Context, db *sql.DB, alias, partPath string) error {
	if err := validateSafeIdentifier(alias); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", quoteSQLString(partPath), alias))
	return err
}

func initializeMergedShardTables(
	ctx context.Context,
	db *sql.DB,
	alias string,
	ensureGraphTables EnsureGraphTablesFunc,
) error {
	metadataExpr, err := attachedMetadataSelectExpr(ctx, db, alias)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE docs AS SELECT id, content, embedding, media_refs, %s FROM %s.docs WHERE 1=0", metadataExpr, alias)); err != nil {
		return err
	}
	return ensureGraphTables(ctx, db)
}

func copyShardTables(ctx context.Context, db *sql.DB, alias string) error {
	metadataExpr, err := attachedMetadataSelectExpr(ctx, db, alias)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO docs (id, content, embedding, media_refs, metadata) SELECT id, content, embedding, media_refs, %s FROM %s.docs", metadataExpr, alias)); err != nil {
		return err
	}
	for _, table := range []string{"doc_entities", "edges"} {
		if err := copyAttachedTableIfExists(ctx, db, alias, table, false); err != nil {
			return err
		}
	}
	return copyAttachedTableIfExists(ctx, db, alias, "entities", true)
}

func attachedMetadataSelectExpr(ctx context.Context, db *sql.DB, alias string) (string, error) {
	ok, err := AttachedColumnExists(ctx, db, alias, "docs", "metadata")
	if err != nil {
		return "", err
	}
	if ok {
		return "metadata", nil
	}
	return "NULL AS metadata", nil
}

func copyAttachedTableIfExists(
	ctx context.Context,
	db *sql.DB,
	alias string,
	table string,
	ignoreConflicts bool,
) error {
	ok, err := AttachedTableExists(ctx, db, alias, table)
	if err != nil || !ok {
		return err
	}
	verb := "INSERT INTO"
	if ignoreConflicts {
		verb = "INSERT OR IGNORE INTO"
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("%s %s SELECT * FROM %s.%s", verb, table, alias, table))
	return err
}

func AttachedColumnExists(ctx context.Context, db *sql.DB, alias string, table string, column string) (bool, error) {
	if err := validateSafeIdentifier(alias); err != nil {
		return false, err
	}
	if err := validateSafeIdentifier(table); err != nil {
		return false, err
	}
	if err := validateSafeIdentifier(column); err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT 0", column, alias, table)
	rows, err := db.QueryContext(ctx, query)
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

func AttachedTableExists(ctx context.Context, db *sql.DB, alias, table string) (bool, error) {
	if err := validateSafeIdentifier(alias); err != nil {
		return false, err
	}
	if err := validateSafeIdentifier(table); err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 1", alias, table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found") {
			return false, nil
		}
		return false, err
	}
	return true, rows.Close()
}

func validateSafeIdentifier(s string) error {
	if s == "" {
		return fmt.Errorf("identifier cannot be empty")
	}
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			continue
		}
		return fmt.Errorf("unsafe identifier %q", s)
	}
	return nil
}

func TempPath(dest string) (string, func(), error) {
	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "minnow-reconstruct-*.duckdb")
	if err != nil {
		return "", nil, err
	}
	tmpDest := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		return "", nil, err
	}
	if err := os.Remove(tmpDest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", nil, err
	}
	return tmpDest, func() { removeTemp(tmpDest) }, nil
}

func removeTemp(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Default().Warn("reconstruct temp remove failed", "path", path, "err", err)
	}
}

func quoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
