package duckdb

import (
	"context"
	"database/sql"

	"github.com/mikills/minnow/kb/duckdb/internal/connection"
)

const DefaultExtensionDir = connection.DefaultExtensionDir

func ResolveExtensionDir() string { return connection.ResolveExtensionDir() }

func (f *DuckDBArtifactFormat) openConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	return connection.Open(
		ctx,
		dbPath,
		connection.Config{
			ExtensionDir: f.deps.ExtensionDir,
			MemoryLimit:  f.deps.MemoryLimit,
			TempDir:      f.deps.TempDir,
			OfflineExt:   f.deps.OfflineExt,
			Threads:      f.deps.DuckDBThreads,
		},
	)
}

func (f *DuckDBArtifactFormat) OpenConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	return f.openConfiguredDB(ctx, dbPath)
}

var duckDBConnectionRootContext = context.Background()

func (f *DuckDBArtifactFormat) LoadVSS(db *sql.DB) error { return loadVSS(db) }

func loadVSS(db *sql.DB) error { return connection.LoadVSSRawContext(duckDBConnectionRootContext, db) }
