package duckdb

import (
	"context"
	"database/sql"

	"github.com/mikills/minnow/kb/duckdb/internal/connection"
)

const DefaultExtensionDir = connection.DefaultExtensionDir

func ResolveExtensionDir() string { return connection.ResolveExtensionDir() }

func (f *DuckDBArtifactFormat) openConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	return f.openWithThreads(ctx, dbPath, f.deps.DuckDBThreads)
}

// openBuildDB opens a connection for sealing or compacting a shard. The caller
// must call the returned release once the build is done, since the thread
// budget is shared across every concurrent build in the process.
func (f *DuckDBArtifactFormat) openBuildDB(ctx context.Context, dbPath string) (*sql.DB, func(), error) {
	threads, release := acquireBuildThreads(ctx, f.buildThreads())
	db, err := f.openWithThreads(ctx, dbPath, threads)
	if err != nil {
		release()
		return nil, nil, err
	}
	return db, release, nil
}

func (f *DuckDBArtifactFormat) openWithThreads(ctx context.Context, dbPath string, threads int) (*sql.DB, error) {
	return connection.Open(
		ctx,
		dbPath,
		connection.Config{
			ExtensionDir: f.deps.ExtensionDir,
			MemoryLimit:  f.deps.MemoryLimit,
			TempDir:      f.deps.TempDir,
			OfflineExt:   f.deps.OfflineExt,
			Threads:      threads,
		},
	)
}

func (f *DuckDBArtifactFormat) OpenConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	return f.openConfiguredDB(ctx, dbPath)
}

var duckDBConnectionRootContext = context.Background()

func (f *DuckDBArtifactFormat) LoadVSS(db *sql.DB) error { return loadVSS(db) }

func loadVSS(db *sql.DB) error { return connection.LoadVSSRawContext(duckDBConnectionRootContext, db) }
