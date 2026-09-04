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
	if err := f.budget().AdmitBuild(); err != nil {
		return nil, nil, err
	}
	threads, release := f.budget().AcquireBuildThreads(ctx, f.buildThreads())
	db, err := f.openWithThreads(ctx, dbPath, threads)
	if err != nil {
		release()
		return nil, nil, err
	}
	return db, release, nil
}

func (f *DuckDBArtifactFormat) openWithThreads(ctx context.Context, dbPath string, threads int) (*sql.DB, error) {
	// Each database gets the planned share or what the budget has left. The
	// limit is fixed here for good: DuckDB takes one connection at a time, so a
	// later SET would queue behind whatever long build it needed to reach.
	limit, release := f.budget().OpenDatabase(f.deps.MemoryLimit)
	db, err := connection.Open(
		ctx,
		dbPath,
		connection.Config{
			ExtensionDir: f.deps.ExtensionDir,
			MemoryLimit:  limit,
			TempDir:      f.deps.TempDir,
			OfflineExt:   f.deps.OfflineExt,
			Threads:      threads,
			OnClose:      release,
		},
	)
	if err != nil {
		release()
		return nil, err
	}
	return db, nil
}

func (f *DuckDBArtifactFormat) OpenConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	return f.openConfiguredDB(ctx, dbPath)
}

var duckDBConnectionRootContext = context.Background()

func (f *DuckDBArtifactFormat) LoadVSS(db *sql.DB) error { return loadVSS(db) }

func loadVSS(db *sql.DB) error { return connection.LoadVSSRawContext(duckDBConnectionRootContext, db) }
