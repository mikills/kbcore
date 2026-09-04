package duckdb

import (
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/mikills/minnow/internal/budget"
	"github.com/stretchr/testify/require"
)

func threadsOf(t *testing.T, f *DuckDBArtifactFormat, build bool) int {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.duckdb")
	ctx := t.Context()
	if build {
		conn, release, err := f.openBuildDB(ctx, path)
		require.NoError(t, err)
		defer release()
		defer conn.Close()
		var got int
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT current_setting('threads')`).Scan(&got))
		return got
	}
	conn, err := f.openConfiguredDB(ctx, path)
	require.NoError(t, err)
	defer conn.Close()
	var got int
	require.NoError(t, conn.QueryRowContext(ctx, `SELECT current_setting('threads')`).Scan(&got))
	return got
}

func TestBuildThreads(t *testing.T) {
	t.Run("build gets more threads than a query", func(t *testing.T) {
		if budget.Process().BuildThreadBudget() < 2 {
			t.Skip("needs more than one usable CPU")
		}
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{MemoryLimit: "256MB"}}
		query := threadsOf(t, f, false)
		build := threadsOf(t, f, true)
		require.Equal(t, 1, query, "queries stay single threaded per shard")
		require.Greater(t, build, query, "a build reaches DuckDB with more threads")
	})

	t.Run("an explicit setting reaches duckdb", func(t *testing.T) {
		if budget.Process().BuildThreadBudget() < 2 {
			t.Skip("needs more than one usable CPU")
		}
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{MemoryLimit: "256MB", BuildThreads: 2}}
		require.Equal(t, 2, threadsOf(t, f, true))
	})

	t.Run("the format takes its limit from the budget", func(t *testing.T) {
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{MemoryLimit: "256MB", BuildThreads: 4096}}
		require.Equal(t, budget.Process().BuildThreads(4096), f.buildThreads())
	})
}
