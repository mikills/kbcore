package duckdb

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
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
		if maxBuildThreads() < 2 {
			t.Skip("needs more than one usable CPU")
		}
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{MemoryLimit: "256MB"}}
		query := threadsOf(t, f, false)
		build := threadsOf(t, f, true)
		require.Equal(t, 1, query, "queries stay single threaded per shard")
		require.Greater(t, build, query, "a build reaches DuckDB with more threads")
	})

	t.Run("an explicit setting reaches duckdb", func(t *testing.T) {
		if maxBuildThreads() < 2 {
			t.Skip("needs more than one usable CPU")
		}
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{MemoryLimit: "256MB", BuildThreads: 2}}
		require.Equal(t, 2, threadsOf(t, f, true))
	})

	t.Run("never exceeds the usable cpus", func(t *testing.T) {
		f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{BuildThreads: 4096}}
		require.Equal(t, maxBuildThreads(), f.buildThreads())
	})

	t.Run("uses gomaxprocs, not the machine's cores", func(t *testing.T) {
		if runtime.NumCPU() < 2 {
			t.Skip("needs more than one core to constrain")
		}
		previous := runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(previous)
		require.Equal(t, 1, maxBuildThreads(), "a cgroup-limited container must not see host cores")
	})

	t.Run("the budget bounds concurrent builds", func(t *testing.T) {
		total := maxBuildThreads()
		granted, release := acquireBuildThreads(t.Context(), total)
		require.Equal(t, total, granted)

		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		second, releaseSecond := acquireBuildThreads(cancelled, total)
		require.Equal(t, 1, second, "a build that cannot reserve falls back rather than failing")
		releaseSecond()
		release()
	})
}
