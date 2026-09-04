package configruntime

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
	"github.com/mikills/minnow/kb/config"
)

// shard is the default sharding.max_shard_bytes.
const shard = int64(64) << 20

// The grammar DuckDB's memory_limit accepts.
var duckDBSize = regexp.MustCompile(`^\d+(B|KB|MB|GB|TB)$`)

// stubDetect drives a ceiling this machine does not have, and restores both the
// seam and the process-wide Go limit afterwards.
func stubDetect(t *testing.T, limit memlimit.Limit) {
	t.Helper()
	previousDetect := detect
	previousLimit := debug.SetMemoryLimit(-1)
	previousProcess := budget.Process()
	detect = func() memlimit.Limit { return limit }
	// No inherited GOMEMLIMIT: the runner sets one and the developer machine
	// does not, which silently changes every number below.
	debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() {
		detect = previousDetect
		debug.SetMemoryLimit(previousLimit)
		budget.SetProcess(previousProcess)
	})
}

func TestResolveMemoryLimit(t *testing.T) {
	t.Run("an explicit limit is left alone", func(t *testing.T) {
		for _, raw := range []string{"128MB", "4GB", " 2GB "} {
			got, err := resolveMemoryLimit(raw, shard, quietLogger(), true)
			require.NoError(t, err)
			require.Equal(t, raw, got)
		}
	})

	t.Run("auto divides by the open database count", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		got, err := resolveMemoryLimit("AUTO", shard, quietLogger(), true)
		require.NoError(t, err)
		require.Regexp(t, duckDBSize, got, "an unparseable limit only surfaces at the first query")

		// Pinned: dividing by anything but the planned count changes this.
		require.Equal(t, 16, budget.CachedReaders)
		require.Equal(t, "645MB", got)
	})

	t.Run("an unset limit is sized from the host", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		got, err := resolveMemoryLimit("", shard, quietLogger(), true)
		require.NoError(t, err)
		require.Equal(t, "645MB", got, "an unset limit fell back instead of sizing")
	})

	t.Run("an unset limit falls back rather than failing", func(t *testing.T) {
		// Nobody asked for auto here, so an unreadable ceiling must not stop
		// a deployment that would have run on the fixed default.
		stubDetect(t, memlimit.Limit{})
		got, err := resolveMemoryLimit("", 1<<20, quietLogger(), true)
		require.NoError(t, err)
		require.Equal(t, FallbackMemoryLimit, got)
	})

	t.Run("a 2GB box keeps fewer databases rather than giving up", func(t *testing.T) {
		// The live Fly shape. Dividing by a fixed sixteen left 62MiB each, so
		// sizing gave up and the box ran on a default too small to seal a shard.
		stubDetect(t, memlimit.Limit{Ceiling: 1968 << 20, Source: "cgroup"})
		debug.SetMemoryLimit(768 << 20)

		got, err := resolveMemoryLimit("", shard, quietLogger(), false)
		require.NoError(t, err)

		plan, sizes := budget.Process().Plan()
		require.True(t, sizes)
		require.Less(t, plan.Databases, 16)
		require.GreaterOrEqual(t, parseSizeMB(t, got), memlimit.MinDatabaseBytes(shard)>>20,
			"the shipped default could not finish an index build")
	})

	t.Run("a ceiling too small for one database still gets a governor", func(t *testing.T) {
		// Sizing gives up; back-pressure is what such a box needs most.
		stubDetect(t, memlimit.Limit{Ceiling: 900 << 20, Source: "cgroup"})
		debug.SetMemoryLimit(768 << 20)

		got, err := resolveMemoryLimit("", shard, quietLogger(), false)
		require.NoError(t, err)
		// Not the index build floor: nothing caps how many databases take an
		// unsized limit, so handing out a bigger one would overcommit worse.
		require.Equal(t, FallbackMemoryLimit, got)

		plan, sizes := budget.Process().Plan()
		require.False(t, sizes, "it must not rewrite memory_limit it could not compute")
		require.Equal(t, int64(900<<20), plan.Ceiling, "the governor lost the ceiling it had read")
	})

	t.Run("a confined process refuses to guess", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 64 << 30, Source: "MemTotal", Confined: true})
		_, err := resolveMemoryLimit("auto", shard, quietLogger(), true)
		require.ErrorIs(t, err, memlimit.ErrConfined)
		require.ErrorContains(t, err, "set an explicit size")
	})

	t.Run("an unsupported platform names the real reason", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{})
		_, err := resolveMemoryLimit("auto", shard, quietLogger(), true)
		require.ErrorIs(t, err, memlimit.ErrNoCeiling)
	})
}

func TestGoMemLimit(t *testing.T) {
	t.Run("auto sets the Go limit when nothing else has", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		debug.SetMemoryLimit(math.MaxInt64)
		_, err := resolveMemoryLimit("auto", shard, quietLogger(), false)
		require.NoError(t, err)

		require.Equal(t, int64(4423), debug.SetMemoryLimit(-1)>>20)
	})

	t.Run("an operator's GOMEMLIMIT is kept and taken from DuckDB", func(t *testing.T) {
		const operator = int64(768) << 20
		stubDetect(t, memlimit.Limit{Ceiling: 2 << 30, Source: "test"})
		debug.SetMemoryLimit(operator)
		got, err := resolveMemoryLimit("auto", shard, quietLogger(), false)
		require.NoError(t, err)
		require.Equal(t, operator, debug.SetMemoryLimit(-1), "minnow overrode a limit it did not set")

		// Nine databases of 119MB, not sixteen of 67MB. The Go heap comes out
		// first, and what is left still has to finish an index build.
		require.Equal(t, "119MB", got, "DuckDB was sized as if the Go heap were free")
	})

	t.Run("a second Build does not ratchet the budget down", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		debug.SetMemoryLimit(math.MaxInt64)

		first, err := resolveMemoryLimit("auto", shard, quietLogger(), false)
		require.NoError(t, err)
		firstHeap := debug.SetMemoryLimit(-1)

		second, err := resolveMemoryLimit("auto", shard, quietLogger(), false)
		require.NoError(t, err)
		require.Equal(t, first, second, "the Go share was subtracted twice")
		require.Equal(t, firstHeap, debug.SetMemoryLimit(-1))
	})

	t.Run("a starving GOMEMLIMIT is refused at startup", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		debug.SetMemoryLimit(15 << 30)
		_, err := resolveMemoryLimit("auto", shard, quietLogger(), true)
		require.ErrorIs(t, err, memlimit.ErrTooSmall, "a share too small dies at the first index build")
	})

	t.Run("a dry run touches nothing", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		debug.SetMemoryLimit(math.MaxInt64)
		_, err := resolveMemoryLimit("auto", shard, quietLogger(), true)
		require.NoError(t, err)
		require.Equal(t, int64(math.MaxInt64), debug.SetMemoryLimit(-1))
	})
}

func TestBuildWithAutoMemory(t *testing.T) {
	minimal := func(limit string) string {
		return `
http:
  address: 127.0.0.1:0
storage:
  blob:
    kind: local
    root: blobs
embedder:
  provider: local
  local:
    dim: 8
format:
  kind: duckdb
  duckdb:
    memory_limit: ` + limit + `
`
	}

	t.Run("auto reaches duckdb as a concrete size", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 16 << 30, Source: "test"})
		cfg, err := config.Load(writeTempConfig(t, minimal("auto")))
		require.NoError(t, err)
		require.Equal(t, AutoMemoryLimit, cfg.Format.DuckDB.MemoryLimit, "validation rejected auto")

		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: logger})
		require.NoError(t, err)
		require.NoError(t, rt.Stop(context.Background()))
		require.Contains(t, logs.String(), "duckdb_per_db=645MB", "auto reached duckdb unresolved")
	})

	t.Run("an unusable ceiling fails the build", func(t *testing.T) {
		stubDetect(t, memlimit.Limit{Ceiling: 64 << 30, Source: "MemTotal", Confined: true})
		cfg, err := config.Load(writeTempConfig(t, minimal("auto")))
		require.NoError(t, err)

		_, err = Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.ErrorIs(t, err, memlimit.ErrConfined)
	})
}

// parseSizeMB reads back the megabytes in a DuckDB memory_limit.
func parseSizeMB(t *testing.T, limit string) int64 {
	t.Helper()
	var mb int64
	_, err := fmt.Sscanf(limit, "%dMB", &mb)
	require.NoError(t, err, "limit %q is not in megabytes", limit)
	return mb
}
