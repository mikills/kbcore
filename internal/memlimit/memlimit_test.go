package memlimit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// shard is a full default shard at a common embedding width.
var shard = Shape{Rows: 75000, Dimensions: 512}

func TestMinDatabaseBytes(t *testing.T) {
	t.Run("tracks raw vector bytes, not rows", func(t *testing.T) {
		// Measured. Same vector bytes, same floor, so row count alone is not
		// what sets it.
		require.Equal(t,
			Shape{Rows: 75000, Dimensions: 256}.MinDatabaseBytes(),
			Shape{Rows: 37500, Dimensions: 512}.MinDatabaseBytes())
	})

	t.Run("covers every measured build", func(t *testing.T) {
		for _, c := range []struct {
			rows, dim int
			measured  int64
		}{
			{18750, 512, 56 << 20},
			{37500, 512, 96 << 20},
			{75000, 256, 96 << 20},
			{37500, 768, 144 << 20},
			{56250, 640, 176 << 20},
			{75000, 512, 192 << 20},
			{100000, 384, 192 << 20},
		} {
			got := Shape{Rows: c.rows, Dimensions: c.dim}.MinDatabaseBytes()
			require.GreaterOrEqualf(t, got, c.measured, "%dx%d built at %dMB", c.rows, c.dim, c.measured>>20)
		}
	})

	t.Run("an unknown shape falls back to the smallest measured build", func(t *testing.T) {
		require.Equal(t, int64(FloorDatabaseBytes), Shape{}.MinDatabaseBytes())
	})
}

func TestDivide(t *testing.T) {
	const ceiling = int64(16) << 30

	t.Run("splits a 16GiB ceiling into fixed shares", func(t *testing.T) {
		plan, err := Limit{Ceiling: ceiling, Source: "test"}.Divide(shard, 16, 0)
		require.NoError(t, err)

		// Pinned, not re-derived: changing a share must fail here.
		require.Equal(t, int64(14745), plan.Budget>>20)
		require.Equal(t, int64(4423), plan.GoHeap>>20)
		require.Equal(t, int64(10321), plan.DuckDBTotal>>20)
		require.Equal(t, "645MB", plan.MemoryLimit())
		require.Equal(t, 16, plan.Databases)
		require.False(t, plan.GoHeapPreset)
	})

	t.Run("every instance together stays inside the budget", func(t *testing.T) {
		// The setting binds one database, so N of them is what the host sees.
		// A floor on the per-database share used to break this quietly.
		for _, c := range []struct {
			ceiling, preset int64
			dbs             int
		}{
			{16 << 30, 0, 16},
			{16 << 30, 8 << 30, 16},
			{4 << 30, 0, 16},
			{2 << 30, 768 << 20, 16},
			{32 << 30, 0, 64},
			{900 << 20, 0, 1},
		} {
			plan, err := Limit{Ceiling: c.ceiling}.Divide(shard, c.dbs, c.preset)
			require.NoErrorf(t, err, "ceiling=%d preset=%d dbs=%d", c.ceiling, c.preset, c.dbs)
			require.LessOrEqualf(t, plan.DuckDBPerDB*int64(plan.Databases)+plan.GoHeap, plan.Budget,
				"ceiling=%d preset=%d dbs=%d", c.ceiling, c.preset, c.dbs)
			require.GreaterOrEqualf(t, plan.DuckDBPerDB, plan.MinPerDB,
				"ceiling=%d preset=%d dbs=%d", c.ceiling, c.preset, c.dbs)
			require.Less(t, plan.Budget, plan.Ceiling)
		}
	})

	t.Run("an existing GOMEMLIMIT takes from DuckDB, not from the ceiling", func(t *testing.T) {
		const preset = int64(768) << 20
		plan, err := Limit{Ceiling: 2 << 30}.Divide(shard, 16, preset)
		require.NoError(t, err)
		require.True(t, plan.GoHeapPreset)
		require.Equal(t, preset, plan.GoHeap)
	})

	t.Run("a tight ceiling gives up databases, not the index build", func(t *testing.T) {
		// The live Fly shape, 1968MiB with a 768MiB Go heap. Dividing by a
		// fixed sixteen left 62MiB each and refused to size at all.
		plan, err := Limit{Ceiling: 1968 << 20}.Divide(shard, 16, 768<<20)
		require.NoError(t, err)
		require.Less(t, plan.Databases, 16)
		require.GreaterOrEqual(t, plan.DuckDBPerDB, shard.MinDatabaseBytes())
	})

	t.Run("a GOMEMLIMIT that starves DuckDB is refused, not rounded up", func(t *testing.T) {
		// Starting on a share too small means dying at the first index build
		// instead of at startup.
		_, err := Limit{Ceiling: 16 << 30}.Divide(shard, 16, 15<<30)
		require.ErrorIs(t, err, ErrTooSmall)
		require.ErrorContains(t, err, "75000 rows at 512 dimensions")
		require.ErrorContains(t, err, "max_vector_rows_per_shard", "the error must name what would fit")
	})

	t.Run("a ceiling too small for one database is refused", func(t *testing.T) {
		_, err := Limit{Ceiling: 512 << 20}.Divide(shard, 16, 0)
		require.ErrorIs(t, err, ErrTooSmall)
	})

	t.Run("the Go share has a floor", func(t *testing.T) {
		plan, err := Limit{Ceiling: 8 << 30}.Divide(shard, 1, 0)
		require.NoError(t, err)
		require.GreaterOrEqual(t, plan.GoHeap, int64(minGoHeap))

		// 30% of this budget is under the floor, so the floor is what applies.
		small, err := Limit{Ceiling: 900 << 20}.Divide(shard, 1, 0)
		require.NoError(t, err)
		require.Equal(t, int64(minGoHeap), small.GoHeap, "a heap too small to hold makes the GC spin")
	})

	t.Run("the divisor is what shrinks the per-database share", func(t *testing.T) {
		one, err := Limit{Ceiling: ceiling}.Divide(shard, 1, 0)
		require.NoError(t, err)
		many, err := Limit{Ceiling: ceiling}.Divide(shard, 16, 0)
		require.NoError(t, err)
		require.Equal(t, one.DuckDBTotal, many.DuckDBTotal, "only the divisor changes")
		require.Equal(t, "10321MB", one.MemoryLimit())
		require.Equal(t, "645MB", many.MemoryLimit())
	})
}

func TestUsable(t *testing.T) {
	t.Run("a confined process must not budget from host memory", func(t *testing.T) {
		l := Limit{Ceiling: 64 << 30, Source: "MemTotal", Confined: true}
		require.ErrorIs(t, l.Usable(), ErrConfined)
		_, err := l.Divide(shard, 16, 0)
		require.ErrorIs(t, err, ErrConfined)
	})

	t.Run("nothing read is a missing ceiling", func(t *testing.T) {
		require.ErrorIs(t, Limit{}.Usable(), ErrNoCeiling)
		require.NotErrorIs(t, Limit{}.Usable(), ErrTooSmall)
	})

	t.Run("Detect agrees with itself", func(t *testing.T) {
		l := Detect()
		if l.Usable() != nil {
			t.Skip("no usable ceiling on this platform")
		}
		require.NotEmpty(t, l.Source)
	})
}

func TestFormatMB(t *testing.T) {
	require.Equal(t, "1024MB", FormatMB(1<<30))
	require.Equal(t, "1MB", FormatMB((1<<20)+(1<<19)), "rounding up would name more than the budget")
	require.Equal(t, "1MB", FormatMB(1), "a floor beats a limit DuckDB rejects")
}
