package budget

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/internal/memlimit"
)

// testShard is the default sharding.max_shard_bytes.
const testShard = int64(64) << 20

func planFor(t *testing.T, ceiling int64) memlimit.Plan {
	t.Helper()
	plan, err := memlimit.Limit{Ceiling: ceiling, Source: "test"}.Divide(testShard, CachedReaders, 0)
	require.NoError(t, err)
	return plan
}

func TestDatabaseShares(t *testing.T) {
	t.Run("the total stays inside the budget however many open", func(t *testing.T) {
		m := New(planFor(t, 16<<30), true)
		var releases []func()
		issued := int64(0)
		for i := 1; i <= CachedReaders*3; i++ {
			limit, release := m.OpenDatabase("256MB")
			releases = append(releases, release)
			require.Equal(t, int64(i), m.LiveDatabases())
			issued += parseMB(t, limit) << 20
		}
		// Bytes against bytes. The total is the bound, and the index build
		// floor is the one documented way past it: every database beyond the
		// planned count gets the floor once the total is spent.
		overshoot := int64(CachedReaders*3-m.CachedDatabases()) * m.minPerDB()
		require.LessOrEqual(t, issued, m.plan.DuckDBTotal+overshoot)
		require.Greater(t, issued, m.plan.DuckDBTotal, "the floor should have been reached by 48")
		// FormatMB floors, so DuckDB is always told slightly less than the
		// manager reserved. Erring the other way would overcommit.
		require.GreaterOrEqual(t, m.IssuedBytes(), issued, "duckdb was told more than was reserved")
		require.Less(t, m.IssuedBytes()-issued, int64(CachedReaders*3)<<20)

		for _, release := range releases {
			release()
		}
		require.Zero(t, m.LiveDatabases())
		require.Zero(t, m.IssuedBytes(), "releasing did not return the share")
	})

	t.Run("a share shrinks as more open", func(t *testing.T) {
		m := New(planFor(t, 16<<30), true)
		first, release := m.OpenDatabase("256MB")
		defer release()
		for range CachedReaders * 4 {
			_, r := m.OpenDatabase("256MB")
			defer r()
		}
		crowded, r := m.OpenDatabase("256MB")
		defer r()
		require.Less(t, parseMB(t, crowded), parseMB(t, first), "the share ignored the crowd")
	})

	t.Run("a share never drops below what can build an index", func(t *testing.T) {
		m := New(planFor(t, 16<<30), true)
		for range 10000 {
			_, r := m.OpenDatabase("256MB")
			defer r()
		}
		limit, r := m.OpenDatabase("256MB")
		defer r()
		require.Equal(t, m.minPerDB()>>20, parseMB(t, limit))
	})

	t.Run("the first database does not take the whole total", func(t *testing.T) {
		m := New(planFor(t, 16<<30), true)
		limit, release := m.OpenDatabase("256MB")
		defer release()
		// Dividing by the live count alone would hand database one everything.
		require.Equal(t, int64(645), parseMB(t, limit))
	})

	t.Run("a release lets the next open have the share back", func(t *testing.T) {
		m := New(planFor(t, 16<<30), true)
		for range CachedReaders * 8 {
			_, r := m.OpenDatabase("256MB")
			r()
		}
		limit, release := m.OpenDatabase("256MB")
		defer release()
		require.Equal(t, int64(645), parseMB(t, limit), "released shares were never reclaimed")
	})

	t.Run("without a plan the configured limit stands", func(t *testing.T) {
		m := New(memlimit.Plan{}, false)
		limit, release := m.OpenDatabase("256MB")
		defer release()
		require.Equal(t, "256MB", limit, "an explicit memory_limit must not be rewritten")
		require.Equal(t, int64(1), m.LiveDatabases(), "an unplanned database still counts")
	})
}

func TestBuildThreadBudget(t *testing.T) {
	t.Run("a build gets more than one thread but not more than the budget", func(t *testing.T) {
		m := New(memlimit.Plan{}, false)
		require.Equal(t, 4, DefaultBuildThreads)
		require.LessOrEqual(t, m.BuildThreads(0), DefaultBuildThreads, "a build must not take the machine")
		require.LessOrEqual(t, m.BuildThreads(0), usableThreads())
		require.Equal(t, m.buildBudget, m.BuildThreads(4096), "an explicit setting escaped the budget")
		require.Equal(t, 2, m.BuildThreads(2), "an explicit setting below the budget is honoured")
	})

	t.Run("follows gomaxprocs down and back up", func(t *testing.T) {
		if runtime.NumCPU() < 2 {
			t.Skip("needs more than one core")
		}
		defer runtime.SetDefaultGOMAXPROCS()
		m := New(memlimit.Plan{}, false)

		runtime.GOMAXPROCS(1)
		require.Equal(t, 1, m.BuildThreads(4096))
		require.Equal(t, 1, m.BuildThreads(0), "the default ignored the quota")
		// Go 1.25+ re-reads the cgroup quota, so more CPU widens builds live.
		runtime.GOMAXPROCS(2)
		require.Equal(t, 2, m.BuildThreads(4096))
		require.Equal(t, 2, m.BuildThreads(0))
	})

	t.Run("a raised gomaxprocs cannot outgrow the pool", func(t *testing.T) {
		defer runtime.SetDefaultGOMAXPROCS()
		m := New(memlimit.Plan{}, false)
		runtime.GOMAXPROCS(m.buildBudget * 2)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// The pool cannot be resized, so a build must never ask past it.
		require.Equal(t, m.buildBudget, m.BuildThreads(4096), "a raised quota escaped the pool")
		require.LessOrEqual(t, m.BuildThreads(0), m.buildBudget)

		granted, release := m.AcquireBuildThreads(ctx, m.buildBudget*2)
		defer release()
		require.Equal(t, m.buildBudget, granted, "asking past the pool parked until the context died")
	})

	t.Run("a build that cannot reserve falls back rather than failing", func(t *testing.T) {
		m := New(memlimit.Plan{}, false)
		granted, release := m.AcquireBuildThreads(context.Background(), m.buildBudget)
		require.Equal(t, m.buildBudget, granted)
		defer release()

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		second, releaseSecond := m.AcquireBuildThreads(cancelled, m.buildBudget)
		require.Equal(t, 1, second)
		releaseSecond()
	})
}

func TestEmbedBudget(t *testing.T) {
	t.Run("one upsert cannot claim the whole process budget", func(t *testing.T) {
		m := New(memlimit.Plan{}, false)
		require.Equal(t, 4, m.EmbedParallelism(0))
		require.Equal(t, 16, m.EmbedParallelism(4096))
		require.Equal(t, 8, m.EmbedParallelism(8))
		require.Equal(t, 64, EmbedBudget)
		require.Less(t, MaxEmbedParallelism, EmbedBudget, "one upsert could starve every other")
	})

	t.Run("bounds holders to the budget", func(t *testing.T) {
		const slots = 3
		m := New(memlimit.Plan{}, false)
		m.SetEmbedBudgetForTest(slots)

		var inFlight, peak atomic.Int64
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for range slots * 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				release, err := m.AcquireEmbed(context.Background())
				require.NoError(t, err)
				defer release()
				current := inFlight.Add(1)
				for {
					high := peak.Load()
					if current <= high || peak.CompareAndSwap(high, current) {
						break
					}
				}
				<-gate
				inFlight.Add(-1)
			}()
		}
		require.Eventually(t, func() bool { return inFlight.Load() == slots },
			5*time.Second, 2*time.Millisecond)
		require.Equal(t, int64(slots), inFlight.Load(), "more holders than the budget allows")
		close(gate)
		wg.Wait()
		require.Equal(t, int64(slots), peak.Load())
	})

	t.Run("a cancelled context does not take a slot", func(t *testing.T) {
		m := New(memlimit.Plan{}, false)
		m.SetEmbedBudgetForTest(1)
		release, err := m.AcquireEmbed(context.Background())
		require.NoError(t, err)

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = m.AcquireEmbed(cancelled)
		require.Error(t, err)

		release()
		second, err := m.AcquireEmbed(context.Background())
		require.NoError(t, err)
		second()
	})
}

func parseMB(t *testing.T, limit string) int64 {
	t.Helper()
	var mb int64
	_, err := fmt.Sscanf(limit, "%dMB", &mb)
	require.NoError(t, err)
	return mb
}
