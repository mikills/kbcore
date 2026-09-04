package budget

import (
	"context"
	"errors"
	"runtime"
)

// UsableThreads is what the process may run right now. Go 1.25 and later
// recompute GOMAXPROCS from the cgroup CPU quota about once a second, so
// raising a container's CPU limit widens builds without a restart.
func usableThreads() int {
	if procs := runtime.GOMAXPROCS(0); procs > 0 {
		return procs
	}
	return 1
}

// BuildThreads is the thread count for one shard seal or compaction. Queries
// stay single-threaded because several run at once; a build does not.
func (m *Manager) BuildThreads(configured int) int {
	// Every build thread raises DuckDB's per-operator reservation, so shedding
	// them is the fastest way to stop a build climbing.
	if m.Pressure() == PressureCritical {
		return 1
	}
	if configured > 0 {
		return min(configured, usableThreads(), m.buildBudget)
	}
	return min(usableThreads(), m.buildBudget, DefaultBuildThreads)
}

// ErrOverMemoryMark refuses work that would start while the process is already
// past the mark it plans for.
var ErrOverMemoryMark = errors.New("memory use is over the mark, not starting new index build work")

// AdmitBuild decides whether a shard seal or compaction may start. Shrinking a
// share does nothing to a build already running: its memory_limit and threads
// were fixed when it opened, and it keeps every byte it has taken. Declining to
// start the next one is the only lever that bounds the peak rather than
// smearing it, so it is the one that has to say no.
func (m *Manager) AdmitBuild() error {
	if m.Pressure() == PressureCritical {
		return ErrOverMemoryMark
	}
	return nil
}

// AcquireBuildThreads reserves a build's threads and returns the count granted.
// It never blocks forever: a cancelled context falls back to one thread rather
// than failing an ingest over a tuning knob. The pool is sized to the hardware
// because a semaphore cannot be resized, while BuildThreads tracks the quota.
func (m *Manager) AcquireBuildThreads(ctx context.Context, want int) (int, func()) {
	if want <= 1 {
		return 1, func() {}
	}
	want = min(want, m.buildBudget)
	if err := m.buildThreads.Acquire(ctx, int64(want)); err != nil {
		return 1, func() {}
	}
	return want, func() { m.buildThreads.Release(int64(want)) }
}

// BuildThreadBudget is the largest number of build threads the process runs.
func (m *Manager) BuildThreadBudget() int { return m.buildBudget }
