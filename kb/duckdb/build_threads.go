package duckdb

import (
	"context"
	"runtime"

	"golang.org/x/sync/semaphore"
)

// Shard seals are not serialized against each other. Publish worker pools, the
// session reaper, and request-path indexing can each seal at once, and only a
// per-KB lock arbitrates them, so several knowledge bases seal in parallel.
// Every build takes its thread count from this semaphore, which bounds the
// whole process to one build thread per usable CPU no matter how many run.
var buildThreadPool = semaphore.NewWeighted(int64(maxBuildThreads()))

// maxBuildThreads reads GOMAXPROCS, not NumCPU. NumCPU reports the machine's
// cores even under a cgroup CPU quota, so a container limited to one CPU would
// otherwise start four DuckDB threads and spend most of its quota throttled.
func maxBuildThreads() int {
	if procs := runtime.GOMAXPROCS(0); procs > 0 {
		return procs
	}
	return 1
}

// buildThreads is the thread count for one shard seal or compaction. Queries
// stay at one thread per shard because several are probed at once; a build is
// one statement at a time, so single-threaded left most of the machine idle.
func (f *DuckDBArtifactFormat) buildThreads() int {
	if f.deps.BuildThreads > 0 {
		return min(f.deps.BuildThreads, maxBuildThreads())
	}
	return min(maxBuildThreads(), defaultBuildThreads)
}

// About 2x faster at four threads than one on a 75k row shard at 512 dim.
// Beyond four the curve flattens, and at small dimensions usearch lock
// contention can make it slower, so this is a ceiling rather than a target.
const defaultBuildThreads = 4

// acquireBuildThreads reserves a build's threads and returns the count granted
// along with its release. It never blocks forever: a cancelled context falls
// back to one thread rather than failing an ingest over a tuning knob.
func acquireBuildThreads(ctx context.Context, want int) (int, func()) {
	if want <= 1 {
		return 1, func() {}
	}
	if err := buildThreadPool.Acquire(ctx, int64(want)); err != nil {
		return 1, func() {}
	}
	return want, func() { buildThreadPool.Release(int64(want)) }
}
