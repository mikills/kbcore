//go:build linux

package memlimit

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These run only where CI has put this process in a cgroup v2 it can write, so
// the parts that talk to the kernel are exercised somewhere. Everything else in
// this package is path-based and runs anywhere.
func requireOwnCgroup(t *testing.T) string {
	t.Helper()
	if os.Getenv("MINNOW_CGROUP_IT") == "" {
		t.Skip("set MINNOW_CGROUP_IT=1 inside a writable cgroup v2 to run")
	}
	dir, err := ownCgroupDir()
	require.NoError(t, err)
	return dir
}

func readValue(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return strings.TrimSpace(string(raw))
}

func TestCgroupIntegration(t *testing.T) {
	dir := requireOwnCgroup(t)

	t.Run("the ceiling is the cgroup limit, not the host", func(t *testing.T) {
		limit := Detect()
		require.NoError(t, limit.Usable())
		require.Equal(t, "cgroup", limit.Source)

		want := readValue(t, filepath.Join(dir, "memory.max"))
		expected, err := strconv.ParseInt(want, 10, 64)
		require.NoError(t, err, "the test cgroup needs a numeric memory.max")
		require.Equal(t, expected, limit.Ceiling)
		require.Equal(t, dir, limit.Dir())
	})

	t.Run("usage is a working set, below the limit", func(t *testing.T) {
		usage := Current(dir)
		require.True(t, usage.Ok)
		require.Equal(t, "working set", usage.Source)
		require.Positive(t, usage.Bytes)
		require.Less(t, usage.Bytes, Detect().Ceiling)
	})

	t.Run("memory.high is written at the backstop and restored", func(t *testing.T) {
		before := readValue(t, filepath.Join(dir, "memory.high"))
		ceiling := Detect().Ceiling

		watcher, err := Watch(dir, ceiling)
		require.NoError(t, err)
		require.True(t, watcher.Enforced(), "the cgroup was writable but nothing was enforced")
		// Stall time, not the events counter: memory.high is policed against
		// memory.current, so page cache at the mark wakes an events watcher
		// continuously while nothing actionable has changed.
		require.Equal(t, "memory.pressure", watcher.Source())

		// The kernel stores memory.high rounded down to a page multiple, so the
		// value asked for has to be aligned or the mark is not where we put it.
		during, err := strconv.ParseInt(readValue(t, filepath.Join(dir, "memory.high")), 10, 64)
		require.NoError(t, err)
		want := int64(float64(ceiling) * BackstopMark)
		want -= want % int64(os.Getpagesize())
		require.Equal(t, want, during, "the kernel rounded the mark we asked for")

		require.NoError(t, watcher.Close())
		// Left written, the next start reads it back as the ceiling and takes
		// 95% of that, walking the budget down on every restart.
		require.Equal(t, before, readValue(t, filepath.Join(dir, "memory.high")))
	})

	t.Run("crossing the mark wakes the watcher, and quiet blocks it", func(t *testing.T) {
		watcher, err := Watch(dir, Detect().Ceiling)
		require.NoError(t, err)
		defer func() { _ = watcher.Close() }()

		hog := make([][]byte, 0, 64)
		for range cap(hog) {
			block := make([]byte, 32<<20)
			for i := range block {
				block[i] = 1
			}
			hog = append(hog, block)
		}
		require.NoError(t, watcher.Wait(20000), "reclaiming past memory.high did not wake the watcher")
		require.Len(t, hog, cap(hog))

		hog = nil
		debug.FreeOSMemory()
		require.Eventually(t, func() bool {
			usage := Current(dir)
			return usage.Ok && float64(usage.Bytes) < float64(Detect().Ceiling)*0.5
		}, 20*time.Second, 200*time.Millisecond, "memory was never returned")

		// Page cache still sits near the mark here, so an events watcher would
		// keep firing. Stall time has stopped, so this one has to go quiet.
		start := time.Now()
		_ = watcher.Wait(1000)
		require.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond,
			"the watcher woke with no stall to report")
	})
}
