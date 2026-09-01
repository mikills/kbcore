//go:build linux

package memlimit

import (
	"os"
	"path/filepath"
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
		require.Equal(t, "memory.events", watcher.Source())

		during := readValue(t, filepath.Join(dir, "memory.high"))
		require.Equal(t, strconv.FormatInt(int64(float64(ceiling)*BackstopMark), 10), during)

		require.NoError(t, watcher.Close())
		// Left written, the next start reads it back as the ceiling and takes
		// 95% of that, walking the budget down on every restart.
		require.Equal(t, before, readValue(t, filepath.Join(dir, "memory.high")))
	})

	t.Run("a wake-up does not repeat forever", func(t *testing.T) {
		watcher, err := Watch(dir, Detect().Ceiling)
		require.NoError(t, err)
		defer func() { _ = watcher.Close() }()

		// Allocate past memory.high so the kernel bumps the events counter.
		hog := make([][]byte, 0, 64)
		for range cap(hog) {
			block := make([]byte, 32<<20)
			for i := range block {
				block[i] = 1
			}
			hog = append(hog, block)
		}
		require.NoError(t, watcher.Wait(5000))

		// memory.events is level-triggered through kernfs: without reading it
		// back, every later wait returns instantly and pegs a core.
		start := time.Now()
		require.NoError(t, watcher.Wait(300))
		require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond,
			"the watcher is spinning instead of blocking")
		require.Len(t, hog, cap(hog))
	})
}
