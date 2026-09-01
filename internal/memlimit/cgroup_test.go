package memlimit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	return root
}

func TestCgroupLimit(t *testing.T) {
	selfV2 := func(t *testing.T, path string) string {
		t.Helper()
		file := filepath.Join(t.TempDir(), "cgroup")
		require.NoError(t, os.WriteFile(file, []byte("0::"+path+"\n"), 0o644))
		return file
	}

	t.Run("takes the tightest limit in the ancestry", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"memory.max":                "8589934592",
			"pod.slice/memory.max":      "2147483648",
			"pod.slice/task/memory.max": "4294967296",
		})
		got, dir, confined := cgroupLimit(root, selfV2(t, "/pod.slice/task"))
		require.False(t, confined)
		require.Equal(t, int64(2147483648), got, "a parent slice binds as hard as the leaf")
		// Usage has to be read from whichever cgroup set the limit: a parent
		// counts our siblings, and our own use would miss the pressure they add.
		require.Equal(t, filepath.Join(root, "pod.slice"), dir)
	})

	t.Run("an unlimited cgroup is not a ceiling", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"memory.max":      "max",
			"task/memory.max": "max",
		})
		got, dir, confined := cgroupLimit(root, selfV2(t, "/task"))
		require.Zero(t, got, "max must fall through to physical memory")
		require.Empty(t, dir, "no limit means no cgroup to read usage from")
		require.False(t, confined, "an unlimited cgroup is readable, just not a ceiling")
	})

	t.Run("reads a v1 hierarchy and skips its unlimited sentinel", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"memory/memory.limit_in_bytes":      "9223372036854771712",
			"memory/task/memory.limit_in_bytes": "1073741824",
		})
		self := filepath.Join(t.TempDir(), "cgroup")
		require.NoError(t, os.WriteFile(self, []byte("4:memory,cpu:/task\n"), 0o644))
		got, dir, confined := cgroupLimit(root, self)
		require.False(t, confined)
		require.Equal(t, int64(1073741824), got)
		require.Equal(t, filepath.Join(root, "memory", "task"), dir)
	})

	t.Run("finds the limit when the leaf path is absent", func(t *testing.T) {
		// Classic v1 Docker: /proc/self/cgroup names /docker/<id>, which does
		// not exist inside the container. The limit is at the mount root.
		root := writeTree(t, map[string]string{"memory/memory.limit_in_bytes": "536870912"})
		self := filepath.Join(t.TempDir(), "cgroup")
		require.NoError(t, os.WriteFile(self, []byte("4:memory:/docker/abc123\n"), 0o644))
		got, dir, confined := cgroupLimit(root, self)
		require.False(t, confined)
		require.Equal(t, int64(536870912), got)
		require.Equal(t, filepath.Join(root, "memory"), dir)
	})

	t.Run("memory.high counts as a ceiling", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"task/memory.max":  "max",
			"task/memory.high": "805306368",
		})
		got, dir, confined := cgroupLimit(root, selfV2(t, "/task"))
		require.False(t, confined)
		require.Equal(t, int64(805306368), got, "MemoryHigh= throttles rather than OOMs, so it binds")
		require.Equal(t, filepath.Join(root, "task"), dir)
	})

	t.Run("no cgroup filesystem at all is not confinement", func(t *testing.T) {
		absent := filepath.Join(t.TempDir(), "no-cgroupfs")
		got, _, confined := cgroupLimit(absent, filepath.Join(t.TempDir(), "absent"))
		require.Zero(t, got)
		require.False(t, confined, "an unconfined process may budget from physical memory")
	})

	t.Run("a mounted but unreadable cgroupfs is confinement", func(t *testing.T) {
		// A cgroup namespace reports "0::/" whatever confines it, so only the
		// mount tells us a limit exists that we cannot see.
		got, _, confined := cgroupLimit(t.TempDir(), selfV2(t, "/"))
		require.Zero(t, got)
		require.True(t, confined, "sizing for the host inside a small container is the bug this guards")
	})
}

func TestProcMemTotal(t *testing.T) {
	t.Run("parses kB into bytes", func(t *testing.T) {
		root := writeTree(t, map[string]string{"meminfo": "MemFree: 1 kB\nMemTotal:   16384000 kB\n"})
		got, ok := procMemTotal(filepath.Join(root, "meminfo"))
		require.True(t, ok)
		require.Equal(t, int64(16384000)<<10, got)
	})

	t.Run("a file without MemTotal yields nothing", func(t *testing.T) {
		root := writeTree(t, map[string]string{"meminfo": "MemFree: 1 kB\n"})
		_, ok := procMemTotal(filepath.Join(root, "meminfo"))
		require.False(t, ok)
	})
}
