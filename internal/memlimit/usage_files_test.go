package memlimit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkingSet(t *testing.T) {
	t.Run("subtracts reclaimable page cache", func(t *testing.T) {
		dir := writeTree(t, map[string]string{
			"memory.current": "8000000000",
			"memory.stat":    "anon 1000000000\ninactive_file 6000000000\nactive_file 500000000\n",
		})
		got, ok := workingSet(dir)
		require.True(t, ok)
		// Counting the cache would read as 8GB of pressure the process cannot
		// shed, and pin it at critical for good.
		require.Equal(t, int64(2000000000), got)
	})

	t.Run("a cgroup without the breakdown reports nothing", func(t *testing.T) {
		dir := writeTree(t, map[string]string{"memory.current": "8000000000"})
		_, ok := workingSet(dir)
		require.False(t, ok, "falling back to memory.current is the bug this avoids")
	})

	t.Run("cache larger than usage does not go negative", func(t *testing.T) {
		dir := writeTree(t, map[string]string{
			"memory.current": "1000",
			"memory.stat":    "inactive_file 4000\n",
		})
		got, ok := workingSet(dir)
		require.True(t, ok)
		require.Zero(t, got)
	})

	t.Run("an unreadable cgroup reports nothing", func(t *testing.T) {
		_, ok := workingSet(filepath.Join(t.TempDir(), "absent"))
		require.False(t, ok)
	})
}

func TestStatField(t *testing.T) {
	dir := writeTree(t, map[string]string{
		"memory.stat": "anon 12\nfile 34\ninactive_file 56\nslab 78\n",
	})
	path := filepath.Join(dir, "memory.stat")

	got, ok := statField(path, "inactive_file")
	require.True(t, ok)
	require.Equal(t, int64(56), got)

	// A prefix match would return anon's value for a field that is not there.
	_, ok = statField(path, "inactive")
	require.False(t, ok)

	_, ok = statField(path, "missing")
	require.False(t, ok)
}

func TestStatmResident(t *testing.T) {
	dir := writeTree(t, map[string]string{"statm": "2000 512 100 1 0 300 0\n"})
	got, ok := statmResident(filepath.Join(dir, "statm"))
	require.True(t, ok)
	require.Equal(t, int64(512)*int64(os.Getpagesize()), got, "the second field is resident pages")

	_, ok = statmResident(filepath.Join(dir, "absent"))
	require.False(t, ok)
}
