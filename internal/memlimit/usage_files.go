package memlimit

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func workingSet(dir string) (int64, bool) {
	current, found := readCgroupValue(filepath.Join(dir, "memory.current"))
	if !found || current <= 0 {
		// A v1 hierarchy names it differently and has no equivalent breakdown
		// worth trusting, so it falls through to this process's own resident
		// pages rather than to a number inflated by cache.
		return 0, false
	}
	inactiveFile, found := statField(filepath.Join(dir, "memory.stat"), "inactive_file")
	if !found {
		// Returning memory.current here is the bug this function exists to
		// avoid: page cache would read as pressure the process cannot shed.
		return 0, false
	}
	return max(current-inactiveFile, 0), true
}

func statField(path, name string) (int64, bool) {
	file, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), " ")
		if !ok || key != name {
			continue
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || parsed < 0 {
			return 0, false
		}
		return parsed, true
	}
	return 0, false
}

// statmResident is the second field of /proc/self/statm, in pages. It is this
// process only, so it misses siblings sharing the ceiling.
func statmResident(path string) (int64, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || pages <= 0 {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}
