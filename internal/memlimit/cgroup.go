package memlimit

import (
	"bufio"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// cgroupLimit takes the tightest limit in the ancestry, since a parent slice
// binds this process just as hard as its own leaf does. It reports whether a
// cgroup governs this process whose limit it could not read.
func cgroupLimit(root, selfPath string) (limit int64, dir string, confined bool) {
	v2, v1 := cgroupPaths(selfPath)
	tightest, readAny := int64(0), false
	binding := ""
	consider := func(path string) {
		candidate, found := readCgroupValue(path)
		readAny = readAny || found
		if candidate > 0 && (tightest == 0 || candidate < tightest) {
			tightest, binding = candidate, filepath.Dir(path)
		}
	}
	for dir := filepath.Join(root, v2); ; dir = filepath.Dir(dir) {
		consider(filepath.Join(dir, "memory.max"))
		// memory.high throttles into reclaim rather than OOM, so a systemd
		// MemoryHigh= is the real ceiling even while memory.max says "max".
		consider(filepath.Join(dir, "memory.high"))
		if dir == root || !strings.HasPrefix(dir, root) {
			break
		}
	}
	if v1 != "" {
		for dir := filepath.Join(root, "memory", v1); ; dir = filepath.Dir(dir) {
			consider(filepath.Join(dir, "memory.limit_in_bytes"))
			if dir == filepath.Join(root, "memory") || !strings.HasPrefix(dir, root) {
				break
			}
		}
	}
	// A cgroup namespace reports "0::/" whatever confines it, so the path says
	// nothing. The mount does: present but unreadable means a limit we cannot
	// see, and budgeting from host memory there would size for another machine.
	return tightest, binding, !readAny && dirExists(root)
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// cgroupPaths returns the v2 and v1 memory paths this process belongs to.
func cgroupPaths(selfPath string) (v2, v1 string) {
	file, err := os.Open(selfPath)
	if err != nil {
		return "/", ""
	}
	defer func() { _ = file.Close() }()

	v2 = "/"
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.SplitN(scanner.Text(), ":", 3)
		if len(fields) != 3 {
			continue
		}
		switch {
		case fields[0] == "0" && fields[1] == "":
			v2 = fields[2]
		case slices.Contains(strings.Split(fields[1], ","), "memory"):
			v1 = fields[2]
		}
	}
	return v2, v1
}

// readCgroupValue returns the limit and whether the file could be read at all.
// A zero limit with found set means "max", or the sentinel v1 writes for
// unlimited, which sits near the top of int64 and would read as exabytes.
func readCgroupValue(path string) (limit int64, found bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	text := strings.TrimSpace(string(raw))
	if text == "max" {
		return 0, true
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 || value > 1<<62 {
		return 0, true
	}
	return value, true
}

func procMemTotal(path string) (int64, bool) {
	file, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || kb <= 0 {
			return 0, false
		}
		return kb << 10, true
	}
	return 0, false
}
