package memlimit

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// cgroupLimit takes the tightest limit in the ancestry, since a parent slice
// binds as hard as the leaf. confined means a limit exists that it could not
// read.
func cgroupLimit(root, selfPath string) (limit int64, dir string, confined bool) {
	v2, v1 := cgroupPaths(selfPath)
	tightest, blocked := int64(0), false
	binding := ""
	consider := func(path string) {
		candidate, refused := readCgroupValue(path)
		blocked = blocked || refused
		if candidate > 0 && (tightest == 0 || candidate < tightest) {
			tightest, binding = candidate, filepath.Dir(path)
		}
	}
	for dir := filepath.Join(root, v2); ; dir = filepath.Dir(dir) {
		consider(filepath.Join(dir, "memory.max"))
		// A systemd MemoryHigh= binds even while memory.max says "max".
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
	// nothing. Only a refusal does. An absent file means no controller at all,
	// which is the v2 root, and sizing from host memory there is correct.
	return tightest, binding, blocked
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

// Zero means no ceiling: absent, "max", or v1's unlimited sentinel, which sits
// near the top of int64 and would read as exabytes.
func readCgroupValue(path string) (limit int64, refused bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, !errors.Is(err, fs.ErrNotExist)
	}
	text := strings.TrimSpace(string(raw))
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 || value > 1<<62 {
		return 0, false
	}
	return value, false
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
