//go:build linux

package memlimit

const cgroupRoot = "/sys/fs/cgroup"

func detectCeiling() Limit {
	limit, dir, confined := cgroupLimit(cgroupRoot, "/proc/self/cgroup")
	if limit > 0 {
		return Limit{Ceiling: limit, Source: "cgroup", dir: dir}
	}
	total, ok := procMemTotal("/proc/meminfo")
	if !ok {
		return Limit{Confined: confined}
	}
	// MemTotal is the host's memory under every runtime but lxcfs, so reporting
	// it for a confined process would size for a machine we cannot have.
	return Limit{Ceiling: total, Source: "MemTotal", Confined: confined}
}
