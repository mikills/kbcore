//go:build linux

package memlimit

// currentUsage reports the working set of the cgroup that set the ceiling.
//
// Not memory.current: that counts the page cache, which the kernel only
// reclaims when it has to, so any cgroup writing shard files converges on its
// own limit and stays there. Reading it directly would leave minnow throttled
// for good by cache it could drop at any moment. Subtracting inactive_file is
// what container tooling calls the working set, and it is the part that
// actually has to fit.
func currentUsage(dir string) Usage {
	if dir != "" {
		if bytes, ok := workingSet(dir); ok {
			return Usage{Bytes: bytes, Source: "working set", Ok: true}
		}
	}
	// This process only, so it misses siblings sharing the ceiling. Reported
	// under its own name so the difference is visible in logs.
	if bytes, ok := statmResident("/proc/self/statm"); ok {
		return Usage{Bytes: bytes, Source: "statm", Ok: true}
	}
	return Usage{}
}
