//go:build !linux

package memlimit

// Only Linux reports usage for the cgroup that binds us. Elsewhere the governor
// stays off rather than acting on a number it cannot trust.
func currentUsage(string) Usage { return Usage{} }
