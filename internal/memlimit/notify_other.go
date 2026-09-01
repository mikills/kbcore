//go:build !linux

package memlimit

import "errors"

// BackstopMark is where the kernel would be asked to reclaim, if it could be.
const BackstopMark = 0.95

// Watch needs cgroup v2, so only Linux can report pressure without polling.
func Watch(string, int64) (Notifier, error) {
	return nil, errors.New("memory notification needs linux")
}
