//go:build !linux

package memlimit

import "errors"

// BackstopMark is where the kernel would be asked to reclaim, if it could be.
const BackstopMark = 0.95

// Watch needs Linux for anything the kernel can tell us. Elsewhere usage is
// unreadable anyway, so the governor never starts and this is never reached.
func Watch(string, int64) (Notifier, error) {
	return nil, errors.New("memory notification needs linux")
}
