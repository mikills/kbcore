//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package codeindex

import (
	"runtime"
	"syscall"
)

func currentProcessRSSBytes() (uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	rss := uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		rss *= bytesPerKiB
	}
	return rss, rss > 0
}
