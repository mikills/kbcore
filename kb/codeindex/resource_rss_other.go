//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd)

package codeindex

func currentProcessRSSBytes() (uint64, bool) {
	return 0, false
}
