//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package main

func processIsAlive(_ int) (alive, known bool) {
	return false, false
}
