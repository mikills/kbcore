//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package main

import "syscall"

func processIsAlive(pid int) (alive, known bool) {
	err := syscall.Kill(pid, 0)
	switch err {
	case nil, syscall.EPERM:
		return true, true
	case syscall.ESRCH:
		return false, true
	default:
		return false, false
	}
}
