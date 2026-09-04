//go:build darwin

package memlimit

import "golang.org/x/sys/unix"

func detectCeiling() Limit {
	total, err := unix.SysctlUint64("hw.memsize")
	if err != nil || total > 1<<62 {
		return Limit{}
	}
	return Limit{Ceiling: int64(total), Source: "hw.memsize"}
}
