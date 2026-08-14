//go:build !windows

package cacheevict

import (
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

func MeasureDiskUsage(path string) (DiskUsage, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return DiskUsage{}, fmt.Errorf("stat filesystem for %s: %w", path, err)
	}
	blockSize := uint64(stat.Bsize)
	capacity := clampedProduct(uint64(stat.Blocks), blockSize)
	available := clampedProduct(uint64(stat.Bavail), blockSize)
	if available > capacity {
		available = capacity
	}
	return DiskUsage{CapacityBytes: int64(capacity), AvailableBytes: int64(available)}, nil
}

func clampedProduct(left, right uint64) uint64 {
	if right != 0 && left > math.MaxInt64/right {
		return math.MaxInt64
	}
	product := left * right
	if product > math.MaxInt64 {
		return math.MaxInt64
	}
	return product
}
