//go:build windows

package cacheevict

import (
	"fmt"
	"math"

	"golang.org/x/sys/windows"
)

func MeasureDiskUsage(path string) (DiskUsage, error) {
	root, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return DiskUsage{}, err
	}
	var available, capacity uint64
	if err := windows.GetDiskFreeSpaceEx(root, &available, &capacity, nil); err != nil {
		return DiskUsage{}, fmt.Errorf("stat filesystem for %s: %w", path, err)
	}
	if capacity > math.MaxInt64 {
		capacity = math.MaxInt64
	}
	if available > capacity {
		available = capacity
	}
	return DiskUsage{CapacityBytes: int64(capacity), AvailableBytes: int64(available)}, nil
}
