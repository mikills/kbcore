package codeindex

import (
	"context"
	"fmt"
	"runtime"
	"time"
)

const (
	bytesPerKiB           = 1024
	DefaultEmbedBatchSize = 32
	DefaultMaxBatchBytes  = 256 * bytesPerKiB
	DefaultMaxHeapBytes   = bytesPerKiB * bytesPerKiB * bytesPerKiB
	DefaultMaxRSSBytes    = bytesPerKiB * bytesPerKiB * bytesPerKiB
	DefaultThrottle       = 100 * time.Millisecond
	DefaultLargeRepoFiles = 1000
)

type ResourcePolicy struct {
	EmbedBatchSize int
	MaxBatchBytes  int
	Throttle       time.Duration
	MaxHeapBytes   uint64
	MaxRSSBytes    uint64
	LargeRepoFiles int
}

func ResourcePolicyFromOptions(opts Options) ResourcePolicy {
	return ResourcePolicy{
		EmbedBatchSize: opts.EmbedBatchSize,
		MaxBatchBytes:  opts.MaxBatchBytes,
		Throttle:       opts.Throttle,
		MaxHeapBytes:   opts.MaxHeapBytes,
		MaxRSSBytes:    opts.MaxRSSBytes,
		LargeRepoFiles: opts.LargeRepoFiles,
	}.Normalize()
}

func (p ResourcePolicy) Normalize() ResourcePolicy {
	if p.EmbedBatchSize <= 0 {
		p.EmbedBatchSize = DefaultEmbedBatchSize
	}
	if p.MaxBatchBytes <= 0 {
		p.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if p.Throttle < 0 {
		p.Throttle = 0
	} else if p.Throttle == 0 {
		p.Throttle = DefaultThrottle
	}
	if p.MaxHeapBytes == 0 {
		p.MaxHeapBytes = DefaultMaxHeapBytes
	}
	if p.MaxRSSBytes == 0 {
		p.MaxRSSBytes = DefaultMaxRSSBytes
	}
	if p.LargeRepoFiles <= 0 {
		p.LargeRepoFiles = DefaultLargeRepoFiles
	}
	return p
}

func (p ResourcePolicy) ApplyToOptions(opts Options) Options {
	p = p.Normalize()
	opts.EmbedBatchSize = p.EmbedBatchSize
	opts.MaxBatchBytes = p.MaxBatchBytes
	opts.Throttle = p.Throttle
	opts.MaxHeapBytes = p.MaxHeapBytes
	opts.MaxRSSBytes = p.MaxRSSBytes
	opts.LargeRepoFiles = p.LargeRepoFiles
	return opts
}

func (p ResourcePolicy) Validate(prefix string) error {
	if prefix == "" {
		prefix = "code_index"
	}
	if p.EmbedBatchSize <= 0 {
		return fmt.Errorf("%s.embed_batch_size must be > 0, got %d", prefix, p.EmbedBatchSize)
	}
	if p.MaxBatchBytes <= 0 {
		return fmt.Errorf("%s.max_batch_bytes must be > 0, got %d", prefix, p.MaxBatchBytes)
	}
	if p.Throttle < 0 {
		return fmt.Errorf("%s.throttle must be >= 0", prefix)
	}
	if p.MaxHeapBytes == 0 {
		return fmt.Errorf("%s.max_heap_bytes must be > 0, got %d", prefix, p.MaxHeapBytes)
	}
	if p.MaxRSSBytes == 0 {
		return fmt.Errorf("%s.max_rss_bytes must be > 0, got %d", prefix, p.MaxRSSBytes)
	}
	if p.LargeRepoFiles <= 0 {
		return fmt.Errorf("%s.large_repo_files must be > 0, got %d", prefix, p.LargeRepoFiles)
	}
	return nil
}

func (p ResourcePolicy) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.MaxHeapBytes > 0 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.Sys > p.MaxHeapBytes {
			return fmt.Errorf(
				"code index resource guard: heap/system memory %d bytes exceeds max_heap_bytes %d",
				mem.Sys,
				p.MaxHeapBytes,
			)
		}
	}
	if p.MaxRSSBytes > 0 {
		rss, ok := currentProcessRSSBytes()
		if ok && rss > p.MaxRSSBytes {
			return fmt.Errorf(
				"code index resource guard: resident memory %d bytes exceeds max_rss_bytes %d",
				rss,
				p.MaxRSSBytes,
			)
		}
	}
	return nil
}

func (p ResourcePolicy) ThrottleBatch(ctx context.Context) error {
	if p.Throttle <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(p.Throttle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p ResourcePolicy) BatchEndByTextBytes(textBytes []int) int {
	if len(textBytes) == 0 {
		return 0
	}
	end := p.EmbedBatchSize
	if end <= 0 || end > len(textBytes) {
		end = len(textBytes)
	}
	if p.MaxBatchBytes <= 0 {
		return end
	}
	bytes := 0
	for i, n := range textBytes[:end] {
		if i > 0 && bytes+n > p.MaxBatchBytes {
			return i
		}
		bytes += n
	}
	return end
}
