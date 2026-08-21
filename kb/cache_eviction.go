package kb

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/mikills/minnow/kb/cacheevict"
)

const defaultCacheEvictionRetryWindow = 150 * time.Millisecond

const defaultCacheEvictionRetryTick = 25 * time.Millisecond

func (l *KB) evictCacheIfNeeded(ctx context.Context, protectedEntry string) error {
	l.cacheReserveMu.Lock()
	reserved := l.cacheReservedBytes
	l.cacheReserveMu.Unlock()
	return l.evictCacheProjected(ctx, protectedEntry, reserved)
}

func (l *KB) evictCacheProjected(ctx context.Context, protectedEntry string, incomingBytes int64) error {
	result, budgetExceeded, err := cacheevict.RetryUntilWithinBudget(cacheevict.RetryConfig{
		Window: defaultCacheEvictionRetryWindow,
		Tick:   defaultCacheEvictionRetryTick,
		Now:    l.Clock.Now,
		Sleep: func(d time.Duration) error {
			return sleepWithContext(ctx, d)
		},
		Sweep: func() cacheevict.SweepResult {
			result := l.evictCacheSweepOnce(protectedEntry, incomingBytes)
			l.recordCacheBytesCurrent(result.CurrentBytes)
			return result
		},
	})
	if err != nil {
		return err
	}
	if !budgetExceeded {
		return nil
	}
	l.recordCacheBudgetExceeded()
	return fmt.Errorf(
		"%w: current_bytes=%d max_bytes=%d protected_entries=%d",
		ErrCacheBudgetExceeded,
		result.CurrentBytes,
		result.MaxBytes,
		result.ProtectedEntries,
	)
}

func (l *KB) evictCacheSweepOnce(protectedEntry string, incomingBytes int64) cacheevict.SweepResult {
	l.mu.Lock()
	maxBytes := l.MaxCacheBytes
	entryTTL := l.CacheEntryTTL
	highWatermark := l.CacheHighWatermarkPercent
	lowWatermark := l.CacheLowWatermarkPercent
	minFreeBytes := l.CacheMinFreeBytes
	l.mu.Unlock()
	result := cacheevict.Sweep(cacheevict.Config{
		Root:                 l.CacheDir,
		MaxBytes:             maxBytes,
		TTL:                  entryTTL,
		HighWatermarkPercent: highWatermark,
		LowWatermarkPercent:  lowWatermark,
		MinFreeBytes:         minFreeBytes,
		IncomingBytes:        incomingBytes,
		Protected:            protectedCacheEntries(protectedEntry),
		Now:                  l.Clock.Now(),
		Remove: func(entry cacheevict.Entry, reason cacheevict.Reason) bool {
			return l.removeCacheEntry(entry, false)
		},
	})
	for i := 0; i < result.TTLEvictions; i++ {
		l.recordCacheEvictionTTL()
	}
	for i := 0; i < result.SizeEvictions; i++ {
		l.recordCacheEvictionSize()
	}
	for i := 0; i < result.RemoveErrors; i++ {
		l.recordCacheEvictionError()
	}
	return result
}

type PooledConnCloser interface {
	ClosePooledConns(pathPrefix string)
}

type PooledConnEvictionBarrier interface {
	BeginPooledConnEviction(pathPrefix string) func()
}

// removeCacheEntry skips unpublished rows unless the caller is deleting the
// knowledge base on purpose. entry.Path is a shard file, so the marker lives
// one level up.
func (l *KB) removeCacheEntry(entry cacheevict.Entry, force bool) bool {
	if !force && HasPendingSession(filepath.Join(l.CacheDir, entry.KBID)) {
		return false
	}
	lock := l.LockFor(entry.KBID)
	// Never wait while a mutation may itself be requesting cache eviction;
	// fixed lock stripes can otherwise self-deadlock or cross-deadlock. A busy
	// entry is skipped and reconsidered by the next sweep.
	if !lock.TryLock() {
		return false
	}
	defer lock.Unlock()

	formats := l.registeredFormatsSnapshot()
	var releases []func()
	for _, format := range formats {
		if barrier, ok := format.(PooledConnEvictionBarrier); ok {
			releases = append(releases, barrier.BeginPooledConnEviction(entry.Path))
			continue
		}
		if closer, ok := format.(PooledConnCloser); ok {
			closer.ClosePooledConns(entry.Path)
		}
	}
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()

	return removeCacheDir(entry.Path)
}

func removeCacheDir(path string) bool {
	return os.RemoveAll(path) == nil
}

func protectedCacheEntries(explicitProtect string) map[string]bool {
	protected := map[string]bool{}
	if explicitProtect != "" {
		protected[explicitProtect] = true
	}
	return protected
}

func (l *KB) recordCacheBytesCurrent(v int64) {
	l.mu.Lock()
	l.cacheBytesCurrent = v
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionTTL() {
	l.mu.Lock()
	l.cacheEvictionsTTLTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionSize() {
	l.mu.Lock()
	l.cacheEvictionsSizeTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionError() {
	l.mu.Lock()
	l.cacheEvictionErrorsTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheBudgetExceeded() {
	l.mu.Lock()
	l.cacheBudgetExceededTotal++
	l.mu.Unlock()
}

func (l *KB) SweepCache(ctx context.Context) error {
	return l.evictCacheIfNeeded(ctx, "")
}

type CacheEvictionMetricsSnapshot = cacheevict.MetricsSnapshot

func (l *KB) CacheEvictionMetricsSnapshot() CacheEvictionMetricsSnapshot {
	l.mu.Lock()
	snapshot := CacheEvictionMetricsSnapshot{
		CacheBytesCurrent:        l.cacheBytesCurrent,
		CacheEvictionsTTLTotal:   l.cacheEvictionsTTLTotal,
		CacheEvictionsSizeTotal:  l.cacheEvictionsSizeTotal,
		CacheEvictionErrorsTotal: l.cacheEvictionErrorsTotal,
		CacheBudgetExceededTotal: l.cacheBudgetExceededTotal,
	}
	cacheDir := l.CacheDir
	l.mu.Unlock()
	if usage, err := cacheevict.MeasureDiskUsage(cacheDir); err == nil {
		snapshot.DiskCapacityBytes = usage.CapacityBytes
		snapshot.DiskAvailableBytes = usage.AvailableBytes
	}
	return snapshot
}

func (l *KB) CacheEvictionOpenMetricsText() string {
	metrics := cacheevict.OpenMetricsText(l.CacheEvictionMetricsSnapshot())
	if shardMetrics := l.ShardingOpenMetricsText(); shardMetrics != "" {
		metrics += shardMetrics
	}
	return metrics
}

func NewCacheOpenMetricsHandler(kb *KB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if kb == nil {
			http.Error(w, "kb is nil", http.StatusServiceUnavailable)
			return
		}
		metrics := kb.CacheEvictionOpenMetricsText()
		if reporter, ok := kb.BlobStore.(interface {
			ReplicationOpenMetrics(context.Context) (string, error)
		}); ok {
			replicationMetrics, err := reporter.ReplicationOpenMetrics(r.Context())
			if err != nil {
				http.Error(w, "replication metrics unavailable", http.StatusServiceUnavailable)
				return
			}
			metrics += replicationMetrics
		}
		w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metrics))
	})
}
