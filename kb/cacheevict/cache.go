package cacheevict

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Reason string

const (
	ReasonTTL  Reason = "ttl"
	ReasonSize Reason = "size"
)

type Entry struct {
	KBID      string
	Path      string
	Bytes     int64
	LastTouch time.Time
}

type Config struct {
	Root      string
	MaxBytes  int64
	TTL       time.Duration
	Protected map[string]bool
	Now       time.Time
	Remove    func(Entry, Reason) bool
}

type MetricsSnapshot struct {
	CacheBytesCurrent        int64
	CacheEvictionsTTLTotal   uint64
	CacheEvictionsSizeTotal  uint64
	CacheEvictionErrorsTotal uint64
	CacheBudgetExceededTotal uint64
}

func OpenMetricsText(m MetricsSnapshot) string {
	lines := []string{
		"# TYPE minnow_cache_evictions_total counter",
		fmt.Sprintf("minnow_cache_evictions_total{reason=\"ttl\"} %d", m.CacheEvictionsTTLTotal),
		fmt.Sprintf("minnow_cache_evictions_total{reason=\"size\"} %d", m.CacheEvictionsSizeTotal),
		"# TYPE minnow_cache_eviction_errors_total counter",
		fmt.Sprintf("minnow_cache_eviction_errors_total %d", m.CacheEvictionErrorsTotal),
		"# TYPE minnow_cache_budget_exceeded_total counter",
		fmt.Sprintf("minnow_cache_budget_exceeded_total %d", m.CacheBudgetExceededTotal),
		"# TYPE minnow_cache_bytes_current gauge",
		fmt.Sprintf("minnow_cache_bytes_current %d", m.CacheBytesCurrent),
	}
	return strings.Join(lines, "\n") + "\n"
}

type RetryConfig struct {
	Window time.Duration
	Tick   time.Duration
	Now    func() time.Time
	Sleep  func(time.Duration) error
	Sweep  func() SweepResult
}

func RetryUntilWithinBudget(cfg RetryConfig) (SweepResult, bool, error) {
	deadline := cfg.Now().Add(cfg.Window)
	for {
		result := cfg.Sweep()
		if !result.OverBudget {
			return result, false, nil
		}
		if !cfg.Now().Before(deadline) {
			return result, true, nil
		}
		wait := cfg.Tick
		if remaining := deadline.Sub(cfg.Now()); remaining < wait {
			wait = remaining
		}
		if err := cfg.Sleep(wait); err != nil {
			return result, false, err
		}
	}
}

type SweepResult struct {
	CurrentBytes     int64
	MaxBytes         int64
	ProtectedKBCount int
	OverBudget       bool
	TTLEvictions     int
	SizeEvictions    int
	RemoveErrors     int
}

func Sweep(cfg Config) SweepResult {
	if cfg.MaxBytes <= 0 && cfg.TTL <= 0 {
		return SweepResult{MaxBytes: cfg.MaxBytes}
	}
	entries, total := ScanEntries(cfg.Root)
	if len(entries) == 0 {
		return SweepResult{MaxBytes: cfg.MaxBytes, CurrentBytes: total}
	}
	if cfg.TTL <= 0 && (cfg.MaxBytes <= 0 || total <= cfg.MaxBytes) {
		return SweepResult{MaxBytes: cfg.MaxBytes, CurrentBytes: total}
	}
	SortOldestFirst(entries)
	candidates, result := removeExpired(cfg, entries, total)
	if cfg.MaxBytes > 0 {
		result.CurrentBytes = removeOverBudget(cfg, candidates, result.CurrentBytes, &result)
	}
	result.MaxBytes = cfg.MaxBytes
	result.ProtectedKBCount = len(cfg.Protected)
	result.OverBudget = cfg.MaxBytes > 0 && result.CurrentBytes > cfg.MaxBytes
	return result
}

func removeExpired(cfg Config, entries []Entry, total int64) ([]Entry, SweepResult) {
	result := SweepResult{CurrentBytes: total}
	if cfg.TTL <= 0 {
		return entries, result
	}
	now := cfg.Now
	if now.IsZero() {
		now = time.Now()
	}
	remaining := entries[:0]
	for _, entry := range entries {
		if cfg.Protected[entry.KBID] || entry.LastTouch.IsZero() || now.Sub(entry.LastTouch) < cfg.TTL {
			remaining = append(remaining, entry)
			continue
		}
		if removeEntry(cfg, entry, ReasonTTL, &result) {
			result.CurrentBytes -= entry.Bytes
			result.TTLEvictions++
			continue
		}
		remaining = append(remaining, entry)
	}
	return remaining, result
}

func removeOverBudget(cfg Config, entries []Entry, total int64, result *SweepResult) int64 {
	for _, entry := range entries {
		if total <= cfg.MaxBytes {
			break
		}
		if cfg.Protected[entry.KBID] {
			continue
		}
		if removeEntry(cfg, entry, ReasonSize, result) {
			total -= entry.Bytes
			result.SizeEvictions++
		}
	}
	return total
}

func removeEntry(cfg Config, entry Entry, reason Reason, result *SweepResult) bool {
	if cfg.Remove == nil {
		result.RemoveErrors++
		return false
	}
	if !cfg.Remove(entry, reason) {
		result.RemoveErrors++
		return false
	}
	return true
}

func ScanEntries(root string) ([]Entry, int64) {
	items, err := os.ReadDir(root)
	if err != nil {
		return nil, 0
	}
	entries := make([]Entry, 0, len(items))
	var total int64
	for _, item := range items {
		if !item.IsDir() {
			continue
		}
		path := filepath.Join(root, item.Name())
		size, touch, ok := DirStats(path)
		if !ok {
			continue
		}
		entries = append(entries, Entry{KBID: item.Name(), Path: path, Bytes: size, LastTouch: touch})
		total += size
	}
	return entries, total
}

func DirStats(root string) (int64, time.Time, bool) {
	var total int64
	var latest time.Time
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if path == root {
				return err
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			total += info.Size()
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
		}
		return nil
	})
	if err != nil {
		return 0, time.Time{}, false
	}
	if latest.IsZero() {
		latest = time.Now()
	}
	return total, latest, true
}

func SortOldestFirst(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].LastTouch.Equal(entries[j].LastTouch) {
			return entries[i].KBID < entries[j].KBID
		}
		return entries[i].LastTouch.Before(entries[j].LastTouch)
	})
}
