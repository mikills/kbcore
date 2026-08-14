package cacheevict

import (
	"fmt"
	"math"
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
	Root                 string
	MaxBytes             int64
	TTL                  time.Duration
	HighWatermarkPercent int
	LowWatermarkPercent  int
	MinFreeBytes         int64
	IncomingBytes        int64
	DiskUsage            func(string) (DiskUsage, error)
	// Protected accepts either a KB ID (protect all its entries) or an exact
	// entry path (protect one shard).
	Protected map[string]bool
	Now       time.Time
	Remove    func(Entry, Reason) bool
}

type DiskUsage struct {
	CapacityBytes  int64
	AvailableBytes int64
}

type MetricsSnapshot struct {
	CacheBytesCurrent        int64
	CacheEvictionsTTLTotal   uint64
	CacheEvictionsSizeTotal  uint64
	CacheEvictionErrorsTotal uint64
	CacheBudgetExceededTotal uint64
	DiskCapacityBytes        int64
	DiskAvailableBytes       int64
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
		"# TYPE minnow_cache_disk_capacity_bytes gauge",
		fmt.Sprintf("minnow_cache_disk_capacity_bytes %d", m.DiskCapacityBytes),
		"# TYPE minnow_cache_disk_available_bytes gauge",
		fmt.Sprintf("minnow_cache_disk_available_bytes %d", m.DiskAvailableBytes),
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
	ProtectedEntries int
	OverBudget       bool
	TTLEvictions     int
	SizeEvictions    int
	RemoveErrors     int
}

func Sweep(cfg Config) SweepResult {
	watermarksEnabled := cfg.HighWatermarkPercent > 0 || cfg.MinFreeBytes > 0
	if cfg.MaxBytes <= 0 && cfg.TTL <= 0 && !watermarksEnabled {
		return SweepResult{MaxBytes: cfg.MaxBytes}
	}
	entries, total := ScanEntries(cfg.Root)
	effectiveMax, limitActive, watermarkBytesToFree, targetAvailable, usageErr := effectiveMaxBytes(cfg, total)
	incomingOverMax := cfg.MaxBytes > 0 && cfg.IncomingBytes > cfg.MaxBytes
	if usageErr != nil {
		return SweepResult{MaxBytes: effectiveMax, CurrentBytes: total, OverBudget: true, RemoveErrors: 1}
	}
	if len(entries) == 0 {
		result := SweepResult{MaxBytes: effectiveMax, CurrentBytes: total}
		result.OverBudget = incomingOverMax || limitActive && (total > effectiveMax || watermarkBytesToFree > 0)
		return verifyFilesystemTarget(cfg, result, targetAvailable)
	}
	if cfg.TTL <= 0 && (!limitActive || total <= effectiveMax) {
		result := SweepResult{MaxBytes: effectiveMax, CurrentBytes: total}
		result.OverBudget = incomingOverMax || limitActive && watermarkBytesToFree > 0
		return verifyFilesystemTarget(cfg, result, targetAvailable)
	}
	SortOldestFirst(entries)
	candidates, result := removeExpired(cfg, entries, total)
	if limitActive {
		sizeCfg := cfg
		sizeCfg.MaxBytes = effectiveMax
		result.CurrentBytes = removeOverBudget(sizeCfg, candidates, result.CurrentBytes, &result)
	}
	result.MaxBytes = effectiveMax
	result.ProtectedEntries = len(cfg.Protected)
	freed := total - result.CurrentBytes
	result.OverBudget = incomingOverMax || limitActive && (result.CurrentBytes > effectiveMax || freed < watermarkBytesToFree)
	return verifyFilesystemTarget(cfg, result, targetAvailable)
}

func verifyFilesystemTarget(cfg Config, result SweepResult, targetAvailable int64) SweepResult {
	if result.OverBudget || targetAvailable <= 0 {
		return result
	}
	measure := cfg.DiskUsage
	if measure == nil {
		measure = MeasureDiskUsage
	}
	usage, err := measure(cfg.Root)
	if err != nil {
		result.RemoveErrors++
		result.OverBudget = true
		return result
	}
	projectedAvailable := usage.AvailableBytes - cfg.IncomingBytes
	if projectedAvailable < 0 {
		projectedAvailable = 0
	}
	result.OverBudget = projectedAvailable < targetAvailable
	return result
}

func effectiveMaxBytes(cfg Config, cacheBytes int64) (int64, bool, int64, int64, error) {
	effective := cfg.MaxBytes
	if effective > 0 {
		effective -= cfg.IncomingBytes
		if effective < 0 {
			effective = 0
		}
	}
	limitActive := effective > 0 || cfg.MaxBytes > 0
	if cfg.HighWatermarkPercent <= 0 && cfg.MinFreeBytes <= 0 {
		return effective, limitActive, 0, 0, nil
	}
	measure := cfg.DiskUsage
	if measure == nil {
		measure = MeasureDiskUsage
	}
	usage, err := measure(cfg.Root)
	if err != nil {
		return effective, limitActive, 0, 0, err
	}
	if usage.CapacityBytes <= 0 || usage.AvailableBytes < 0 || usage.AvailableBytes > usage.CapacityBytes {
		return effective, limitActive, 0, 0, fmt.Errorf("invalid filesystem capacity for %s", cfg.Root)
	}
	projectedAvailable := usage.AvailableBytes - cfg.IncomingBytes
	if projectedAvailable < 0 {
		projectedAvailable = 0
	}
	used := usage.CapacityBytes - projectedAvailable
	triggered := cfg.MinFreeBytes > 0 && projectedAvailable < cfg.MinFreeBytes
	if cfg.HighWatermarkPercent > 0 && used >= percentBytes(usage.CapacityBytes, cfg.HighWatermarkPercent) {
		triggered = true
	}
	if !triggered {
		return effective, limitActive, 0, 0, nil
	}
	targetAvailable := cfg.MinFreeBytes
	if cfg.LowWatermarkPercent > 0 {
		lowAvailable := percentBytes(usage.CapacityBytes, 100-cfg.LowWatermarkPercent)
		if lowAvailable > targetAvailable {
			targetAvailable = lowAvailable
		}
	}
	bytesToFree := targetAvailable - projectedAvailable
	if bytesToFree < 0 {
		bytesToFree = 0
	}
	watermarkMax := cacheBytes - bytesToFree
	if watermarkMax < 0 {
		watermarkMax = 0
	}
	if !limitActive || watermarkMax < effective {
		effective = watermarkMax
	}
	return effective, true, bytesToFree, targetAvailable, nil
}

func percentBytes(total int64, percent int) int64 {
	if percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return total
	}
	return (total/100)*int64(percent) + (total%100)*int64(percent)/100
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
		if entryProtected(cfg, entry) || entry.LastTouch.IsZero() || now.Sub(entry.LastTouch) < cfg.TTL {
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
		if entryProtected(cfg, entry) {
			continue
		}
		if removeEntry(cfg, entry, ReasonSize, result) {
			total -= entry.Bytes
			result.SizeEvictions++
		}
	}
	return total
}

func entryProtected(cfg Config, entry Entry) bool {
	return cfg.Protected[entry.KBID] || cfg.Protected[entry.Path]
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
		kbPath := filepath.Join(root, item.Name())
		size, touch, ok := DirStats(kbPath)
		if !ok {
			continue
		}
		if total > math.MaxInt64-size {
			total = math.MaxInt64
		} else {
			total += size
		}
		shards, shardLayout, downloadInFlight := scanQueryShards(item.Name(), filepath.Join(kbPath, "query-shards"))
		if shardLayout && (len(shards) > 0 || downloadInFlight) {
			entries = append(entries, shards...)
			continue
		}
		// Legacy cache layouts remain evictable as one KB-sized entry. Current
		// DuckDB query caches use per-shard entries above.
		entries = append(entries, Entry{KBID: item.Name(), Path: kbPath, Bytes: size, LastTouch: touch})
	}
	return entries, total
}

func scanQueryShards(kbID, root string) ([]Entry, bool, bool) {
	items, err := os.ReadDir(root)
	if err != nil {
		return nil, false, false
	}
	entries := make([]Entry, 0, len(items))
	downloadInFlight := false
	for _, item := range items {
		if strings.Contains(item.Name(), ".download-") {
			downloadInFlight = true
			continue
		}
		if item.IsDir() || filepath.Ext(item.Name()) != ".duckdb" {
			continue
		}
		info, err := item.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		entries = append(entries, Entry{
			KBID:      kbID,
			Path:      filepath.Join(root, item.Name()),
			Bytes:     info.Size(),
			LastTouch: info.ModTime(),
		})
	}
	return entries, true, downloadInFlight
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
			if info.Size() < 0 || total > math.MaxInt64-info.Size() {
				total = math.MaxInt64
			} else {
				total += info.Size()
			}
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
			return entries[i].Path < entries[j].Path
		}
		return entries[i].LastTouch.Before(entries[j].LastTouch)
	})
}
