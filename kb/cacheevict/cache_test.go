package cacheevict

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	t.Run("sweep removes expired entries first", func(t *testing.T) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		entries := []Entry{
			{KBID: "old", Bytes: 80, LastTouch: now.Add(-2 * time.Hour)},
			{KBID: "new", Bytes: 80, LastTouch: now.Add(-time.Minute)},
		}
		removed := map[string]Reason{}
		result := Sweep(Config{
			MaxBytes:  100,
			TTL:       time.Hour,
			Protected: map[string]bool{},
			Now:       now,
			Remove: func(entry Entry, reason Reason) bool {
				removed[entry.KBID] = reason
				return true
			},
		})
		require.False(t, result.OverBudget)
		require.Empty(t, removed)

		result = sweepEntries(entries, Config{
			MaxBytes:  100,
			TTL:       time.Hour,
			Protected: map[string]bool{},
			Now:       now,
			Remove: func(entry Entry, reason Reason) bool {
				removed[entry.KBID] = reason
				return true
			},
		})
		require.Equal(t, ReasonTTL, removed["old"])
		require.Equal(t, int64(80), result.CurrentBytes)
		require.False(t, result.OverBudget)
	})

	t.Run("filesystem watermarks evict to the low watermark", func(t *testing.T) {
		root := t.TempDir()
		now := time.Now()
		for index, name := range []string{"old", "middle", "new"} {
			path := filepath.Join(root, name)
			require.NoError(t, os.MkdirAll(path, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(path, "blob"), make([]byte, 100), 0o600))
			touch := now.Add(time.Duration(index) * time.Minute)
			require.NoError(t, os.Chtimes(filepath.Join(path, "blob"), touch, touch))
		}
		var removed []string
		available := int64(100)
		result := Sweep(Config{
			Root:                 root,
			HighWatermarkPercent: 80,
			LowWatermarkPercent:  70,
			DiskUsage: func(string) (DiskUsage, error) {
				return DiskUsage{CapacityBytes: 1000, AvailableBytes: available}, nil
			},
			Remove: func(entry Entry, _ Reason) bool {
				removed = append(removed, entry.KBID)
				available += entry.Bytes
				return true
			},
		})
		require.Equal(t, []string{"old", "middle"}, removed)
		require.EqualValues(t, 100, result.CurrentBytes)
		require.EqualValues(t, 100, result.MaxBytes)
		require.False(t, result.OverBudget)
	})

	t.Run("incoming reservation is included before download", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "old")
		require.NoError(t, os.MkdirAll(path, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "blob"), make([]byte, 200), 0o600))
		available := int64(250)
		result := Sweep(Config{
			Root:                 root,
			IncomingBytes:        100,
			HighWatermarkPercent: 80,
			LowWatermarkPercent:  70,
			DiskUsage: func(string) (DiskUsage, error) {
				return DiskUsage{CapacityBytes: 1000, AvailableBytes: available}, nil
			},
			Remove: func(entry Entry, _ Reason) bool {
				available += entry.Bytes
				return true
			},
		})
		require.False(t, result.OverBudget)
		require.Equal(t, 1, result.SizeEvictions)
		require.GreaterOrEqual(t, available-100, int64(300))
	})

	t.Run("filesystem measurement failure rejects budget-sensitive writes", func(t *testing.T) {
		root := t.TempDir()
		result := Sweep(Config{
			Root:         root,
			MinFreeBytes: 1,
			DiskUsage: func(string) (DiskUsage, error) {
				return DiskUsage{}, errors.New("statfs unavailable")
			},
		})
		require.True(t, result.OverBudget)
		require.Equal(t, 1, result.RemoveErrors)
	})

	t.Run("unreachable free-space reserve remains over budget", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "only-cache-entry")
		require.NoError(t, os.MkdirAll(path, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "blob"), make([]byte, 100), 0o600))
		result := Sweep(Config{
			Root:         root,
			MinFreeBytes: 500,
			DiskUsage: func(string) (DiskUsage, error) {
				return DiskUsage{CapacityBytes: 1000, AvailableBytes: 100}, nil
			},
			Remove: func(Entry, Reason) bool { return true },
		})
		require.True(t, result.OverBudget, "evicting 100 bytes cannot satisfy a 400-byte deficit")
		require.Zero(t, result.CurrentBytes)
	})

	t.Run("filesystem hysteresis does not evict below the high watermark", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "kb")
		require.NoError(t, os.MkdirAll(path, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "blob"), make([]byte, 100), 0o600))
		removed := false
		result := Sweep(Config{
			Root:                 root,
			HighWatermarkPercent: 80,
			LowWatermarkPercent:  70,
			DiskUsage: func(string) (DiskUsage, error) {
				return DiskUsage{CapacityBytes: 1000, AvailableBytes: 250}, nil
			},
			Remove: func(Entry, Reason) bool { removed = true; return true },
		})
		require.False(t, removed)
		require.EqualValues(t, 100, result.CurrentBytes)
		require.False(t, result.OverBudget)
	})

	t.Run("retry stops after budget clears", func(t *testing.T) {
		calls := 0
		result, exceeded, err := RetryUntilWithinBudget(RetryConfig{
			Window: time.Second,
			Tick:   time.Millisecond,
			Now:    time.Now,
			Sleep:  func(time.Duration) error { return nil },
			Sweep: func() SweepResult {
				calls++
				return SweepResult{OverBudget: calls == 1, CurrentBytes: 10, MaxBytes: 5}
			},
		})
		require.NoError(t, err)
		require.False(t, exceeded)
		require.False(t, result.OverBudget)
		require.Equal(t, 2, calls)
	})

	t.Run("sweep evicts the oldest query shard without removing its sibling", func(t *testing.T) {
		root := t.TempDir()
		queryDir := filepath.Join(root, "kb-a", "query-shards")
		require.NoError(t, os.MkdirAll(queryDir, 0o755))
		oldPath := filepath.Join(queryDir, "old.duckdb")
		newPath := filepath.Join(queryDir, "new.duckdb")
		require.NoError(t, os.WriteFile(oldPath, make([]byte, 100), 0o600))
		require.NoError(t, os.WriteFile(newPath, make([]byte, 100), 0o600))
		now := time.Now().UTC()
		require.NoError(t, os.Chtimes(oldPath, now.Add(-time.Hour), now.Add(-time.Hour)))
		require.NoError(t, os.Chtimes(newPath, now, now))
		result := Sweep(Config{
			Root:      root,
			MaxBytes:  100,
			Protected: map[string]bool{newPath: true},
			Remove: func(entry Entry, _ Reason) bool {
				return os.Remove(entry.Path) == nil
			},
		})
		require.Equal(t, 1, result.SizeEvictions)
		require.NoFileExists(t, oldPath)
		require.FileExists(t, newPath)
	})

	t.Run("scan returns directory sizes", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "kb-a")
		require.NoError(t, os.MkdirAll(path, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "blob"), []byte("abc"), 0o644))
		entries, total := ScanEntries(root)
		require.Len(t, entries, 1)
		require.Equal(t, "kb-a", entries[0].KBID)
		require.Equal(t, int64(3), total)
	})

	t.Run("lease state is not a cache entry", func(t *testing.T) {
		root := t.TempDir()
		kbPath := filepath.Join(root, "kb-a")
		require.NoError(t, os.MkdirAll(kbPath, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(kbPath, "blob"), []byte("abc"), 0o644))
		leasePath := filepath.Join(root, ".leases")
		require.NoError(t, os.MkdirAll(leasePath, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(leasePath, "kb-a.lock"), []byte("held"), 0o600))

		// Charging leases to the cache would let a sweep delete the record of
		// who is mid-write.
		entries, total := ScanEntries(root)
		require.Len(t, entries, 1)
		require.Equal(t, "kb-a", entries[0].KBID)
		require.Equal(t, int64(3), total)
	})
}

func sweepEntries(entries []Entry, cfg Config) SweepResult {
	SortOldestFirst(entries)
	total := int64(0)
	for _, entry := range entries {
		total += entry.Bytes
	}
	candidates, result := removeExpired(cfg, entries, total)
	if cfg.MaxBytes > 0 {
		result.CurrentBytes = removeOverBudget(cfg, candidates, result.CurrentBytes, &result)
	}
	result.MaxBytes = cfg.MaxBytes
	result.ProtectedEntries = len(cfg.Protected)
	result.OverBudget = cfg.MaxBytes > 0 && result.CurrentBytes > cfg.MaxBytes
	return result
}
