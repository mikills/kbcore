package budget

import (
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/mikills/minnow/internal/memlimit"
)

// OpenDatabase returns the memory_limit a new database may use and the release
// to call when it closes.
//
// It never blocks. Opens nest, so waiting on a slot could wait on one the
// caller already holds. The share shrinks as more open instead.
func (m *Manager) OpenDatabase(configured string) (limit string, release func()) {
	m.liveDatabases.Add(1)
	if !m.sizes {
		m.issued.Add(0)
		return configured, m.releaser(0)
	}
	share := m.claimShare()
	return memlimit.FormatMB(share), m.releaser(share)
}

// Idempotent: a failed open and a failed configure both reach the same release.
func (m *Manager) releaser(share int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.liveDatabases.Add(-1)
			m.issued.Add(-share)
		})
	}
}

// databaseShare hands out the planned share, or what the total has left if that
// is less. Databases already open keep the limit they were born with, so only
// tracking the remainder holds the sum down.
//
// The index build floor is the one way past the total. Shrinking below it would
// trade one failure for a worse one.
// claimShare reserves a share against the total. Compare and swap because two
// opens reading the same remainder would both issue it.
func (m *Manager) claimShare() int64 {
	for {
		issued := m.issued.Load()
		share := m.databaseShare(issued)
		if m.issued.CompareAndSwap(issued, issued+share) {
			return share
		}
	}
}

func (m *Manager) databaseShare(issued int64) int64 {
	remaining := m.plan.DuckDBTotal - issued
	planned := m.plan.DuckDBTotal / int64(m.CachedDatabases())
	// Use has already outrun the plan, so the planned share is the wrong one.
	switch m.Pressure() {
	case PressureCritical:
		planned = m.minPerDB()
	case PressureHigh:
		planned /= 2
	}
	return max(min(planned, remaining), m.minPerDB())
}

// minPerDB falls back to the smallest measured build when no plan named a shape.
func (m *Manager) minPerDB() int64 {
	if m.plan.MinPerDB > 0 {
		return m.plan.MinPerDB
	}
	return memlimit.FloorDatabaseBytes
}

// IssuedBytes is every live database's memory_limit together.
func (m *Manager) IssuedBytes() int64 { return m.issued.Load() }

// LiveDatabases is how many DuckDB databases are open right now.
func (m *Manager) LiveDatabases() int64 { return m.liveDatabases.Load() }

// SetEmbedBudgetForTest drives contention without running 64 requests.
func (m *Manager) SetEmbedBudgetForTest(n int) {
	m.embeds = semaphore.NewWeighted(int64(n))
}
