package budget

import (
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/mikills/minnow/internal/memlimit"
)

// OpenDatabase records that a DuckDB database is about to open and returns the
// memory_limit it may use, plus the release to call when it closes.
//
// It never blocks. Opens nest, so a database waiting on a slot could wait on
// one its own caller holds. Instead the share shrinks as more open at once,
// which bounds the total without a lock ordering to get wrong.
func (m *Manager) OpenDatabase(configured string) (limit string, release func()) {
	m.liveDatabases.Add(1)
	if !m.sizes {
		m.issued.Add(0)
		return configured, m.releaser(0)
	}
	share := m.databaseShare()
	m.issued.Add(share)
	return memlimit.FormatMB(share), m.releaser(share)
}

// releaser is idempotent. A failed open releases by hand, and a configure
// failure closes the database first, so both paths can reach the same release.
func (m *Manager) releaser(share int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.liveDatabases.Add(-1)
			m.issued.Add(-share)
		})
	}
}

// databaseShare hands out the planned share, or whatever the total has left if
// that is less. Tracking what is left is what holds the sum: shrinking only the
// new opens would leave every database already running with the larger limit it
// was born with, so the sum would climb with every open.
//
// The floor is the smallest buffer manager that can still finish an index
// build, and is the one way past the total: once it applies, every further
// database adds 64MiB the plan did not account for. Shrinking below it would
// trade one failure for a worse one.
func (m *Manager) databaseShare() int64 {
	remaining := m.plan.DuckDBTotal - m.issued.Load()
	planned := m.plan.DuckDBTotal / int64(PlannedDatabases)
	// Under pressure the measured use has already outrun the plan, so the
	// planned share is the wrong number to keep handing out.
	switch m.Pressure() {
	case PressureCritical:
		planned = memlimit.MinDuckDBPerDB
	case PressureHigh:
		planned /= 2
	}
	return max(min(planned, remaining), memlimit.MinDuckDBPerDB)
}

// IssuedBytes is the memory_limit handed to every live database together.
func (m *Manager) IssuedBytes() int64 { return m.issued.Load() }

// LiveDatabases is how many DuckDB databases are open right now.
func (m *Manager) LiveDatabases() int64 { return m.liveDatabases.Load() }

// SetEmbedBudgetForTest replaces the process-wide embedding ceiling. Tests use
// it to drive contention without running 64 requests.
func (m *Manager) SetEmbedBudgetForTest(n int) {
	m.embeds = semaphore.NewWeighted(int64(n))
}
