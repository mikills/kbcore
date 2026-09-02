// Package budget owns every process-wide limit minnow shares between knowledge
// bases. One manager holds them together because they trade against each other:
// scattering them is what let the memory plan assume a count nothing enforced.
package budget

import (
	"runtime"
	"sync/atomic"

	"golang.org/x/sync/semaphore"

	"github.com/mikills/minnow/internal/memlimit"
)

const (
	// A cap, not a fixed count. The plan lowers it on a host too small to give
	// each reader an index build's worth.
	CachedReaders = 16

	DefaultBuildThreads = 4
	// Batches in flight for one upsert.
	DefaultEmbedParallelism = 4
	// Keeps one upsert from starving every other.
	MaxEmbedParallelism = 16
	EmbedBudget         = 64
)

// Manager holds the process's shared limits.
type Manager struct {
	plan memlimit.Plan
	// False when an operator pinned memory_limit. The governor still runs.
	sizes bool

	liveDatabases atomic.Int64
	issued        atomic.Int64

	buildThreads *semaphore.Weighted
	buildBudget  int

	embeds *semaphore.Weighted

	gov atomic.Pointer[governor]
}

// New builds a manager. A zero Plan means no ceiling was readable, so only the
// concurrency limits apply.
func New(plan memlimit.Plan, sizes bool) *Manager {
	threads := max(runtime.NumCPU(), usableThreads(), 1)
	return &Manager{
		plan:         plan,
		sizes:        sizes,
		buildThreads: semaphore.NewWeighted(int64(threads)),
		buildBudget:  threads,
		embeds:       semaphore.NewWeighted(EmbedBudget),
	}
}

var process atomic.Pointer[Manager]

func init() { process.Store(New(memlimit.Plan{}, false)) }

// Process is the shared manager, used when no other is wired in.
func Process() *Manager { return process.Load() }

// CachedDatabases is the count the plan divided by, so the cache and the
// budget cannot drift.
func (m *Manager) CachedDatabases() int {
	if m.plan.Databases > 0 {
		return m.plan.Databases
	}
	return CachedReaders
}

// SetProcess returns the manager it displaced, so a caller can put it back.
func SetProcess(m *Manager) *Manager { return process.Swap(m) }

// Plan is the memory division this manager was built from.
func (m *Manager) Plan() (memlimit.Plan, bool) { return m.plan, m.sizes }

// governs reports whether a ceiling was read, which is what the governor needs.
func (m *Manager) governs() bool { return m.plan.Ceiling > 0 }
