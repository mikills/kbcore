// Package budget owns every process-wide limit minnow shares between knowledge
// bases: memory, open DuckDB databases, index build threads, and in-flight
// embedding requests. One manager holds them together because they trade
// against each other. Splitting them across packages is what let the memory
// plan assume a database count nothing enforced.
package budget

import (
	"runtime"
	"sync/atomic"

	"golang.org/x/sync/semaphore"

	"github.com/mikills/minnow/internal/memlimit"
)

const (
	// CachedReaders is how many shard readers the connection pool holds open.
	// This is the steady-state population, and what the memory plan divides by.
	CachedReaders = 16
	// PlannedDatabases is the divisor for the memory plan. Ingest, sealing, and
	// compaction open their own databases on top of the cache; reserving for
	// them here would starve small hosts, so OpenDatabase shrinks every share
	// once they appear instead.
	PlannedDatabases = CachedReaders

	// DefaultBuildThreads caps one index build. Past four the curve flattens.
	DefaultBuildThreads = 4
	// DefaultEmbedParallelism is batches in flight for one upsert.
	DefaultEmbedParallelism = 4
	// MaxEmbedParallelism keeps one upsert below the process budget so it
	// cannot starve every other.
	MaxEmbedParallelism = 16
	// EmbedBudget is the process-wide ceiling on concurrent embed requests.
	EmbedBudget = 64
)

// Manager holds the process's shared limits.
type Manager struct {
	plan memlimit.Plan
	// sizes is false when an operator pinned memory_limit themselves. The
	// governor still runs; it just does not rewrite their number.
	sizes bool

	liveDatabases atomic.Int64
	issued        atomic.Int64

	buildThreads *semaphore.Weighted
	buildBudget  int

	embeds *semaphore.Weighted

	gov atomic.Pointer[governor]
}

// New builds a manager. sizes says whether it may choose memory_limit; a zero
// Plan means no ceiling was readable and only the concurrency limits apply.
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

// process is read on every database open and every embed batch, and replaced
// once at startup, so it is held atomically rather than as a plain global.
var process atomic.Pointer[Manager]

func init() { process.Store(New(memlimit.Plan{}, false)) }

// Process is the manager every knowledge base shares when no other is wired in.
func Process() *Manager { return process.Load() }

// SetProcess replaces the shared manager and returns the one it displaced, so a
// caller that installs a manager can put the previous one back.
func SetProcess(m *Manager) *Manager { return process.Swap(m) }

// Plan is the memory division this manager was built from.
func (m *Manager) Plan() (memlimit.Plan, bool) { return m.plan, m.sizes }

// governs reports whether a ceiling was read, which is what the governor needs.
func (m *Manager) governs() bool { return m.plan.Ceiling > 0 }
