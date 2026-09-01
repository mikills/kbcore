// Package memlimit reports the memory ceiling this process must stay under and
// divides the share minnow plans for between DuckDB and the Go heap.
package memlimit

import (
	"errors"
	"fmt"
)

const (
	// Headroom is the share of the ceiling minnow plans to use. The rest
	// absorbs what no budget can see: the HNSW index the VSS extension builds
	// outside DuckDB's buffer manager, cgo arenas, and pages the allocator has
	// not yet returned to the OS.
	Headroom = 0.90
	// GoHeapShare splits the budget when nothing has set GOMEMLIMIT already.
	GoHeapShare = 0.30
	// minGoHeap keeps the Go limit above the point where the GC would spin
	// against a heap it cannot shrink, since it does not govern DuckDB's cgo.
	minGoHeap = 256 << 20
	// MinDuckDBPerDB is the smallest buffer manager that can still finish an
	// index build. Below it DuckDB starts, then fails at the first real query.
	MinDuckDBPerDB = 64 << 20
)

var (
	ErrNoCeiling = errors.New("no readable memory ceiling on this platform")
	ErrConfined  = errors.New("this process is in a cgroup whose limit could not be read")
	ErrTooSmall  = errors.New("memory ceiling is too small to budget from")
)

// Limit is a memory ceiling and where it was read from.
type Limit struct {
	Ceiling int64
	Source  string
	// Confined records that a cgroup governs this process but its limit could
	// not be read, so Ceiling is the host's memory and not the real one.
	Confined bool
	// dir is the cgroup that supplied Ceiling. Usage has to be read from the
	// same place: a parent slice counts our siblings too, and comparing our own
	// use against its limit would miss the pressure they cause.
	dir string
}

// Dir is the cgroup that supplied the ceiling, empty when it came from
// physical memory.
func (l Limit) Dir() string { return l.dir }

// Detect reads the cgroup limit if a cgroup confines this process, and the
// machine's physical memory otherwise.
func Detect() Limit { return detectCeiling() }

// Usable explains why a ceiling cannot be budgeted from, or returns nil.
func (l Limit) Usable() error {
	switch {
	case l.Confined:
		return ErrConfined
	case l.Ceiling <= 0:
		return ErrNoCeiling
	}
	return nil
}

// Plan is how a ceiling is divided. Every field is bytes.
type Plan struct {
	// Dir is the cgroup the ceiling came from, for reading usage back.
	Dir     string
	Ceiling int64
	Budget  int64
	GoHeap  int64
	// GoHeapPreset means GoHeap came from an existing GOMEMLIMIT rather than
	// from the split, so applying it again would be a no-op.
	GoHeapPreset bool
	// DuckDBTotal is what every DuckDB instance may use between them.
	DuckDBTotal int64
	// DuckDBPerDB is what one instance may use. DuckDB's memory_limit binds a
	// single database and minnow keeps one open per cached shard, so the
	// setting is the total divided by how many are held at once.
	DuckDBPerDB int64
	Source      string
}

// Divide splits the ceiling. presetGoHeap is an already-effective GOMEMLIMIT in
// bytes, or 0; concurrentDBs is how many DuckDB instances may be open at once.
func (l Limit) Divide(concurrentDBs int, presetGoHeap int64) (Plan, error) {
	if err := l.Usable(); err != nil {
		return Plan{}, err
	}
	if concurrentDBs < 1 {
		concurrentDBs = 1
	}
	budget := int64(float64(l.Ceiling) * Headroom)

	goHeap, preset := presetGoHeap, true
	if goHeap <= 0 {
		goHeap, preset = max(int64(float64(budget)*GoHeapShare), minGoHeap), false
	}
	// No floor on the result: a share too small to build an index is a config
	// to reject, not one to round up until it looks valid.
	duckTotal := budget - goHeap
	perDB := duckTotal / int64(concurrentDBs)
	if perDB < MinDuckDBPerDB {
		return Plan{}, fmt.Errorf(
			"%w: a %s ceiling leaves %s for each of %d databases after a %s Go heap, and %s is the minimum",
			ErrTooSmall, formatMiB(l.Ceiling), formatMiB(perDB), concurrentDBs,
			formatMiB(goHeap), formatMiB(MinDuckDBPerDB),
		)
	}
	return Plan{
		Dir:          l.dir,
		Ceiling:      l.Ceiling,
		Budget:       budget,
		GoHeap:       goHeap,
		GoHeapPreset: preset,
		DuckDBTotal:  duckTotal,
		DuckDBPerDB:  perDB,
		Source:       l.Source,
	}, nil
}

// MemoryLimit renders DuckDB's memory_limit, rounded down so the string never
// names more than the plan allows.
func (p Plan) MemoryLimit() string { return FormatMB(p.DuckDBPerDB) }

// FormatMB renders bytes the way DuckDB's memory_limit expects.
func FormatMB(bytes int64) string {
	mb := bytes >> 20
	if mb < 1 {
		mb = 1
	}
	return fmt.Sprintf("%dMB", mb)
}

func formatMiB(bytes int64) string { return fmt.Sprintf("%dMiB", bytes>>20) }
