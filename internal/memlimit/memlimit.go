// Package memlimit reports the memory ceiling this process must stay under and
// divides the share minnow plans for between DuckDB and the Go heap.
package memlimit

import (
	"errors"
	"fmt"
)

const (
	// The unplanned tenth absorbs the HNSW index, cgo arenas, and pages the
	// allocator has not returned.
	Headroom    = 0.90
	GoHeapShare = 0.30
	// Below this the GC spins against a heap it cannot shrink.
	minGoHeap = 256 << 20
	// 18,750 rows at 512 dimensions built at 56MB and failed below it.
	FloorDatabaseBytes = 64 << 20
	// Measured peak over raw vector bytes: 1.53 at the smallest shard, 1.28 at
	// the largest. Overshooting can abort the process, so this sits above both.
	buildOverhead = 1.6
)

// Shape is the largest shard an index build has to fit.
type Shape struct {
	Rows       int
	Dimensions int
}

// MinDatabaseBytes is the smallest memory_limit that can finish an index build
// over this shape. Measured against raw vector bytes, not row count.
func (s Shape) MinDatabaseBytes() int64 {
	raw := int64(s.Rows) * int64(s.Dimensions) * 4
	return max(int64(float64(raw)*buildOverhead), FloorDatabaseBytes)
}

// RowsWithin is the largest shard this many bytes could index, for telling an
// operator what would fit.
func (s Shape) RowsWithin(bytes int64) int {
	if s.Dimensions <= 0 || bytes <= 0 {
		return 0
	}
	return int(float64(bytes) / (buildOverhead * float64(s.Dimensions) * 4))
}

var (
	ErrNoCeiling = errors.New("no readable memory ceiling on this platform")
	ErrConfined  = errors.New("this process is in a cgroup whose limit could not be read")
	ErrTooSmall  = errors.New("memory ceiling is too small to budget from")
)

// Limit is a memory ceiling and where it was read from.
type Limit struct {
	Ceiling int64
	Source  string
	// Confined means a cgroup governs us but its limit was unreadable, so
	// Ceiling is the host's memory rather than the real one.
	Confined bool
	// Usage has to come from the same cgroup. A parent slice counts siblings,
	// and our own use against its limit would miss the pressure they cause.
	dir string
}

// Dir is empty when the ceiling came from physical memory.
func (l Limit) Dir() string { return l.dir }

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
	Dir     string
	Ceiling int64
	Budget  int64
	GoHeap  int64
	// GoHeapPreset means GoHeap came from an existing GOMEMLIMIT, so setting
	// it again is a no-op.
	GoHeapPreset bool
	DuckDBTotal  int64
	// memory_limit binds one database, so this is the total over Databases.
	DuckDBPerDB int64
	Databases   int
	MinPerDB    int64
	Source      string
}

// Divide splits the ceiling. presetGoHeap is an effective GOMEMLIMIT or 0.
// maxDBs is a cap, lowered to what the ceiling can give each instance.
func (l Limit) Divide(shape Shape, maxDBs int, presetGoHeap int64) (Plan, error) {
	if err := l.Usable(); err != nil {
		return Plan{}, err
	}
	if maxDBs < 1 {
		maxDBs = 1
	}
	budget := int64(float64(l.Ceiling) * Headroom)

	goHeap, preset := presetGoHeap, true
	if goHeap <= 0 {
		goHeap, preset = max(int64(float64(budget)*GoHeapShare), minGoHeap), false
	}
	duckTotal := budget - goHeap
	minPerDB := shape.MinDatabaseBytes()
	// The count gives way, not the share.
	dbs := int(duckTotal / minPerDB)
	if dbs < 1 {
		return Plan{}, fmt.Errorf(
			"%w: a %s ceiling leaves %s for DuckDB after a %s Go heap, and one database needs %s to index %d rows at %d dimensions; %s would fit sharding.max_vector_rows_per_shard of %d",
			ErrTooSmall, formatMiB(l.Ceiling), formatMiB(duckTotal), formatMiB(goHeap),
			formatMiB(minPerDB), shape.Rows, shape.Dimensions,
			formatMiB(duckTotal), shape.RowsWithin(duckTotal),
		)
	}
	dbs = min(dbs, maxDBs)
	return Plan{
		Dir:          l.dir,
		Ceiling:      l.Ceiling,
		Budget:       budget,
		GoHeap:       goHeap,
		GoHeapPreset: preset,
		DuckDBTotal:  duckTotal,
		DuckDBPerDB:  duckTotal / int64(dbs),
		Databases:    dbs,
		MinPerDB:     minPerDB,
		Source:       l.Source,
	}, nil
}

// MemoryLimit rounds down, so it never names more than the plan allows.
func (p Plan) MemoryLimit() string { return FormatMB(p.DuckDBPerDB) }

// FormatMB renders bytes the way memory_limit expects.
func FormatMB(bytes int64) string {
	mb := bytes >> 20
	if mb < 1 {
		mb = 1
	}
	return fmt.Sprintf("%dMB", mb)
}

func formatMiB(bytes int64) string { return fmt.Sprintf("%dMiB", bytes>>20) }
