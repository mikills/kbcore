package budget

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mikills/minnow/internal/memlimit"
)

// Pressure is how close the process is to its ceiling.
type Pressure int32

const (
	// PressureNone means use is below SoftMark: budgets apply as planned.
	PressureNone Pressure = iota
	// PressureHigh means use is past SoftMark. New work takes smaller shares
	// so the process stops climbing before it reaches the ceiling.
	PressureHigh
	// PressureCritical means use is past HardMark, which is the share the plan
	// plays for. Nothing new is admitted until use falls back.
	PressureCritical
)

func (p Pressure) String() string {
	switch p {
	case PressureHigh:
		return "high"
	case PressureCritical:
		return "critical"
	default:
		return "none"
	}
}

const (
	// SoftMark is where back-pressure starts, leaving room to react before the
	// plan's own share of the ceiling is spent.
	SoftMark = 0.75
	// HardMark is the share of the ceiling the plan plays for. Past it the
	// remaining tenth is all that stands between minnow and the OOM killer, so
	// it stops taking on work rather than spending it.
	HardMark = memlimit.Headroom
	// releaseMargin is how far use has to fall below a mark before the level
	// drops. Without it a signal sitting on a mark flaps every wake-up, and two
	// builds starting a second apart get different thread counts for no reason.
	releaseMargin = 0.05
	// recoveryInterval is how often use is re-read while under pressure, to
	// find out when it has passed. The kernel only reports crossing the mark
	// upward, so coming back down is the one thing worth sampling for, and
	// only while there is something to come back from.
	recoveryInterval = 2 * time.Second
	// minWakeInterval paces the loop while use sits over the mark. Every
	// allocation past memory.high bumps the counter, so the kernel wakes us
	// continuously and correctly; without a floor between wake-ups that is a
	// spin. Costs nothing while healthy, because then there are no wake-ups.
	minWakeInterval = 250 * time.Millisecond
)

// governor turns real memory use into back-pressure. The plan divides a budget,
// but nothing in it can see the HNSW index the VSS extension builds outside
// DuckDB's buffer manager, or any other cgo. Only the kernel counts those.
//
// It does not poll. Setting memory.high makes the kernel reclaim and throttle
// at the mark itself, and writes to memory.events wake the watcher the moment
// the mark is crossed, so a healthy process costs one blocked goroutine and no
// cycles at all. Use is re-read only while pressure is on, to notice it lift.
type governor struct {
	pressure atomic.Int32
	usage    atomic.Int64
	wakes    atomic.Int64
	stop     context.CancelFunc
	done     chan struct{}
	// read and watch are seams for tests, which can neither allocate to order
	// nor make the kernel report pressure.
	read  func() memlimit.Usage
	watch func(string, int64) (memlimit.Notifier, error)
	// enforced records that the kernel is throttling at the mark, not just
	// telling us about it.
	enforced atomic.Bool
	source   atomic.Value
	// onChange reports every transition. Silent degradation is the hardest
	// kind to debug: throughput collapses and nothing says why.
	onChange func(from, to Pressure, usage memlimit.Usage)
	dir      string
	// armed closes once the watcher is up or has failed, so a caller can log
	// which mechanism is in force instead of guessing.
	armed  chan struct{}
	armErr atomic.Value
}

// pressedGCPercent collects far more often than the default 100, trading CPU
// for a smaller live heap while the process is near its ceiling.
const pressedGCPercent = 25

// Pressure is the last reading's verdict.
func (m *Manager) Pressure() Pressure {
	g := m.gov.Load()
	if g == nil {
		return PressureNone
	}
	return Pressure(g.pressure.Load())
}

// Usage is the last measured memory use in bytes, or 0 when unmeasured.
func (m *Manager) Usage() int64 {
	g := m.gov.Load()
	if g == nil {
		return 0
	}
	return g.usage.Load()
}

// MemoryEnforced reports whether the kernel is throttling at the mark rather
// than minnow only reacting after the fact.
func (m *Manager) MemoryEnforced() bool {
	g := m.gov.Load()
	return g != nil && g.enforced.Load()
}

// StartGovernor arms kernel notification. It is a no-op without a memory plan,
// or where the platform cannot report use, so callers need check neither.
func (m *Manager) StartGovernor(ctx context.Context, onChange func(from, to Pressure, usage memlimit.Usage)) {
	dir := m.plan.Dir
	m.startGovernor(ctx, func() memlimit.Usage { return memlimit.Current(dir) }, memlimit.Watch, onChange)
}

func (m *Manager) startGovernor(
	ctx context.Context,
	read func() memlimit.Usage,
	watch func(string, int64) (memlimit.Notifier, error),
	onChange func(from, to Pressure, usage memlimit.Usage),
) {
	if !m.governs() || !read().Ok {
		return
	}
	if onChange == nil {
		onChange = func(Pressure, Pressure, memlimit.Usage) {}
	}
	runCtx, cancel := context.WithCancel(ctx)
	g := &governor{
		stop:     cancel,
		done:     make(chan struct{}),
		read:     read,
		watch:    watch,
		onChange: onChange,
		dir:      m.plan.Dir,
		armed:    make(chan struct{}),
	}
	if !m.gov.CompareAndSwap(nil, g) {
		cancel()
		return
	}
	go g.run(runCtx, m.plan.Ceiling)
}

// StopGovernor ends notification and waits for the watcher to finish.
func (m *Manager) StopGovernor() {
	g := m.gov.Swap(nil)
	if g == nil {
		return
	}
	g.stop()
	<-g.done
	debug.SetGCPercent(baseGCPercent())
}

// baseGCPercent is GOGC as the process started, read once. Reading it means
// briefly disabling the collector, so doing it per governor would race two
// managers into leaving it off for good.
var baseGCPercent = sync.OnceValue(func() int {
	previous := debug.SetGCPercent(-1)
	debug.SetGCPercent(previous)
	return previous
})

// Armed blocks until the watcher is up, and reports why if it is not.
func (m *Manager) Armed() (source string, enforced bool, err error) {
	g := m.gov.Load()
	if g == nil {
		return "", false, errors.New("no memory ceiling to govern against")
	}
	<-g.armed
	if reason, ok := g.armErr.Load().(string); ok {
		return "", false, errors.New(reason)
	}
	source, _ = g.source.Load().(string)
	return source, g.enforced.Load(), nil
}

func (g *governor) run(ctx context.Context, ceiling int64) {
	defer close(g.done)

	watcher, err := g.watch(g.dir, ceiling)
	if err != nil {
		// Without notification there is nothing to react to. Polling for a
		// spike that lasts milliseconds would cost more than it caught.
		g.armErr.Store(err.Error())
		close(g.armed)
		return
	}
	// Teardown belongs to this goroutine alone. Interrupt only wakes Wait, so
	// the descriptors cannot go away while it is still blocked on them.
	defer func() { _ = watcher.Close() }()
	g.enforced.Store(watcher.Enforced())
	g.source.Store(watcher.Source())
	close(g.armed)

	go func() {
		<-ctx.Done()
		watcher.Interrupt()
	}()

	handled := time.Now()
	for {
		// Blocked with no timeout while healthy; once pressure is on, wake to
		// check whether it has lifted.
		timeout := -1
		if Pressure(g.pressure.Load()) != PressureNone {
			timeout = int(recoveryInterval / time.Millisecond)
		}
		if err := watcher.Wait(timeout); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		g.wakes.Add(1)
		g.sample(ceiling)

		if since := time.Since(handled); since < minWakeInterval {
			select {
			case <-ctx.Done():
				return
			case <-time.After(minWakeInterval - since):
			}
		}
		handled = time.Now()
	}
}

func (g *governor) sample(ceiling int64) {
	usage := g.read()
	if !usage.Ok || ceiling <= 0 {
		return
	}
	g.usage.Store(usage.Bytes)

	previous := Pressure(g.pressure.Load())
	next := pressureFor(usage.Bytes, ceiling, previous)
	if next == previous {
		return
	}
	g.pressure.Store(int32(next))
	g.onChange(previous, next, usage)

	// GC percent, not FreeOSMemory: the latter is a stop-the-world scavenge on
	// this goroutine at the worst possible moment, and it can only return Go
	// pages when the memory that raised the alarm is DuckDB's. Collecting more
	// often is continuous and cheap, and GOMEMLIMIT already bounds the heap.
	if next == PressureCritical {
		debug.SetGCPercent(pressedGCPercent)
	} else if previous == PressureCritical {
		debug.SetGCPercent(baseGCPercent())
	}
}

// pressureFor needs the level already in force, because the mark to leave a
// level sits below the mark to enter it.
func pressureFor(usage, ceiling int64, current Pressure) Pressure {
	used := float64(usage) / float64(ceiling)
	enterHard, enterSoft := HardMark, SoftMark
	leaveHard, leaveSoft := HardMark-releaseMargin, SoftMark-releaseMargin
	switch {
	case used >= enterHard:
		return PressureCritical
	case current == PressureCritical && used >= leaveHard:
		return PressureCritical
	case used >= enterSoft:
		return PressureHigh
	case current >= PressureHigh && used >= leaveSoft:
		return PressureHigh
	}
	return PressureNone
}
