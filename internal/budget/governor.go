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
	// Below SoftMark: budgets apply as planned.
	PressureNone Pressure = iota
	// Past SoftMark: new work takes smaller shares.
	PressureHigh
	// Past HardMark: nothing new is admitted until use falls back.
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
	// Back-pressure starts here, leaving room to react before the plan's share
	// is spent.
	SoftMark = 0.75
	// Past this the remaining tenth is all that stands before the OOM killer.
	HardMark = memlimit.Headroom
	// How far use must fall below a mark before the level drops, so a signal
	// sitting on a mark does not flap.
	releaseMargin = 0.05
	// The kernel only reports crossing upward, so coming back down is the one
	// thing worth sampling for.
	recoveryInterval = 2 * time.Second
	// Sustained pressure wakes us continuously and correctly. Without a floor
	// between wake-ups that is a spin.
	minWakeInterval = 250 * time.Millisecond
)

// The kernel only reports stall, which starts at BackstopMark, above both marks.
// Waiting on it alone would make the first reading critical. A var so tests can
// drive the tick without waiting on it.
var idleInterval = 5 * time.Second

// governor turns real memory use into back-pressure. Nothing in the plan can
// see the HNSW index or any other cgo allocation. Only the kernel counts those.
//
// It wakes on kernel pressure, and samples on a tick because pressure alone
// arrives too late to shed work gently.
type governor struct {
	pressure atomic.Int32
	usage    atomic.Int64
	wakes    atomic.Int64
	stop     context.CancelFunc
	done     chan struct{}
	// Seams for tests, which cannot make the kernel report pressure.
	read  func() memlimit.Usage
	watch func(string, int64) (memlimit.Notifier, error)
	// The kernel throttles at the mark, not just reports it.
	enforced atomic.Bool
	source   atomic.Value
	// Silent degradation is the hardest kind to debug.
	onChange func(from, to Pressure, usage memlimit.Usage)
	dir      string
	// Closes once the watcher is up or has failed, so a caller can log which.
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

// baseGCPercent is GOGC before the governor touched it, read once. It has to be
// forced before the first sample: read lazily, the first read happens on the way
// back down from critical and memoizes the pressed value as the base.
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
	// Teardown belongs to this goroutine alone.
	defer func() { _ = watcher.Close() }()
	baseGCPercent()
	g.enforced.Store(watcher.Enforced())
	g.source.Store(watcher.Source())
	close(g.armed)

	go func() {
		<-ctx.Done()
		watcher.Interrupt()
	}()

	handled := time.Now()
	for {
		// Faster under pressure, to catch the moment it lifts.
		timeout := int(idleInterval / time.Millisecond)
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
