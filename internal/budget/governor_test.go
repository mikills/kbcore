package budget

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/internal/memlimit"
)

// stubUsage drives memory use, since a test cannot make the process allocate
// gigabytes to order.
type stubUsage struct{ bytes atomic.Int64 }

func (s *stubUsage) read() memlimit.Usage {
	return memlimit.Usage{Bytes: s.bytes.Load(), Source: "stub", Ok: true}
}

// fakeWatch stands in for kernel notification. Wait returns as soon as the
// test moves usage, so the governor is driven by events rather than a clock.
type fakeWatch struct {
	wake     chan struct{}
	closed   chan struct{}
	once     sync.Once
	enforced bool
}

func newFakeWatch(enforced bool) *fakeWatch {
	return &fakeWatch{wake: make(chan struct{}, 64), closed: make(chan struct{}), enforced: enforced}
}

func (f *fakeWatch) watcher(string, int64) (memlimit.Notifier, error) { return f, nil }

func (f *fakeWatch) Enforced() bool { return f.enforced }
func (f *fakeWatch) Source() string { return "fake" }

func (f *fakeWatch) Wait(timeoutMS int) error {
	var timeout <-chan time.Time
	if timeoutMS >= 0 {
		timer := time.NewTimer(time.Duration(timeoutMS) * time.Millisecond)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-f.wake:
		return nil
	case <-timeout:
		return nil
	case <-f.closed:
		return os.ErrClosed
	}
}

func (f *fakeWatch) Interrupt() { f.close() }

func (f *fakeWatch) Close() error { return f.close() }

func (f *fakeWatch) close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeWatch) signal() { f.wake <- struct{}{} }

func TestGovernorSamplesWithoutAKernelSignal(t *testing.T) {
	// The kernel only reports stall, and stall starts above both marks, so a
	// governor that waited for it would never report anything but critical.
	previous := idleInterval
	idleInterval = 10 * time.Millisecond
	t.Cleanup(func() { idleInterval = previous })

	ceiling := int64(4) << 30
	m, usage, watch := governed(t, ceiling)
	usage.bytes.Store(int64(float64(ceiling) * 0.80))

	waitPressure(t, m, PressureHigh)
	require.Zero(t, len(watch.wake), "the tick, not a signal, is what had to find this")
}

func governed(t *testing.T, ceiling int64) (*Manager, *stubUsage, *fakeWatch) {
	t.Helper()
	m := New(planFor(t, ceiling), true)
	usage := &stubUsage{}
	watch := newFakeWatch(true)
	m.startGovernor(context.Background(), usage.read, watch.watcher, nil)
	t.Cleanup(m.StopGovernor)
	return m, usage, watch
}

func waitPressure(t *testing.T, m *Manager, want Pressure) {
	t.Helper()
	require.Eventuallyf(t, func() bool { return m.Pressure() == want },
		5*time.Second, 5*time.Millisecond, "pressure never reached %s", want)
}

func TestGovernor(t *testing.T) {
	ceiling := int64(16) << 30

	t.Run("pressure follows measured use", func(t *testing.T) {
		m, usage, watch := governed(t, ceiling)
		require.Equal(t, PressureNone, m.Pressure())

		usage.bytes.Store(int64(float64(ceiling) * 0.80))
		watch.signal()
		waitPressure(t, m, PressureHigh)

		usage.bytes.Store(int64(float64(ceiling) * 0.95))
		watch.signal()
		waitPressure(t, m, PressureCritical)

		// Back-pressure has to lift, or one spike degrades the process forever.
		// Nothing is signalled: recovery is found by the timed re-read that
		// only runs while pressure is on.
		usage.bytes.Store(int64(float64(ceiling) * 0.10))
		waitPressure(t, m, PressureNone)
	})

	t.Run("a new database gets less while under pressure", func(t *testing.T) {
		m, usage, watch := governed(t, ceiling)
		relaxed, release := m.OpenDatabase("256MB")
		defer release()

		usage.bytes.Store(int64(float64(ceiling) * 0.80))
		watch.signal()
		waitPressure(t, m, PressureHigh)
		squeezed, release2 := m.OpenDatabase("256MB")
		defer release2()
		require.Less(t, parseMB(t, squeezed), parseMB(t, relaxed))

		usage.bytes.Store(int64(float64(ceiling) * 0.95))
		watch.signal()
		waitPressure(t, m, PressureCritical)
		floored, release3 := m.OpenDatabase("256MB")
		defer release3()
		require.Equal(t, m.minPerDB()>>20, parseMB(t, floored))
	})

	t.Run("builds and embeds shed concurrency at critical", func(t *testing.T) {
		m, usage, watch := governed(t, ceiling)
		require.Greater(t, m.BuildThreads(0), 1)
		require.Greater(t, m.EmbedParallelism(0), 1)

		usage.bytes.Store(int64(float64(ceiling) * 0.95))
		watch.signal()
		waitPressure(t, m, PressureCritical)
		require.Equal(t, 1, m.BuildThreads(0), "a build kept its threads past the ceiling")
		require.Equal(t, 1, m.BuildThreads(4096), "an explicit setting outranked the ceiling")
		require.Equal(t, 1, m.EmbedParallelism(0))
		require.Equal(t, 1, m.EmbedParallelism(16))
	})

	t.Run("high pressure halves embed parallelism", func(t *testing.T) {
		m, usage, watch := governed(t, ceiling)
		usage.bytes.Store(int64(float64(ceiling) * 0.80))
		watch.signal()
		waitPressure(t, m, PressureHigh)
		require.Equal(t, 8, m.EmbedParallelism(16))
	})

	t.Run("the marks are where the plan says they are", func(t *testing.T) {
		ceiling := ceiling
		// Pinned: the plan reserves the last tenth, so reacting later than this
		// leaves nothing to react with.
		require.Equal(t, 0.75, SoftMark)
		require.Equal(t, 0.90, HardMark)
		require.Less(t, SoftMark, HardMark)
		require.Less(t, HardMark, 1.0, "reacting at the ceiling is reacting too late")

		// Ceil, not truncate: the mark itself has to land on the mark.
		at := func(fraction float64) int64 { return int64(math.Ceil(float64(ceiling) * fraction)) }
		require.Equal(t, PressureNone, pressureFor(at(0.7499), ceiling, PressureNone))
		require.Equal(t, PressureHigh, pressureFor(at(SoftMark), ceiling, PressureNone), "the soft mark is inclusive")
		require.Equal(t, PressureHigh, pressureFor(at(0.8999), ceiling, PressureHigh))
		// A round ceiling so the mark lands on a whole byte and the comparison
		// is tested at the mark, not just above it.
		require.Equal(t, PressureCritical, pressureFor(900, 1000, PressureHigh), "the hard mark is inclusive")
		require.Equal(t, PressureHigh, pressureFor(899, 1000, PressureHigh))
		require.Equal(t, PressureHigh, pressureFor(750, 1000, PressureNone), "the soft mark is inclusive")
		require.Equal(t, PressureNone, pressureFor(749, 1000, PressureNone))
		require.Equal(t, PressureCritical, pressureFor(at(HardMark), ceiling, PressureHigh))
	})

	t.Run("a level holds until use falls clear of its mark", func(t *testing.T) {
		ceiling := ceiling
		at := func(fraction float64) int64 { return int64(float64(ceiling) * fraction) }
		// Sitting on a mark must not flap: two builds a second apart would
		// otherwise get different thread counts for no reason.
		require.Equal(t, PressureCritical, pressureFor(at(0.88), ceiling, PressureCritical))
		require.Equal(t, PressureHigh, pressureFor(at(0.84), ceiling, PressureCritical), "critical never released")
		require.Equal(t, PressureHigh, pressureFor(at(0.72), ceiling, PressureHigh))
		require.Equal(t, PressureNone, pressureFor(at(0.69), ceiling, PressureHigh), "high never released")
		// Falling fast still steps through High: dropping straight to None
		// would restore full concurrency while use is still above the soft mark.
		require.Equal(t, PressureHigh, pressureFor(at(0.72), ceiling, PressureCritical))
	})

	t.Run("an unreadable platform leaves the governor off", func(t *testing.T) {
		m := New(planFor(t, ceiling), true)
		watch := newFakeWatch(false)
		m.startGovernor(context.Background(), func() memlimit.Usage { return memlimit.Usage{} }, watch.watcher, nil)
		t.Cleanup(m.StopGovernor)
		// Asserting on pressure alone would pass with the guard deleted, since
		// an unreadable sample leaves it at zero either way.
		require.Nil(t, m.gov.Load(), "the governor started without a usable reading")
		require.Equal(t, PressureNone, m.Pressure())
	})

	t.Run("without a plan there is nothing to measure against", func(t *testing.T) {
		m := New(memlimit.Plan{}, false)
		usage := &stubUsage{}
		usage.bytes.Store(ceiling)
		watch := newFakeWatch(false)
		m.startGovernor(context.Background(), usage.read, watch.watcher, nil)
		t.Cleanup(m.StopGovernor)
		// A zero plan has a zero ceiling, so checking pressure alone would pass
		// with the guard gone too.
		require.Nil(t, m.gov.Load(), "the governor started with nothing to measure against")
	})

	t.Run("no kernel notification means no governor", func(t *testing.T) {
		m := New(planFor(t, ceiling), true)
		usage := &stubUsage{}
		usage.bytes.Store(ceiling)
		m.startGovernor(context.Background(),
			usage.read,
			func(string, int64) (memlimit.Notifier, error) { return nil, errors.New("no cgroup") }, nil)
		t.Cleanup(m.StopGovernor)
		// Polling for a spike that lasts milliseconds costs more than it catches.
		require.Equal(t, PressureNone, m.Pressure())
		require.False(t, m.MemoryEnforced())
	})

	t.Run("the kernel enforcing is reported", func(t *testing.T) {
		m, _, _ := governed(t, ceiling)
		require.Eventually(t, m.MemoryEnforced, 5*time.Second, 5*time.Millisecond,
			"memory.high was set but nothing recorded it")
	})

	t.Run("stopping is safe twice and ends the watcher", func(t *testing.T) {
		m, _, _ := governed(t, ceiling)
		g := m.gov.Load()
		require.NotNil(t, g)

		m.StopGovernor()
		m.StopGovernor()
		require.Equal(t, PressureNone, m.Pressure())
		// Stop must not return before the watcher is gone, or shutdown races a
		// goroutine still reading a cgroup file.
		select {
		case <-g.done:
		default:
			t.Fatal("StopGovernor returned with the watcher still running")
		}
	})

	t.Run("starting twice leaves one governor", func(t *testing.T) {
		m, _, watch := governed(t, ceiling)
		first := m.gov.Load()
		m.startGovernor(context.Background(), (&stubUsage{}).read, watch.watcher, nil)
		require.Same(t, first, m.gov.Load(), "a second start replaced the running governor")
	})

	t.Run("pressure is readable while the governor starts and stops", func(t *testing.T) {
		m := New(planFor(t, ceiling), true)
		usage := &stubUsage{}
		watch := newFakeWatch(true)
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = m.Pressure(), m.Usage()
				}
			}
		}()
		for range 50 {
			m.startGovernor(context.Background(), usage.read, watch.watcher, nil)
			m.StopGovernor()
		}
		close(stop)
		<-done
	})

	t.Run("a transition is reported once, with the reading behind it", func(t *testing.T) {
		m := New(planFor(t, ceiling), true)
		usage := &stubUsage{}
		watch := newFakeWatch(true)
		type change struct {
			from, to Pressure
			bytes    int64
		}
		changes := make(chan change, 8)
		m.startGovernor(context.Background(), usage.read, watch.watcher,
			func(from, to Pressure, u memlimit.Usage) { changes <- change{from, to, u.Bytes} })
		t.Cleanup(m.StopGovernor)

		usage.bytes.Store(int64(float64(ceiling) * 0.95))
		watch.signal()
		got := <-changes
		require.Equal(t, PressureNone, got.from)
		require.Equal(t, PressureCritical, got.to)
		require.Equal(t, usage.bytes.Load(), got.bytes)

		// A second wake-up at the same level is not a transition.
		watch.signal()
		watch.signal()
		select {
		case extra := <-changes:
			t.Fatalf("reported an unchanged level: %v -> %v", extra.from, extra.to)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("sustained pressure does not spin the loop", func(t *testing.T) {
		m := New(planFor(t, ceiling), true)
		usage := &stubUsage{}
		usage.bytes.Store(int64(float64(ceiling) * 0.95))
		watch := newFakeWatch(true)
		m.startGovernor(context.Background(), usage.read, watch.watcher, nil)
		t.Cleanup(m.StopGovernor)

		// The kernel bumps memory.events on every allocation past the mark, so
		// while use stays over it the wake-ups are continuous and correct.
		go func() {
			for range 200 {
				watch.signal()
			}
		}()
		waitPressure(t, m, PressureCritical)

		time.Sleep(minWakeInterval * 3)
		g := m.gov.Load()
		require.NotNil(t, g)
		require.LessOrEqual(t, g.wakes.Load(), int64(6),
			"the loop handled wake-ups faster than its floor allows")
	})

	t.Run("a build is refused while critical and admitted once it lifts", func(t *testing.T) {
		m, usage, watch := governed(t, ceiling)
		require.NoError(t, m.AdmitBuild())

		usage.bytes.Store(int64(float64(ceiling) * 0.95))
		watch.signal()
		waitPressure(t, m, PressureCritical)
		// Shrinking shares cannot touch a build already running, so refusing
		// the next one is the only lever that bounds the peak.
		require.ErrorIs(t, m.AdmitBuild(), ErrOverMemoryMark)

		usage.bytes.Store(int64(float64(ceiling) * 0.10))
		waitPressure(t, m, PressureNone)
		require.NoError(t, m.AdmitBuild())
	})
}
