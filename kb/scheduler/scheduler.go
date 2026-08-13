package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mikills/minnow/kb/internal/cron"
	"github.com/mikills/minnow/kb/lease"
)

var schedulerRootContext = context.Background()

const (
	logKeyError      = "error"
	logKeyJobID      = "job_id"
	logKeyDurationMS = "duration_ms"

	defaultSchedulerTickInterval = 1 * time.Minute
	schedulerLeaseKeyPrefix      = "sweep:"
	schedulerMinLeaseTTL         = 60 * time.Second
	// DefaultStopTimeout bounds how long Stop waits for in-flight
	// jobs to drain. Beyond this window the scheduler returns. stragglers
	// may still be running but we do not hold up process shutdown.
	DefaultStopTimeout = 30 * time.Second
	// schedulerObserverBufferSize is how many tick reports can be queued
	// before the dispatcher starts dropping to avoid blocking the cron loop.
	schedulerObserverBufferSize = 64
)

// Outcome reports the result of a single scheduler tick for a job.
type Outcome string

const (
	OutcomeSuccess      Outcome = "success"
	OutcomeSkippedLease Outcome = "skipped_lease"
	OutcomeFailed       Outcome = "failed"
)

// Observer receives one event per tick.
//
// Dispatch is non-blocking: the scheduler enqueues calls on a bounded
// channel and fires them from a dedicated goroutine. A slow observer can
// miss notifications (reflected in the "observer_drops" counter via
// ObserverDrops). it can never block the scheduler.
type Observer interface {
	OnSchedulerTick(jobID string, outcome Outcome, duration time.Duration, err error)
}

// Job is the function signature registered with the scheduler.
// Returning a non-nil error causes the tick to be recorded as Failed.
type Job func(ctx context.Context) error

type registeredJob struct {
	id       string
	cronExpr string
	fn       Job
}

type schedulerTick struct {
	jobID    string
	outcome  Outcome
	duration time.Duration
	err      error
}

// Scheduler runs registered Jobs on cron schedules, wrapping each
// invocation in a per-job WriteLease so only one replica executes per tick.
type Scheduler struct {
	mu        sync.Mutex
	cron      *cron.Cron
	jobs      []registeredJob
	disabled  map[string]struct{}
	observer  Observer
	leaseMgr  lease.Manager
	leaseTTL  time.Duration
	tickEvery time.Duration
	started   bool
	stopping  bool
	stopped   bool
	stopDone  chan struct{}

	// Job lifecycle: every wrapped job runs in a goroutine that participates
	// in jobWG. rootCtx is canceled in Stop to signal cooperative shutdown.
	// stopTimeout bounds the drain.
	jobWG       sync.WaitGroup
	rootCtx     context.Context
	rootCancel  context.CancelFunc
	stopTimeout time.Duration

	// Observer dispatch: buffered channel + dedicated goroutine so a slow
	// observer cannot stall the cron loop. When the channel is full we
	// drop and increment observerDrops.
	observerCh     chan schedulerTick
	observerDone   chan struct{}
	observerDrops  atomic.Uint64
	observerDoneMu sync.Mutex

	logger *slog.Logger
}

// New constructs a Scheduler. Use Register to add jobs and Start to
// begin the cron loop.
func New(
	leaseMgr lease.Manager,
	tickEvery time.Duration,
	disabled []string,
	observer Observer,
) *Scheduler {
	if tickEvery <= 0 {
		tickEvery = defaultSchedulerTickInterval
	}
	if leaseMgr == nil {
		leaseMgr = lease.NewInMemoryManager()
	}

	leaseTTL := max(tickEvery*2, schedulerMinLeaseTTL)

	disabledSet := make(map[string]struct{}, len(disabled))
	for _, id := range disabled {
		id = strings.TrimSpace(id)
		if id != "" {
			disabledSet[id] = struct{}{}
		}
	}

	c := cron.New()
	c.SetInterval(tickEvery)

	rootCtx, rootCancel := context.WithCancel(schedulerRootContext)

	return &Scheduler{
		cron:        c,
		disabled:    disabledSet,
		observer:    observer,
		leaseMgr:    leaseMgr,
		leaseTTL:    leaseTTL,
		tickEvery:   tickEvery,
		rootCtx:     rootCtx,
		rootCancel:  rootCancel,
		stopTimeout: DefaultStopTimeout,
		logger:      slog.Default(),
	}
}

// SetStopTimeout overrides the default drain window applied on Stop. Must be
// called before Start.
func (s *Scheduler) SetStopTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d <= 0 {
		d = DefaultStopTimeout
	}
	s.stopTimeout = d
}

// ObserverDrops reports the number of observer notifications dropped because
// the observer buffer was full.
func (s *Scheduler) ObserverDrops() uint64 {
	return s.observerDrops.Load()
}

// Register adds a job to the scheduler. Must be called before Start. Returns
// an error if cronExpr is invalid. Disabled jobs are silently skipped.
func (s *Scheduler) Register(id, cronExpr string, fn Job) error {
	if id == "" {
		return errors.New("scheduler: job id required")
	}
	if fn == nil {
		return errors.New("scheduler: job fn required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return errors.New("scheduler: cannot register after start")
	}

	if _, isDisabled := s.disabled[id]; isDisabled {
		s.logger.Info("scheduler job disabled, skipping registration", logKeyJobID, id)
		return nil
	}

	wrapped := s.makeLeasedRunner(id, fn)
	if err := s.cron.Add(id, cronExpr, wrapped); err != nil {
		return fmt.Errorf("scheduler: register %q: %w", id, err)
	}

	s.jobs = append(s.jobs, registeredJob{id: id, cronExpr: cronExpr, fn: fn})
	return nil
}

// Start begins the cron loop. Idempotent. also starts the observer
// dispatcher.
//
// Start uses real wall-clock time for the tick loop. it does NOT honour a
// FakeClock that has been substituted into the KB. Simulation code should
// drive jobs directly via Scheduler.RunOnce (or the underlying cron's
// RunDue) with a timestamp derived from the KB's Clock, rather than calling
// Start.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopping || s.stopped {
		return
	}
	s.started = true
	s.startObserverDispatcher()
	s.cron.Start()
}

// Stop halts the cron loop and waits for running jobs to drain.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	timeout := s.stopTimeout
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = s.StopContext(ctx)
}

// BeginStop prevents new jobs and signals in-flight work without waiting.
func (s *Scheduler) BeginStop() {
	s.mu.Lock()
	if s.stopped || s.stopping {
		s.mu.Unlock()
		return
	}
	s.stopping = true
	s.stopDone = make(chan struct{})
	wasStarted := s.started
	cancel := s.rootCancel
	done := s.stopDone
	s.mu.Unlock()
	if wasStarted {
		s.cron.Stop()
	}
	cancel()
	go func() {
		s.jobWG.Wait()
		s.stopObserverDispatcher()
		s.mu.Lock()
		s.stopped = true
		s.stopping = false
		close(done)
		s.mu.Unlock()
	}()
}

// StopContext bounds the wait for in-flight scheduler jobs.
func (s *Scheduler) StopContext(ctx context.Context) error {
	s.BeginStop()
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	done := s.stopDone
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunOnce executes a registered job synchronously, bypassing the cron schedule
// but still acquiring its lease. Returns the outcome and any error.
func (s *Scheduler) RunOnce(ctx context.Context, jobID string) (Outcome, error) {
	s.mu.Lock()
	if s.stopping || s.stopped {
		s.mu.Unlock()
		return OutcomeFailed, errors.New("scheduler: stopping")
	}
	var fn Job
	for _, j := range s.jobs {
		if j.id == jobID {
			fn = j.fn
			break
		}
	}
	leaseTTL := s.leaseTTL
	if fn != nil {
		s.jobWG.Add(1)
	}
	s.mu.Unlock()
	if fn == nil {
		return OutcomeFailed, fmt.Errorf("scheduler: unknown job %q", jobID)
	}
	defer s.jobWG.Done()
	return s.runWithLease(ctx, jobID, fn, leaseTTL)
}

// JobIDs returns the registered (non-disabled) job ids in registration order.
func (s *Scheduler) JobIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.jobs))
	for i, j := range s.jobs {
		out[i] = j.id
	}
	return out
}

// makeLeasedRunner returns the callback handed to the vendored cron. Each
// invocation owns its own goroutine tracking, panic recovery, and context
// derived from the scheduler's rootCtx. leaseTTL is captured by value at
// dispatch time so mid-flight SetInterval changes cannot retroactively
// shrink the TTL observed by a running job.
func (s *Scheduler) makeLeasedRunner(id string, fn Job) func() {
	return func() {
		s.mu.Lock()
		if s.stopped || s.stopping {
			s.mu.Unlock()
			return
		}
		leaseTTL := s.leaseTTL
		rootCtx := s.rootCtx
		s.jobWG.Add(1)
		s.mu.Unlock()

		go s.runLeasedJobTrampoline(rootCtx, id, fn, leaseTTL)
	}
}

func (s *Scheduler) runLeasedJobTrampoline(
	rootCtx context.Context,
	id string,
	fn Job,
	leaseTTL time.Duration,
) {
	defer s.jobWG.Done()
	defer func() { s.handleJobTrampolinePanic(id, recover()) }()
	ctx, cancel := context.WithTimeout(rootCtx, leaseTTL)
	defer cancel()
	_, _ = s.runWithLease(ctx, id, fn, leaseTTL)
}

func (s *Scheduler) handleJobTrampolinePanic(id string, recovered any) {
	if recovered == nil {
		return
	}
	s.logger.Error("scheduler job trampoline panic", logKeyJobID, id, "panic", fmt.Sprint(recovered))
	s.report(id, OutcomeFailed, 0, fmt.Errorf("scheduler job trampoline panicked: %v", recovered))
}

func (s *Scheduler) runWithLease(
	ctx context.Context,
	id string,
	fn Job,
	leaseTTL time.Duration,
) (Outcome, error) {
	leaseKey := schedulerLeaseKeyPrefix + id
	start := time.Now()

	heldLease, err := s.leaseMgr.Acquire(ctx, leaseKey, leaseTTL)
	if err != nil {
		if errors.Is(err, lease.ErrConflict) {
			s.report(id, OutcomeSkippedLease, time.Since(start), nil)
			return OutcomeSkippedLease, nil
		}
		// context.DeadlineExceeded during lease acquisition is a real
		// failure: the lease TTL was not long enough to establish the
		// lease at all. Report it distinctly so operators can tell it
		// apart from a peer-held lease (skipped_lease).
		s.logger.Error("scheduler lease acquire failed", logKeyJobID, id, logKeyError, err)
		s.report(id, OutcomeFailed, time.Since(start), err)
		return OutcomeFailed, err
	}
	defer func() {
		if err := s.leaseMgr.Release(context.WithoutCancel(ctx), heldLease); err != nil {
			s.logger.Warn("scheduler lease release failed", logKeyJobID, id, logKeyError, err)
		}
	}()

	jobErr := safeRun(ctx, fn)
	dur := time.Since(start)

	// A ctx.DeadlineExceeded after safeRun returns without error still
	// means the job overran its lease window and any side-effects are
	// suspect. Surface this as a failure instead of silently swallowing.
	if jobErr == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		jobErr = fmt.Errorf("scheduler job %q exceeded lease TTL", id)
	}

	if jobErr != nil {
		s.logger.Warn(
			"scheduler job failed",
			logKeyJobID,
			id,
			logKeyDurationMS,
			dur.Milliseconds(),
			logKeyError,
			jobErr,
		)
		s.report(id, OutcomeFailed, dur, jobErr)
		return OutcomeFailed, jobErr
	}

	s.logger.Info("scheduler job ok", logKeyJobID, id, logKeyDurationMS, dur.Milliseconds())
	s.report(id, OutcomeSuccess, dur, nil)
	return OutcomeSuccess, nil
}

func safeRun(ctx context.Context, fn Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("scheduler job panicked: %v", r)
		}
	}()
	return fn(ctx)
}

// startObserverDispatcher spins the background goroutine that drains the
// observer channel. Safe to call under s.mu.
func notifyObserver(obs Observer, tick schedulerTick) {
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Warn("scheduler observer panicked", logKeyJobID, tick.jobID, "panic", fmt.Sprint(r))
		}
	}()
	obs.OnSchedulerTick(tick.jobID, tick.outcome, tick.duration, tick.err)
}

func (s *Scheduler) startObserverDispatcher() {
	if s.observer == nil {
		return
	}
	s.observerCh = make(chan schedulerTick, schedulerObserverBufferSize)
	s.observerDone = make(chan struct{})
	go func(obs Observer, in <-chan schedulerTick, done chan<- struct{}) {
		defer close(done)
		for tick := range in {
			notifyObserver(obs, tick)
		}
	}(s.observer, s.observerCh, s.observerDone)
}

// stopObserverDispatcher closes the observer channel (once) and waits for
// the dispatcher goroutine to exit. Takes s.mu while clearing the reference
// so concurrent report callers do not send on a closed channel.
func (s *Scheduler) stopObserverDispatcher() {
	s.observerDoneMu.Lock()
	defer s.observerDoneMu.Unlock()
	s.mu.Lock()
	ch := s.observerCh
	s.observerCh = nil
	s.mu.Unlock()
	if ch == nil {
		return
	}
	close(ch)
	// Observer callbacks are external code and may block forever. Do not let
	// them hold scheduler/resource shutdown hostage; the channel is detached
	// so no additional callbacks can be admitted.
	s.observerDone = nil
}

func (s *Scheduler) report(id string, outcome Outcome, dur time.Duration, err error) {
	if s.observer == nil {
		return
	}
	// For RunOnce paths before Start, dispatch synchronously so tests do
	// not need to stand the scheduler up fully. After Start, we route
	// through the buffered channel so a slow observer cannot block the
	// cron loop.
	s.mu.Lock()
	ch := s.observerCh
	s.mu.Unlock()

	tick := schedulerTick{jobID: id, outcome: outcome, duration: dur, err: err}
	if ch == nil {
		s.notifyObserverSafely(id, outcome, dur, err)
		return
	}
	// A race with stopObserverDispatcher could close ch between our read
	// above and the send below. Recover from the resulting panic. drops
	// post-Stop are not user-visible.
	defer func() {
		if r := recover(); r != nil {
			s.observerDrops.Add(1)
		}
	}()
	select {
	case ch <- tick:
	default:
		s.observerDrops.Add(1)
	}
}

func (s *Scheduler) notifyObserverSafely(id string, outcome Outcome, dur time.Duration, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Warn("scheduler observer panicked", logKeyJobID, id, "panic", fmt.Sprint(r))
		}
	}()
	s.observer.OnSchedulerTick(id, outcome, dur, err)
}
