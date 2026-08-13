package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/eventing"
)

// WorkerResult captures the post-handle commit work for a worker attempt.
type WorkerResult struct {
	FollowUps     []eventing.Event
	Commit        func(context.Context) error
	AfterTerminal func(context.Context) error
}

// Worker handles events of a single Kind. Handle runs the side-effects and
// returns the follow-up events plus any worker-specific commit-side mutation
// that must land atomically with Ack.
type Worker interface {
	Kind() eventing.EventKind
	WorkerID() string
	Handle(ctx context.Context, event *eventing.Event) (WorkerResult, error)
}

type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

var RealClock Clock = realClock{}

// WorkerPoolConfig configures a worker pool.

type FailureEventInput struct {
	Event      *eventing.Event
	WorkerKind eventing.EventKind
	Error      error
	WillRetry  bool
	Now        time.Time
}

type FailureEventBuilder func(FailureEventInput) (eventing.Event, error)

type WorkerPoolConfig struct {
	Concurrency       int
	PollInterval      time.Duration
	VisibilityTimeout time.Duration
	MaxAttempts       int
	// Clock drives timestamps on synthetic worker.failed events and is also
	// used for elapsed-time measurements where deterministic sim matters.
	// Defaults to RealClock when nil.
	Clock             Clock
	BuildFailureEvent FailureEventBuilder
}

const (
	defaultWorkerConcurrency = 1 << 2
	logKeyError              = "error"
	logKeyEventID            = "event_id"
	mongoKeyKind             = "kind"
)

func (c WorkerPoolConfig) withDefaults() WorkerPoolConfig {
	out := c
	if out.Clock == nil {
		out.Clock = RealClock
	}
	if out.Concurrency <= 0 {
		out.Concurrency = defaultWorkerConcurrency
	}
	if out.PollInterval <= 0 {
		out.PollInterval = 250 * time.Millisecond
	}
	if out.VisibilityTimeout <= 0 {
		out.VisibilityTimeout = 30 * time.Second
	}
	if out.MaxAttempts <= 0 {
		out.MaxAttempts = 5
	}
	return out
}

// WorkerMetrics observes per-event outcomes.
type WorkerMetrics interface {
	OnWorkerTick(kind eventing.EventKind, workerID string, outcome string, duration time.Duration, err error)
}

// NoopWorkerMetrics discards observations.
type NoopWorkerMetrics struct{}

// OnWorkerTick implements WorkerMetrics.
func (NoopWorkerMetrics) OnWorkerTick(_ eventing.EventKind, _ string, _ string, _ time.Duration, _ error) {
}

// Sentinel errors for WorkerPool construction and lifecycle.
var (
	ErrStoreNotTransactional = errors.New("worker pool: event store must implement eventing.TransactionRunner")
	ErrAlreadyStarted        = errors.New("worker pool: already started")
)

// WorkerPool runs goroutines that claim and dispatch events for a single
// Worker.
type WorkerPool struct {
	worker  Worker
	store   eventing.Store
	runner  eventing.TransactionRunner
	inbox   eventing.EventInbox
	cfg     WorkerPoolConfig
	metrics WorkerMetrics

	wg     sync.WaitGroup
	cancel context.CancelFunc
	done   chan struct{}

	startedMu sync.Mutex
	started   bool
}

// NewWorkerPool constructs a pool around the given worker. The eventing.Store
// must also implement eventing.TransactionRunner, otherwise the pool cannot uphold
// the outbox's atomicity contract and construction returns
// ErrStoreNotTransactional.
func NewWorkerPool(
	worker Worker,
	store eventing.Store,
	inbox eventing.EventInbox,
	cfg WorkerPoolConfig,
) (*WorkerPool, error) {
	runner, ok := store.(eventing.TransactionRunner)
	if !ok {
		return nil, ErrStoreNotTransactional
	}
	return &WorkerPool{
		worker:  worker,
		store:   store,
		runner:  runner,
		inbox:   inbox,
		cfg:     cfg.withDefaults(),
		metrics: NoopWorkerMetrics{},
	}, nil
}

// SetMetrics attaches a metrics observer. Safe to call before Start.
func (p *WorkerPool) SetMetrics(m WorkerMetrics) {
	if m == nil {
		m = NoopWorkerMetrics{}
	}
	p.metrics = m
}

// Start begins polling. Returns ErrAlreadyStarted if the pool was already
// started and not yet stopped.
func (p *WorkerPool) Start(parentCtx context.Context) error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()
	if p.started {
		return ErrAlreadyStarted
	}
	p.started = true

	ctx, cancel := context.WithCancel(parentCtx)
	p.cancel = cancel
	p.done = make(chan struct{})

	for i := 0; i < p.cfg.Concurrency; i++ {
		p.startWorker(ctx)
	}
	done := p.done
	go func() {
		p.wg.Wait()
		close(done)
	}()
	return nil
}

func (p *WorkerPool) startWorker(ctx context.Context) {
	p.wg.Add(1)
	go p.loop(ctx)
}

// Stop signals workers to exit and waits for them. After Stop returns, the
// pool may be started again with Start.
func (p *WorkerPool) Stop() { _ = p.StopContext(context.Background()) }

// BeginStop signals this pool without waiting, allowing runtime shutdown to
// cancel every pool before draining any one of them.
func (p *WorkerPool) BeginStop() {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()
	if p.started && p.cancel != nil {
		p.cancel()
	}
}

// StopContext bounds the wait for in-flight worker handlers.
func (p *WorkerPool) StopContext(ctx context.Context) error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()
	if !p.started || p.cancel == nil {
		return nil
	}
	cancel := p.cancel
	done := p.done
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	p.cancel = nil
	p.done = nil
	p.started = false
	return nil
}

func (p *WorkerPool) loop(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *WorkerPool) tick(ctx context.Context) {
	for {
		event, err := p.store.Claim(ctx, p.worker.Kind(), p.worker.WorkerID(), p.cfg.VisibilityTimeout)
		if err != nil {
			if errors.Is(err, eventing.ErrNoneAvailable) {
				return
			}
			slog.Default().Warn("worker claim failed", mongoKeyKind, string(p.worker.Kind()), logKeyError, err)
			return
		}
		p.dispatch(ctx, event)
	}
}

// HandleOnce claims at most one event and dispatches it. Returns the
// dispatched event id and the handler error (if any).
func (p *WorkerPool) HandleOnce(ctx context.Context) (string, error) {
	event, err := p.store.Claim(ctx, p.worker.Kind(), p.worker.WorkerID(), p.cfg.VisibilityTimeout)
	if err != nil {
		return "", err
	}
	return event.EventID, p.dispatch(ctx, event)
}

func (p *WorkerPool) dispatch(ctx context.Context, event *eventing.Event) error {
	start := time.Now()

	// Run side-effects. They must remain idempotent across crashes because
	// until MarkProcessed lands inside the commit transaction, a concurrent
	// worker (or the reaper+next claim) may rerun Handle on the same event.
	result, handlerErr := safeHandle(ctx, p.worker, event)
	if handlerErr != nil {
		if p.recordFailure(ctx, event, handlerErr, time.Since(start)) {
			p.runAfterTerminal(ctx, event, result.AfterTerminal)
		}
		return handlerErr
	}

	commitErr := p.runner.InTransaction(ctx, func(ctx context.Context) error {
		return p.commitWorkerResult(ctx, event, result)
	})
	dur := time.Since(start)
	return p.finishDispatch(ctx, event, result, commitErr, dur)
}

func (p *WorkerPool) commitWorkerResult(ctx context.Context, event *eventing.Event, result WorkerResult) error {
	// Order matters: follow-ups → commit mutation → MarkProcessed → Ack source.
	for _, up := range result.FollowUps {
		if err := p.store.Append(ctx, up); err != nil && !errors.Is(err, eventing.ErrDuplicateKey) {
			return fmt.Errorf("append follow-up %s: %w", up.Kind, err)
		}
	}
	if result.Commit != nil {
		if err := result.Commit(ctx); err != nil {
			return err
		}
	}
	// Record inbox idempotency before Ack. The in-memory transaction runner is
	// best-effort and Ack releases the payload, so a later inbox failure must
	// never requeue a payloadless source event.
	if err := p.markWorkerProcessed(ctx, event); err != nil {
		return err
	}
	return p.store.Ack(ctx, event.EventID)
}

func (p *WorkerPool) markWorkerProcessed(ctx context.Context, event *eventing.Event) error {
	if event.IdempotencyKey == "" || p.inbox == nil {
		return nil
	}
	return p.inbox.MarkProcessed(ctx, p.worker.WorkerID(), event.IdempotencyKey, event.EventID)
}

func (p *WorkerPool) finishDispatch(
	ctx context.Context,
	event *eventing.Event,
	result WorkerResult,
	commitErr error,
	dur time.Duration,
) error {
	if commitErr == nil {
		p.runAfterTerminal(ctx, event, result.AfterTerminal)
		p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), "success", dur, nil)
		return nil
	}
	if eventing.IsInboxDuplicate(commitErr) {
		return p.finishDuplicateDispatch(ctx, event, result.AfterTerminal, dur)
	}
	slog.Default().
		Warn("worker commit failed", mongoKeyKind, string(p.worker.Kind()), logKeyEventID, event.EventID, logKeyError, commitErr)
	if p.recordFailure(ctx, event, commitErr, dur) {
		p.runAfterTerminal(ctx, event, result.AfterTerminal)
	}
	return commitErr
}

func (p *WorkerPool) finishDuplicateDispatch(
	ctx context.Context,
	event *eventing.Event,
	afterTerminal func(context.Context) error,
	dur time.Duration,
) error {
	slog.Default().
		Info("worker lost inbox race; acking duplicate", mongoKeyKind, string(p.worker.Kind()), logKeyEventID, event.EventID, "idempotency_key", event.IdempotencyKey)
	if err := p.store.Ack(ctx, event.EventID); err != nil && !errors.Is(err, eventing.ErrNotFound) {
		slog.Default().Warn("worker ack after duplicate failed", logKeyError, err)
		return err
	}
	p.runAfterTerminal(ctx, event, afterTerminal)
	p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), "duplicate", dur, nil)
	return nil
}

// recordFailure appends a worker.failed domain event and then calls
// eventing.Store.Fail. The ordering matters: if the fail-event append fails
// we must NOT transition the source to Dead, because observers would then
// see "gone dead, no reason". Leaving the source pending lets the next
// visibility-timeout retry try again.
func (p *WorkerPool) recordFailure(ctx context.Context, event *eventing.Event, handlerErr error, dur time.Duration) bool {
	maxAttempts := event.EffectiveMaxAttempts()
	willRetry := event.Attempt < maxAttempts
	if p.cfg.BuildFailureEvent == nil {
		if err := p.store.Fail(ctx, event.EventID, event.Attempt, handlerErr.Error()); err != nil {
			slog.Default().Warn("worker store.Fail failed", logKeyError, err)
			return false
		}
		p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), failureOutcome(willRetry), dur, handlerErr)
		return !willRetry
	}
	failEvent, err := p.cfg.BuildFailureEvent(
		FailureEventInput{
			Event:      event,
			WorkerKind: p.worker.Kind(),
			Error:      handlerErr,
			WillRetry:  willRetry,
			Now:        p.cfg.Clock.Now(),
		},
	)
	if err == nil {
		err = p.store.Append(ctx, failEvent)
	}
	if err != nil && !errors.Is(err, eventing.ErrDuplicateKey) {
		slog.Default().
			Warn("worker.failed append failed; leaving source pending for retry", mongoKeyKind, string(p.worker.Kind()), logKeyEventID, event.EventID, logKeyError, err)
		p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), "failed", dur, handlerErr)
		return false
	}
	if err := p.store.Fail(ctx, event.EventID, event.Attempt, handlerErr.Error()); err != nil {
		slog.Default().Warn("worker store.Fail failed", logKeyError, err)
		return false
	}
	p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), failureOutcome(willRetry), dur, handlerErr)
	return !willRetry
}

func (p *WorkerPool) runAfterTerminal(
	ctx context.Context,
	event *eventing.Event,
	callback func(context.Context) error,
) {
	if callback == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := callback(cleanupCtx); err != nil {
		slog.Default().Warn("worker post-terminal cleanup failed",
			mongoKeyKind, string(event.Kind), logKeyEventID, event.EventID, logKeyError, err)
	}
}

func failureOutcome(willRetry bool) string {
	if willRetry {
		return "failed"
	}
	return "dead"
}

func safeHandle(ctx context.Context, w Worker, event *eventing.Event) (result WorkerResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("worker panicked: %v", r)
		}
	}()
	return w.Handle(ctx, event)
}
