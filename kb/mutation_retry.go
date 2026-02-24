package kb

import (
	"context"
	"errors"
	"time"
)

// MutationRetryStats tracks retry behavior for mutations.
type MutationRetryStats struct {
	Operation       string
	Attempts        int
	ConflictCount   int
	TotalRetryDelay time.Duration
	Success         bool
}

// MutationRetryObserver is notified of mutation retry events.
type MutationRetryObserver interface {
	ObserveMutationRetry(stats MutationRetryStats)
}

// MutationRetryObserverFunc is an adapter to allow ordinary functions
// to be used as MutationRetryObserver.
type MutationRetryObserverFunc func(stats MutationRetryStats)

// ObserveMutationRetry calls f(stats).
func (f MutationRetryObserverFunc) ObserveMutationRetry(stats MutationRetryStats) {
	if f != nil {
		f(stats)
	}
}

// WithMutationRetryObserver sets an observer for mutation retry events.
func WithMutationRetryObserver(observer MutationRetryObserver) KBOption {
	return func(kb *KB) {
		kb.RetryObserver = observer
	}
}

// SetMutationRetryObserver updates the observer for mutation retry events.
func (l *KB) SetMutationRetryObserver(observer MutationRetryObserver) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.RetryObserver = observer
}

func runWithUploadRetry(ctx context.Context, operation string, maxRetries int, observer MutationRetryObserver, op func() error) error {
	if maxRetries < 0 {
		maxRetries = 0
	}

	stats := MutationRetryStats{Operation: operation}
	for {
		stats.Attempts++
		err := op()
		if err == nil {
			stats.Success = true
			notifyMutationRetryObserver(observer, stats)
			return nil
		}
		if !errors.Is(err, ErrBlobVersionMismatch) {
			notifyMutationRetryObserver(observer, stats)
			return err
		}

		stats.ConflictCount++
		if stats.ConflictCount > maxRetries {
			notifyMutationRetryObserver(observer, stats)
			return err
		}

		attempt := stats.ConflictCount

		backoff := time.Duration(attempt*attempt) * 10 * time.Millisecond
		if backoff <= 0 {
			backoff = 10 * time.Millisecond
		}
		stats.TotalRetryDelay += backoff

		if err := sleepWithContext(ctx, backoff); err != nil {
			notifyMutationRetryObserver(observer, stats)
			return err
		}
	}
}

func notifyMutationRetryObserver(observer MutationRetryObserver, stats MutationRetryStats) {
	if observer == nil {
		return
	}
	observer.ObserveMutationRetry(stats)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
