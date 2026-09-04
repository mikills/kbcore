package memlimit

import "errors"

// Usage is what this process is using right now, against the ceiling it must
// stay under. Ok is false where the platform cannot report it, in which case
// callers must not infer headroom from silence.
type Usage struct {
	Bytes  int64
	Source string
	Ok     bool
}

// Current reads memory use for the cgroup at dir, which must be the one that
// supplied the ceiling. An empty dir falls back to this process's own resident
// pages. Either way it counts DuckDB's cgo, which the Go runtime never sees.
func Current(dir string) Usage { return currentUsage(dir) }

// Notifier blocks until the kernel reports memory pressure.
type Notifier interface {
	// Wait returns on pressure, on timeout, or with an error once closed. A
	// negative timeout means the caller has nothing to wake for, so a watcher
	// blocks; a poller still returns on its own interval.
	Wait(timeoutMS int) error
	// Interrupt wakes a blocked Wait without releasing anything, so the waiter
	// returns before Close runs.
	Interrupt()
	Close() error
	// Enforced reports that the kernel throttles at the mark itself rather
	// than only reporting that it was crossed.
	Enforced() bool
	// Source names the mechanism, for startup logs.
	Source() string
}

// errClosed ends a Wait once the notifier is interrupted.
var errClosed = errors.New("memory notifier closed")
