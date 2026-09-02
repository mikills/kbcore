package memlimit

import (
	"sync"
	"time"
)

// PollInterval applies where the kernel offers no way to be told about
// pressure. Deliberately slow, and it reads one small file.
const PollInterval = 2 * time.Second

// pollNotifier is the last resort, for a host with no cgroup v2 controller
// whose kernel also refuses pressure triggers.
type pollNotifier struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newPollNotifier() *pollNotifier {
	return &pollNotifier{closed: make(chan struct{})}
}

func (p *pollNotifier) Source() string { return "poll" }

// Enforced is false: nothing here asks the kernel to hold a line.
func (p *pollNotifier) Enforced() bool { return false }

func (p *pollNotifier) Wait(timeoutMS int) error {
	wait := PollInterval
	if timeoutMS >= 0 && time.Duration(timeoutMS)*time.Millisecond < wait {
		wait = time.Duration(timeoutMS) * time.Millisecond
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-p.closed:
		return errClosed
	case <-timer.C:
		return nil
	}
}

func (p *pollNotifier) Interrupt() { p.closeOnce.Do(func() { close(p.closed) }) }

func (p *pollNotifier) Close() error {
	p.Interrupt()
	return nil
}
