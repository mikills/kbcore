//go:build linux

package memlimit

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// BackstopMark is where the kernel is asked to start reclaiming, above the
// marks the governor acts on. Userspace sheds work first; the kernel throttles
// only if that was not enough. Setting it at the governor's own mark would have
// the kernel stalling allocations while the governor still reported calm,
// because memory.high is policed against memory.current and the governor
// measures the working set inside it.
const BackstopMark = 0.95

// Watcher blocks until the kernel reports memory pressure. It costs nothing
// while idle: no timer, no sampling, the goroutine sits in epoll_wait.
type Watcher struct {
	epfd   int
	wakefd int
	file   *os.File
	source string
	// events is read after every wake. memory.events is level-triggered
	// through kernfs, and the poll flag only clears when the file is read, so
	// skipping this turns the watcher into a spin on one core.
	events   bool
	enforced bool

	// restore puts memory.high back as it was. Left unwritten it ratchets: the
	// next start reads our own mark as the ceiling and takes 90% of that.
	restore func()

	closeOnce sync.Once
	wakeOnce  sync.Once
}

func (w *Watcher) Source() string { return w.source }

// Enforced is true when memory.high was set, so the kernel reclaims and
// throttles on its own and this watcher only decides what work to shed.
func (w *Watcher) Enforced() bool { return w.enforced }

// Watch arms kernel notification for the cgroup at dir, which must be the one
// that supplied the ceiling.
//
// It sets memory.high where it can, because that makes the kernel reclaim at
// the mark rather than leaving minnow to notice afterwards.
//
// For notification it prefers memory.pressure over memory.events. The events
// counter bumps on every allocation the kernel throttles, and memory.high is
// policed against memory.current, so a process whose page cache sits at the
// mark is woken continuously while nothing it can act on has changed. Pressure
// stall is time actually lost to reclaim, which is the thing worth waking for.
func Watch(dir string, ceiling int64) (Notifier, error) {
	own, err := ownCgroupDir()
	if err != nil {
		return nil, err
	}
	// Only ever our own cgroup. Writing memory.high on an ancestor would
	// throttle every sibling sharing it.
	restore := func() {}
	enforced := false
	if dir == "" || dir == own {
		if put, err := setHigh(own, int64(float64(ceiling)*BackstopMark)); err == nil {
			restore, enforced = put, true
		}
	}
	w, err := watchPressure(own)
	if err != nil {
		w, err = watchEvents(own)
	}
	if err != nil {
		restore()
		return nil, err
	}
	w.restore = restore
	w.enforced = enforced
	return w, nil
}

// setHigh writes the backstop and returns the undo.
func setHigh(dir string, mark int64) (func(), error) {
	// Page aligned: the kernel rounds memory.high down to a page multiple, so
	// an unaligned request lands somewhere other than where it was asked for.
	mark -= mark % int64(os.Getpagesize())
	if mark <= 0 {
		return nil, fmt.Errorf("memory.high needs a positive mark")
	}
	path := filepath.Join(dir, "memory.high")
	previous, _ := os.ReadFile(path)
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", mark)), 0o644); err != nil {
		return nil, fmt.Errorf("set memory.high: %w", err)
	}
	return func() {
		value := strings.TrimSpace(string(previous))
		if value == "" {
			value = "max"
		}
		_ = os.WriteFile(path, []byte(value+"\n"), 0o644)
	}, nil
}

// watchEvents is the fallback where pressure stall is unavailable. It wakes far
// more often than it needs to, which is why it is not the first choice.
func watchEvents(dir string) (*Watcher, error) {
	file, err := os.Open(filepath.Join(dir, "memory.events"))
	if err != nil {
		return nil, err
	}
	return arm(file, "memory.events", true)
}

// watchPressure asks for a wake-up when this cgroup stalls on memory for 150ms
// of any second, which means reclaim is already costing real time.
func watchPressure(dir string) (*Watcher, error) {
	file, err := os.OpenFile(filepath.Join(dir, "memory.pressure"), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteString("some 150000 1000000"); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("arm memory.pressure trigger: %w", err)
	}
	return arm(file, "memory.pressure", false)
}

func arm(file *os.File, source string, events bool) (*Watcher, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	w := &Watcher{epfd: epfd, file: file, source: source, events: events}
	event := unix.EpollEvent{Events: unix.EPOLLPRI | unix.EPOLLERR, Fd: int32(file.Fd())}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, int(file.Fd()), &event); err != nil {
		_ = w.Close()
		return nil, err
	}
	// An eventfd in the same set is what lets Close interrupt a blocked wait.
	wakefd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	w.wakefd = wakefd
	wake := unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(wakefd)}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, wakefd, &wake); err != nil {
		_ = w.Close()
		return nil, err
	}
	return w, nil
}

// Wait blocks until the kernel reports pressure, or timeoutMS elapses, or
// Interrupt is called. A negative timeout blocks indefinitely at no cost.
func (w *Watcher) Wait(timeoutMS int) error {
	events := make([]unix.EpollEvent, 2)
	for {
		n, err := unix.EpollWait(w.epfd, events, timeoutMS)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		for _, event := range events[:n] {
			if event.Fd == int32(w.wakefd) {
				return os.ErrClosed
			}
		}
		w.drain()
		return nil
	}
}

// drain re-reads memory.events so kernfs clears the poll flag. Without it the
// next epoll_wait returns immediately, forever.
func (w *Watcher) drain() {
	if !w.events || w.file == nil {
		return
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, w.file)
}

// Interrupt wakes a blocked Wait without tearing anything down, so the waiter
// can return before the descriptors go away.
func (w *Watcher) Interrupt() {
	w.wakeOnce.Do(func() {
		var one [8]byte
		one[7] = 1
		_, _ = unix.Write(w.wakefd, one[:])
	})
}

// Close releases the descriptors and puts memory.high back. It must not run
// while Wait is still blocked: call Interrupt, let Wait return, then Close.
func (w *Watcher) Close() error {
	w.closeOnce.Do(func() {
		if w.restore != nil {
			w.restore()
		}
		if w.wakefd > 0 {
			_ = unix.Close(w.wakefd)
		}
		if w.epfd > 0 {
			_ = unix.Close(w.epfd)
		}
		if w.file != nil {
			_ = w.file.Close()
		}
	})
	return nil
}

// ownCgroupDir is the v2 directory this process is in.
func ownCgroupDir() (string, error) {
	v2, _ := cgroupPaths("/proc/self/cgroup")
	dir := filepath.Join(cgroupRoot, v2)
	if _, err := os.Stat(filepath.Join(dir, "memory.current")); err != nil {
		return "", fmt.Errorf("no cgroup v2 memory controller: %w", err)
	}
	return dir, nil
}
