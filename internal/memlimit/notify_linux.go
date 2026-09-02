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

// BackstopMark sits above the marks the governor acts on, so userspace sheds
// work first. It is policed against memory.current, which includes the page
// cache the governor subtracts, so the two cannot share a mark.
const BackstopMark = 0.95

// Watcher blocks in epoll_wait until the kernel reports memory pressure.
type Watcher struct {
	epfd   int
	wakefd int
	file   *os.File
	source string
	// memory.events is level-triggered: the poll flag only clears on read, so
	// skipping that turns the watcher into a spin.
	events   bool
	enforced bool

	// Left unrestored, the next start reads our own mark as the ceiling.
	restore func()

	// Orders Interrupt against Close. A late Interrupt would otherwise write to
	// a descriptor the process has handed back.
	mu     sync.Mutex
	woken  bool
	closed bool
}

func (w *Watcher) Source() string { return w.source }

// Enforced means memory.high was set, so the kernel throttles on its own.
func (w *Watcher) Enforced() bool { return w.enforced }

// Watch arms kernel notification for dir, which must be the cgroup that
// supplied the ceiling. It sets memory.high where it can.
//
// It prefers memory.pressure to memory.events. The events counter bumps on
// every throttled allocation, so a process whose page cache sits at the mark is
// woken continuously. Stall time is the part worth waking for.
func Watch(dir string, ceiling int64) (Notifier, error) {
	own, err := ownCgroupDir()
	if err != nil {
		// No cgroup v2 controller. On a single-tenant VM machine-wide stall is
		// this process's stall anyway.
		if w, perr := watchPressure(globalPressurePath); perr == nil {
			return w, nil
		}
		// Fly's kernel reports pressure but refuses triggers on it.
		return newPollNotifier(), nil
	}
	// Writing memory.high on an ancestor would throttle every sibling.
	restore := func() {}
	enforced := false
	if dir == "" || dir == own {
		if put, err := setHigh(own, int64(float64(ceiling)*BackstopMark)); err == nil {
			restore, enforced = put, true
		}
	}
	w, err := watchPressure(filepath.Join(own, "memory.pressure"))
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
	// The kernel rounds memory.high down to a page multiple.
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

// watchEvents is the fallback where stall is unavailable. It over-wakes.
func watchEvents(dir string) (*Watcher, error) {
	file, err := os.Open(filepath.Join(dir, "memory.events"))
	if err != nil {
		return nil, err
	}
	return arm(file, "memory.events", true)
}

// globalPressurePath is machine-wide stall, for hosts with no cgroup v2.
const globalPressurePath = "/proc/pressure/memory"

// 150ms of stall in any second means reclaim is already costing real time.
func watchPressure(path string) (*Watcher, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteString("some 150000 1000000"); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("arm pressure trigger on %s: %w", path, err)
	}
	return arm(file, filepath.Base(path), false)
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
	// An eventfd in the same set lets Interrupt wake a blocked Wait.
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

// Wait returns on pressure, on timeout, or on Interrupt.
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

// Re-read so kernfs clears the poll flag, or epoll_wait spins forever.
func (w *Watcher) drain() {
	if !w.events || w.file == nil {
		return
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, w.file)
}

// Interrupt wakes a blocked Wait without tearing anything down.
func (w *Watcher) Interrupt() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.woken || w.closed {
		return
	}
	w.woken = true
	var one [8]byte
	one[7] = 1
	_, _ = unix.Write(w.wakefd, one[:])
}

// Close must not run while Wait is blocked. Interrupt, let Wait return, then
// Close. A later Interrupt is a no-op.
func (w *Watcher) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
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
