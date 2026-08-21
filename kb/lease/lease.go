package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultTTL = 30 * time.Second

var ErrConflict = errors.New("write lease conflict")

type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

var RealClock Clock = realClock{}

type Lease struct {
	KBID      string
	Token     string
	ExpiresAt time.Time
}

type Manager interface {
	Acquire(ctx context.Context, kbID string, ttl time.Duration) (*Lease, error)
	Renew(ctx context.Context, lease *Lease, ttl time.Duration) (*Lease, error)
	Release(ctx context.Context, lease *Lease) error
	// Peek reports the current holder, or nil, taking and extending nothing.
	Peek(ctx context.Context, kbID string) (*Lease, error)
}

type memoryRecord struct {
	token     string
	expiresAt time.Time
}

type InMemoryManager struct {
	mu       sync.Mutex
	leases   map[string]memoryRecord
	tokenSeq atomic.Uint64
	clock    Clock
}

func NewInMemoryManager() *InMemoryManager {
	return &InMemoryManager{leases: make(map[string]memoryRecord), clock: RealClock}
}

func (m *InMemoryManager) SetClock(c Clock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c == nil {
		m.clock = RealClock
		return
	}
	m.clock = c
}

func (m *InMemoryManager) now() time.Time { return m.clock.Now() }

func (m *InMemoryManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec, ok := m.leases[kbID]; ok && now.Before(rec.expiresAt) {
		return nil, ErrConflict
	}
	token := fmt.Sprintf("%s-%d-%d", kbID, now.UnixNano(), m.tokenSeq.Add(1))
	expiresAt := now.Add(ttl)
	m.leases[kbID] = memoryRecord{token: token, expiresAt: expiresAt}
	return &Lease{KBID: kbID, Token: token, ExpiresAt: expiresAt}, nil
}

func (m *InMemoryManager) Peek(ctx context.Context, kbID string) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.leases[kbID]
	if !ok || !now.Before(rec.expiresAt) {
		return nil, nil
	}
	return &Lease{KBID: kbID, Token: rec.token, ExpiresAt: rec.expiresAt}, nil
}

func (m *InMemoryManager) Renew(ctx context.Context, lease *Lease, ttl time.Duration) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lease == nil || lease.KBID == "" || lease.Token == "" {
		return nil, fmt.Errorf("valid lease is required")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.leases[lease.KBID]
	if !ok || rec.token != lease.Token || !now.Before(rec.expiresAt) {
		return nil, ErrConflict
	}
	expiresAt := now.Add(ttl)
	m.leases[lease.KBID] = memoryRecord{token: lease.Token, expiresAt: expiresAt}
	return &Lease{KBID: lease.KBID, Token: lease.Token, ExpiresAt: expiresAt}, nil
}

// Release ignores the context. It does no I/O, and refusing because the caller
// has gone would strand the lease for its whole TTL.
func (m *InMemoryManager) Release(_ context.Context, lease *Lease) error {
	if lease == nil || lease.KBID == "" || lease.Token == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.leases[lease.KBID]
	if !ok {
		return nil
	}
	if rec.token == lease.Token {
		delete(m.leases, lease.KBID)
	}
	return nil
}
