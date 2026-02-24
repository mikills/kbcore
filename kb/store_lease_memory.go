package kb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type inMemoryLeaseRecord struct {
	token     string
	expiresAt time.Time
}

// InMemoryWriteLeaseManager provides in-memory lease coordination.
type InMemoryWriteLeaseManager struct {
	mu       sync.Mutex
	leases   map[string]inMemoryLeaseRecord
	tokenSeq atomic.Uint64
}

// NewInMemoryWriteLeaseManager creates a new in-memory lease manager.
func NewInMemoryWriteLeaseManager() *InMemoryWriteLeaseManager {
	return &InMemoryWriteLeaseManager{
		leases: make(map[string]inMemoryLeaseRecord),
	}
}

// Acquire obtains a write lease for the given kbID.
func (m *InMemoryWriteLeaseManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*WriteLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if ttl <= 0 {
		ttl = defaultWriteLeaseTTL
	}

	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	if rec, ok := m.leases[kbID]; ok && now.Before(rec.expiresAt) {
		return nil, ErrWriteLeaseConflict
	}

	token := fmt.Sprintf("%s-%d-%d", kbID, now.UnixNano(), m.tokenSeq.Add(1))
	expiresAt := now.Add(ttl)
	m.leases[kbID] = inMemoryLeaseRecord{token: token, expiresAt: expiresAt}

	return &WriteLease{KBID: kbID, Token: token, ExpiresAt: expiresAt}, nil
}

// Renew extends an existing write lease.
func (m *InMemoryWriteLeaseManager) Renew(ctx context.Context, lease *WriteLease, ttl time.Duration) (*WriteLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lease == nil || lease.KBID == "" || lease.Token == "" {
		return nil, fmt.Errorf("valid lease is required")
	}
	if ttl <= 0 {
		ttl = defaultWriteLeaseTTL
	}

	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.leases[lease.KBID]
	if !ok || rec.token != lease.Token || !now.Before(rec.expiresAt) {
		return nil, ErrWriteLeaseConflict
	}

	expiresAt := now.Add(ttl)
	m.leases[lease.KBID] = inMemoryLeaseRecord{token: lease.Token, expiresAt: expiresAt}

	return &WriteLease{KBID: lease.KBID, Token: lease.Token, ExpiresAt: expiresAt}, nil
}

// Release gives up a write lease.
func (m *InMemoryWriteLeaseManager) Release(ctx context.Context, lease *WriteLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
