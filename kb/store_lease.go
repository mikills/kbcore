// store_lease.go defines the WriteLeaseManager interface and the KB's lease
// acquisition helpers.
//
// System fit:
//
//   - Every KB write path (upsert, delete, compaction) calls acquireWriteLease
//     before doing any work. The lease serialises writes per KB ID cluster-wide,
//     reducing duplicate shard uploads and CAS conflicts on the manifest.
//   - The lease is intentionally coarse: it does not guarantee exclusivity —
//     the manifest CAS (UploadIfMatch) remains the hard correctness guard.
//     The lease is an optimisation that makes conflicts rare rather than safe.
//
// Implementations:
//
//   - InMemoryWriteLeaseManager — in-process mutex, suitable for single-pod
//     deployments and tests.
//   - RedisWriteLeaseManager — Redis SET NX / Lua scripts, suitable for
//     multi-pod Kubernetes deployments.

package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const defaultWriteLeaseTTL = 30 * time.Second

// WriteLease represents a held distributed write lock for a single KB. The
// Token field is used by the lease manager to verify ownership on Renew and
// Release, preventing one writer from accidentally releasing another's lease.
type WriteLease struct {
	KBID      string
	Token     string
	ExpiresAt time.Time
}

// WriteLeaseManager provides distributed coordination for KB writes.
// Acquire returns ErrWriteLeaseConflict when the lease is already held.
// Renew extends an existing lease; it returns ErrWriteLeaseConflict if the
// lease has expired or been taken by another writer. Release is always
// best-effort and must not be skipped on error paths.
type WriteLeaseManager interface {
	Acquire(ctx context.Context, kbID string, ttl time.Duration) (*WriteLease, error)
	Renew(ctx context.Context, lease *WriteLease, ttl time.Duration) (*WriteLease, error)
	Release(ctx context.Context, lease *WriteLease) error
}

// writeLeaseManagerAndTTL returns the configured WriteLeaseManager and TTL,
// falling back to an in-memory manager and defaultWriteLeaseTTL when either is
// unset. This ensures single-pod deployments work without explicit configuration.
func (l *KB) writeLeaseManagerAndTTL() (WriteLeaseManager, time.Duration) {
	leaseManager := l.WriteLeaseManager
	if leaseManager == nil {
		leaseManager = NewInMemoryWriteLeaseManager()
	}
	ttl := l.WriteLeaseTTL
	if ttl <= 0 {
		ttl = defaultWriteLeaseTTL
	}
	return leaseManager, ttl
}

// acquireWriteLease acquires a write lease for kbID and returns the manager
// and lease. The caller must defer leaseManager.Release(context.Background(), lease)
// regardless of subsequent errors to avoid holding the lease until TTL expiry.
//
// Conflicts are logged at WARN level; other errors at ERROR level.
func (l *KB) acquireWriteLease(ctx context.Context, kbID string) (WriteLeaseManager, *WriteLease, error) {
	leaseManager, ttl := l.writeLeaseManagerAndTTL()
	lease, err := leaseManager.Acquire(ctx, kbID, ttl)
	if err != nil {
		if errors.Is(err, ErrWriteLeaseConflict) {
			slog.Default().WarnContext(ctx, "write lease acquisition conflict", "kb_id", kbID, "reason", "lease_conflict", "ttl", ttl.String())
		} else {
			slog.Default().ErrorContext(ctx, "write lease acquisition failed", "kb_id", kbID, "reason", "lease_acquire_failed", "error", err)
		}
		return nil, nil, fmt.Errorf("acquire write lease: %w", err)
	}
	return leaseManager, lease, nil
}
