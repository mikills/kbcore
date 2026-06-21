package lease

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

const DefaultS3Prefix = "leases/"

// S3Manager implements Manager using S3 object CAS as the distributed lock
// primitive. A lock object at "<prefix><kbID>.lock" holds the token and expiry.
//
// Protocol:
//   - Acquire: evict any expired/corrupt lock via CAS tombstone, then
//     UploadBytesIfNotExists (If-None-Match: *)
//   - Renew: DownloadBytesWithInfo (single round-trip gives data + ETag),
//     validate token, UploadBytesIfMatch(ETag)
//   - Release: DownloadBytesWithInfo, validate token, UploadBytesIfMatch(ETag)
//     with tombstone to atomically mark released, then best-effort delete
//
// All mutations use CAS — no TOCTOU between read and write.
type S3Manager struct {
	store  *blobstore.S3BlobStore
	prefix string
	clock  Clock
}

func NewS3Manager(store *blobstore.S3BlobStore, prefix string) (*S3Manager, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if strings.TrimSpace(prefix) == "" {
		prefix = DefaultS3Prefix
	}
	return &S3Manager{store: store, prefix: prefix, clock: RealClock}, nil
}

func (m *S3Manager) SetClock(c Clock) {
	if c == nil {
		m.clock = RealClock
		return
	}
	m.clock = c
}

func (m *S3Manager) now() time.Time { return m.clock.Now() }

func (m *S3Manager) key(kbID string) string { return m.prefix + kbID + ".lock" }

func (m *S3Manager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	now := m.now()
	if err := m.evictExpiredCAS(ctx, kbID, now); err != nil {
		return nil, err
	}

	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	expiresAt := now.Add(ttl)
	_, err = m.store.UploadBytesIfNotExists(ctx, m.key(kbID), encodeLockPayload(token, expiresAt))
	if err != nil {
		if errors.Is(err, blobstore.ErrVersionMismatch) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("acquire s3 lease: %w", err)
	}
	return &Lease{KBID: kbID, Token: token, ExpiresAt: expiresAt}, nil
}

func (m *S3Manager) Renew(ctx context.Context, lease *Lease, ttl time.Duration) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil, fmt.Errorf("valid lease is required")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	now := m.now()

	// DownloadBytesWithInfo returns data + ETag in a single round-trip,
	// eliminating the separate Head call and its TOCTOU window.
	data, info, err := m.store.DownloadBytesWithInfo(ctx, m.key(lease.KBID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("renew s3 lease: %w", err)
	}
	if info.Version == "" {
		// Empty ETag means we can't use If-Match — treat as conflict rather
		// than fall through to an unconditional overwrite.
		return nil, ErrConflict
	}
	storedToken, expiresAt, err := decodeLockPayload(data)
	if err != nil || storedToken != lease.Token || !now.Before(expiresAt) {
		return nil, ErrConflict
	}

	newExpiresAt := now.Add(ttl)
	_, err = m.store.UploadBytesIfMatch(ctx, m.key(lease.KBID), encodeLockPayload(lease.Token, newExpiresAt), info.Version)
	if err != nil {
		if errors.Is(err, blobstore.ErrVersionMismatch) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("renew s3 lease: upload: %w", err)
	}
	return &Lease{KBID: lease.KBID, Token: lease.Token, ExpiresAt: newExpiresAt}, nil
}

func (m *S3Manager) Release(ctx context.Context, lease *Lease) error {
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	data, info, err := m.store.DownloadBytesWithInfo(releaseCtx, m.key(lease.KBID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil // already gone
		}
		return fmt.Errorf("release s3 lease: %w", err)
	}
	if info.Version == "" {
		return nil // can't CAS without ETag; treat as released
	}
	storedToken, _, decErr := decodeLockPayload(data)
	if decErr != nil || storedToken != lease.Token {
		return nil // not our lock
	}

	// CAS-write a tombstone to atomically mark the lock as released.
	// If this fails (someone else already replaced the lock), do nothing.
	_, err = m.store.UploadBytesIfMatch(releaseCtx, m.key(lease.KBID), tombstonePayload(), info.Version)
	if err != nil {
		return nil // not our lock anymore
	}
	// Best-effort delete. If it fails or races with another eviction, the
	// tombstone (past expiry) will be cleaned up by the next Acquire.
	_ = m.store.Delete(releaseCtx, m.key(lease.KBID))
	return nil
}

// evictExpiredCAS removes an expired or corrupt lock using a CAS tombstone
// write before deleting, preventing concurrent evictions from wiping a freshly
// acquired live lock.
func (m *S3Manager) evictExpiredCAS(ctx context.Context, kbID string, now time.Time) error {
	data, info, err := m.store.DownloadBytesWithInfo(ctx, m.key(kbID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("s3 lease evict: %w", err)
	}
	if info.Version == "" {
		return nil // can't CAS; leave it alone
	}

	_, expiresAt, decErr := decodeLockPayload(data)
	if decErr == nil && now.Before(expiresAt) {
		return nil // still valid
	}
	// Expired or corrupt — claim exclusive eviction rights via CAS.
	_, err = m.store.UploadBytesIfMatch(ctx, m.key(kbID), tombstonePayload(), info.Version)
	if err != nil {
		return nil // lost the race; another eviction or acquire handled it
	}
	// We hold exclusive rights; delete the tombstone.
	_ = m.store.Delete(ctx, m.key(kbID))
	return nil
}

// tombstonePayload returns a lock payload with a past expiry used to atomically
// claim eviction rights before deleting the lock object.
func tombstonePayload() []byte {
	return encodeLockPayload("tombstone", time.Unix(0, 1))
}

func encodeLockPayload(token string, expiresAt time.Time) []byte {
	return []byte(fmt.Sprintf("%s\n%d", token, expiresAt.UnixNano()))
}

func decodeLockPayload(data []byte) (token string, expiresAt time.Time, err error) {
	parts := bytes.SplitN(data, []byte("\n"), 2)
	if len(parts) != 2 {
		return "", time.Time{}, fmt.Errorf("invalid lock payload")
	}
	token = string(parts[0])
	var ns int64
	if _, err := fmt.Sscanf(string(parts[1]), "%d", &ns); err != nil {
		return "", time.Time{}, fmt.Errorf("invalid lock expiry: %w", err)
	}
	return token, time.Unix(0, ns).UTC(), nil
}
