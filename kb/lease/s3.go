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
// primitive. A lock object at "<prefix><kbID>.lock" holds the token and
// expiry. Acquire uses If-None-Match: * to create the lock atomically;
// Renew uses If-Match on the current ETag; Release deletes it.
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

	// Evict an expired lock before attempting the atomic create.
	if err := m.evictExpired(ctx, kbID, now); err != nil {
		return nil, err
	}

	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	expiresAt := now.Add(ttl)
	payload := encodeLockPayload(token, expiresAt)

	_, err = m.store.UploadBytesIfNotExists(ctx, m.key(kbID), payload)
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

	current, err := m.store.DownloadBytes(ctx, m.key(lease.KBID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("renew s3 lease: read: %w", err)
	}
	storedToken, expiresAt, err := decodeLockPayload(current)
	if err != nil || storedToken != lease.Token || !now.Before(expiresAt) {
		return nil, ErrConflict
	}

	info, err := m.store.Head(ctx, m.key(lease.KBID))
	if err != nil {
		return nil, fmt.Errorf("renew s3 lease: head: %w", err)
	}

	newExpiresAt := now.Add(ttl)
	payload := encodeLockPayload(lease.Token, newExpiresAt)
	_, err = m.store.UploadBytesIfMatch(ctx, m.key(lease.KBID), payload, info.Version)
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

	current, err := m.store.DownloadBytes(releaseCtx, m.key(lease.KBID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("release s3 lease: read: %w", err)
	}
	storedToken, _, decErr := decodeLockPayload(current)
	if decErr != nil || storedToken != lease.Token {
		return nil // not our lock
	}
	if err := m.store.Delete(releaseCtx, m.key(lease.KBID)); err != nil && !errors.Is(err, blobstore.ErrNotFound) {
		return fmt.Errorf("release s3 lease: delete: %w", err)
	}
	return nil
}

func (m *S3Manager) evictExpired(ctx context.Context, kbID string, now time.Time) error {
	current, err := m.store.DownloadBytes(ctx, m.key(kbID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("s3 lease evict: read: %w", err)
	}
	_, expiresAt, err := decodeLockPayload(current)
	if err != nil || now.Before(expiresAt) {
		return nil
	}
	if err := m.store.Delete(ctx, m.key(kbID)); err != nil && !errors.Is(err, blobstore.ErrNotFound) {
		return fmt.Errorf("s3 lease evict: delete: %w", err)
	}
	return nil
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
