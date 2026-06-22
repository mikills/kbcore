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
	etag, err := m.verifyOwnership(ctx, lease.KBID, lease.Token, now, true)
	if err != nil {
		return nil, fmt.Errorf("renew s3 lease: %w", err)
	}
	newExpiresAt := now.Add(ttl)
	_, err = m.store.UploadBytesIfMatch(ctx, m.key(lease.KBID), encodeLockPayload(lease.Token, newExpiresAt), etag)
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
	etag, err := m.verifyOwnership(releaseCtx, lease.KBID, lease.Token, m.now(), false)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return nil // not our lock or already released
		}
		return fmt.Errorf("release s3 lease: %w", err)
	}
	_, casErr := m.store.UploadBytesIfMatch(releaseCtx, m.key(lease.KBID), tombstonePayload(), etag)
	if casErr != nil {
		return nil // CAS failed — another caller already replaced this lock
	}
	if deleteErr := m.store.Delete(releaseCtx, m.key(lease.KBID)); deleteErr != nil && !errors.Is(deleteErr, blobstore.ErrNotFound) {
		return fmt.Errorf("release s3 lease: delete tombstone: %w", deleteErr)
	}
	return nil
}

// verifyOwnership reads the lock object and returns its ETag if the token
// matches. Expiry is checked only when checkExpiry is true (Renew requires
// a live lease; Release should clean up an expired but still-present lock).
func (m *S3Manager) verifyOwnership(ctx context.Context, kbID, token string, now time.Time, checkExpiry bool) (string, error) {
	data, info, err := m.store.DownloadBytesWithInfo(ctx, m.key(kbID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return "", ErrConflict
		}
		return "", err
	}
	if info.Version == "" {
		return "", ErrConflict
	}
	storedToken, expiresAt, decErr := decodeLockPayload(data)
	if decErr != nil || storedToken != token {
		return "", ErrConflict
	}
	if checkExpiry && !now.Before(expiresAt) {
		return "", ErrConflict
	}
	return info.Version, nil
}

func (m *S3Manager) evictExpiredCAS(ctx context.Context, kbID string, now time.Time) error {
	data, info, err := m.store.DownloadBytesWithInfo(ctx, m.key(kbID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("s3 lease evict: %w", err)
	}
	if info.Version == "" {
		return nil
	}
	_, expiresAt, decErr := decodeLockPayload(data)
	if decErr == nil && now.Before(expiresAt) {
		return nil
	}
	// CAS claim before delete prevents concurrent eviction from wiping a live lock
	_, casErr := m.store.UploadBytesIfMatch(ctx, m.key(kbID), tombstonePayload(), info.Version)
	if casErr != nil {
		return nil // CAS failed — another eviction or acquire already claimed this slot
	}
	if deleteErr := m.store.Delete(ctx, m.key(kbID)); deleteErr != nil && !errors.Is(deleteErr, blobstore.ErrNotFound) {
		return fmt.Errorf("s3 lease evict: delete tombstone: %w", deleteErr)
	}
	return nil
}

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
