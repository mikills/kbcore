package lease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	lockFileSuffix  = ".lock"
	takeoverSuffix  = ".takeover"
	takeoverTimeout = 30 * time.Second
)

// FileManager keeps leases on disk, claimed with an exclusive link so two
// processes sharing a directory cannot both believe they hold one.
type FileManager struct {
	mu    sync.Mutex
	dir   string
	clock Clock

	// Test seams for the interleaving where a lease lapses between the read and
	// the write and another process takes it.
	beforeEvict        func()
	beforeRenewWrite   func()
	beforeReleaseWrite func()
}

// NewFileManager does not touch the filesystem, so a config check cannot
// create the directory.
func NewFileManager(dir string) (*FileManager, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("lease directory is required")
	}
	return &FileManager{dir: dir, clock: RealClock}, nil
}

func (m *FileManager) SetClock(c Clock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c == nil {
		m.clock = RealClock
		return
	}
	m.clock = c
}

func (m *FileManager) now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clock.Now()
}

// path appends a digest so two keys that escape alike stay apart.
func (m *FileManager) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(m.dir, safeKeySegment(key)+"-"+hex.EncodeToString(sum[:8])+lockFileSuffix)
}

func safeKeySegment(key string) string {
	var b strings.Builder
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 48 {
			break
		}
	}
	if b.Len() == 0 {
		return "lease"
	}
	return b.String()
}

// read reports the recorded holder. An unreadable record is not a free lease,
// so only a missing or undecodable one counts as unheld.
func (m *FileManager) read(key string) (string, time.Time, bool, error) {
	data, err := os.ReadFile(m.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, fmt.Errorf("read lease record: %w", err)
	}
	token, expiresAt, err := decodeLockPayload(data)
	if err != nil || strings.TrimSpace(token) == "" {
		return "", time.Time{}, false, nil
	}
	return token, expiresAt, true, nil
}

// stage writes the record to a temporary file for the caller to claim or
// replace with.
func (m *FileManager) stage(token string, expiresAt time.Time) (string, error) {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return "", fmt.Errorf("create lease directory: %w", err)
	}
	tmp, err := os.CreateTemp(m.dir, ".lease-*")
	if err != nil {
		return "", fmt.Errorf("write lease record: %w", err)
	}
	if _, err := tmp.Write(encodeLockPayload(token, expiresAt)); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write lease record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write lease record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write lease record: %w", err)
	}
	return tmp.Name(), nil
}

// claim links the staged record to the lease name. Link fails when it exists.
func (m *FileManager) claim(key, token string, expiresAt time.Time) error {
	tmp, err := m.stage(token, expiresAt)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, m.path(key)); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrConflict
		}
		return fmt.Errorf("claim lease: %w", err)
	}
	m.syncDir()
	return nil
}

// replace overwrites a record this caller already owns.
func (m *FileManager) replace(key, token string, expiresAt time.Time) error {
	tmp, err := m.stage(token, expiresAt)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path(key)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write lease record: %w", err)
	}
	m.syncDir()
	return nil
}

// syncDir persists the directory entry. Best effort, because the lease is
// already claimed and failing the caller would be worse.
func (m *FileManager) syncDir() {
	dir, err := os.Open(m.dir)
	if err != nil {
		return
	}
	defer dir.Close()
	_ = dir.Sync()
}

// lockTakeover claims the key's takeover marker, the only mutex two processes
// sharing this directory have. Linked into place complete, since an empty
// marker reads as no deadline and gets cleared while still in use.
func (m *FileManager) lockTakeover(key string, now time.Time) (func(), error) {
	marker := m.path(key) + takeoverSuffix
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	tmp, err := m.stage(token, now.Add(takeoverTimeout))
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, marker); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("claim lease takeover: %w", err)
		}
		// Carried in the marker, not its mtime, which is a different clock.
		if data, readErr := os.ReadFile(marker); readErr == nil {
			if _, deadline, decErr := decodeLockPayload(data); decErr == nil && now.Before(deadline) {
				return nil, ErrConflict
			}
		}
		_ = os.Remove(marker)
		return nil, ErrConflict
	}
	// Only this caller's marker. A replaced one belongs to whoever holds it.
	return func() {
		if data, err := os.ReadFile(marker); err == nil {
			if held, _, decErr := decodeLockPayload(data); decErr == nil && held != token {
				return
			}
		}
		_ = os.Remove(marker)
	}, nil
}

// evictExpired removes a lapsed record so the claim runs against a free name.
func (m *FileManager) evictExpired(key string, now time.Time) error {
	unlock, err := m.lockTakeover(key, now)
	if err != nil {
		return err
	}
	defer unlock()

	// Re-read under the marker, which the first read was not.
	_, expiresAt, held, err := m.read(key)
	if err != nil {
		return err
	}
	if held && now.Before(expiresAt) {
		return ErrConflict
	}
	if err := os.Remove(m.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("evict expired lease: %w", err)
	}
	return nil
}

func (m *FileManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*Lease, error) {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	_, expiresAt, held, err := m.read(kbID)
	if err != nil {
		return nil, err
	}
	if held && now.Before(expiresAt) {
		return nil, ErrConflict
	}
	// A bare remove lets two processes interleave and both end up holding.
	if _, statErr := os.Stat(m.path(kbID)); statErr == nil {
		if m.beforeEvict != nil {
			m.beforeEvict()
		}
		if err := m.evictExpired(kbID, now); err != nil {
			return nil, err
		}
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	expires := now.Add(ttl)
	if err := m.claim(kbID, token, expires); err != nil {
		return nil, err
	}
	return &Lease{KBID: kbID, Token: token, ExpiresAt: expires}, nil
}

func (m *FileManager) Peek(ctx context.Context, kbID string) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	token, expiresAt, held, err := m.read(kbID)
	if err != nil || !held || !now.Before(expiresAt) {
		return nil, err
	}
	return &Lease{KBID: kbID, Token: token, ExpiresAt: expiresAt}, nil
}

func (m *FileManager) Renew(ctx context.Context, lease *Lease, ttl time.Duration) (*Lease, error) {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	// Held across the read and the rename, which would otherwise put this
	// holder back over whoever took the lapsed lease.
	unlock, err := m.lockTakeover(lease.KBID, now)
	if err != nil {
		return nil, err
	}
	defer unlock()
	token, expiresAt, held, err := m.read(lease.KBID)
	if err != nil {
		return nil, err
	}
	if !held || token != lease.Token || !now.Before(expiresAt) {
		return nil, ErrConflict
	}
	if m.beforeRenewWrite != nil {
		m.beforeRenewWrite()
	}
	renewed := now.Add(ttl)
	if err := m.replace(lease.KBID, lease.Token, renewed); err != nil {
		return nil, err
	}
	return &Lease{KBID: lease.KBID, Token: lease.Token, ExpiresAt: renewed}, nil
}

func (m *FileManager) Release(ctx context.Context, lease *Lease) error {
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Same read-then-write hazard as Renew.
	unlock, err := m.lockTakeover(lease.KBID, m.clock.Now())
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return nil
		}
		return err
	}
	defer unlock()
	token, _, held, err := m.read(lease.KBID)
	if err != nil {
		return err
	}
	if !held || token != lease.Token {
		return nil
	}
	if m.beforeReleaseWrite != nil {
		m.beforeReleaseWrite()
	}
	if err := os.Remove(m.path(lease.KBID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("release lease: %w", err)
	}
	m.syncDir()
	return nil
}
