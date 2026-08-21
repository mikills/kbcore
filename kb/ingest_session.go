package kb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/cacheevict"
)

const (
	// Covers embedding one batch, the gap between renewals.
	DefaultIngestSessionTTL = 10 * time.Minute

	instanceIDFileName = cacheevict.InstanceIDFileName

	// A separate key space from compaction, which leases the bare kbID.
	ingestLeasePrefix = "ingest:"
)

// IngestSessions stops two deferring clients interleaving in the same shard.
// Unpublished rows sit in one instance's local shard, so the handle names the
// instance holding them and the lease alone is not enough.
type IngestSessions struct {
	mgr      WriteLeaseManager
	ttl      time.Duration
	cacheDir string

	instanceOnce sync.Once
	instanceID   string
}

func NewIngestSessions(mgr WriteLeaseManager, cacheDir string) *IngestSessions {
	if mgr == nil {
		mgr = NewInMemoryWriteLeaseManager()
	}
	return &IngestSessions{mgr: mgr, ttl: DefaultIngestSessionTTL, cacheDir: cacheDir}
}

// Instance resolves lazily, so a config check cannot claim an identity.
func (s *IngestSessions) Instance() string {
	s.instanceOnce.Do(func() { s.instanceID = instanceIdentity(s.cacheDir) })
	return s.instanceID
}

func (s *IngestSessions) leaseKey(kbID string) string { return ingestLeasePrefix + kbID }

// instanceIdentity survives a restart. Only a missing file mints a new one,
// since rotating on a read error would disown every live session.
func instanceIdentity(cacheDir string) string {
	if strings.TrimSpace(cacheDir) == "" {
		return randomIdentity()
	}
	path := filepath.Join(cacheDir, instanceIDFileName)
	switch data, err := os.ReadFile(path); {
	case err == nil:
		if id := usableIdentity(string(data)); id != "" {
			return id
		}
	case !errors.Is(err, os.ErrNotExist):
		return derivedIdentity(cacheDir)
	}
	id := randomIdentity()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return derivedIdentity(cacheDir)
	}
	if err := os.WriteFile(path, []byte(id), 0o600); err != nil {
		return derivedIdentity(cacheDir)
	}
	return id
}

// usableIdentity rejects a colon, the handle separator.
func usableIdentity(raw string) string {
	id := strings.TrimSpace(raw)
	if id == "" || strings.Contains(id, ":") {
		return ""
	}
	return id
}

func randomIdentity() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "instance"
	}
	return hex.EncodeToString(buf[:])
}

// derivedIdentity covers a data directory that cannot be read or written.
// Stable across restarts, which matters more than unique, though two machines
// mounting the same path do collide.
func derivedIdentity(cacheDir string) string {
	sum := sha256.Sum256([]byte(cacheDir))
	return hex.EncodeToString(sum[:8])
}

type ErrIngestSessionConflict struct{ kbID string }

func (e ErrIngestSessionConflict) Error() string {
	return fmt.Sprintf(
		"knowledge base %s has an uncommitted ingest session held by another client", e.kbID,
	)
}

type ErrIngestSessionElsewhere struct{ kbID, instance string }

func (e ErrIngestSessionElsewhere) Error() string {
	// The instance the handle claims, which is all this end knows.
	return fmt.Sprintf(
		"session handle for knowledge base %s belongs to instance %s: "+
			"every request in one session must reach the instance holding its rows",
		e.kbID, e.instance,
	)
}

// Hold opens a session, renews the caller's own, or takes over one whose lease
// lapsed. Takeover is safe because chunk IDs are content addressed.
func (s *IngestSessions) Hold(ctx context.Context, kbID, sessionID string) (string, error) {
	return s.hold(ctx, kbID, sessionID, false)
}

// Renew refuses anything but the session the caller still holds, so a commit
// cannot publish another client's rows once their lease lapses.
func (s *IngestSessions) Renew(ctx context.Context, kbID, sessionID string) (string, error) {
	return s.hold(ctx, kbID, sessionID, true)
}

func (s *IngestSessions) hold(ctx context.Context, kbID, sessionID string, ownedOnly bool) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	instance, token, ok := strings.Cut(sessionID, ":")
	// The rows are on that instance, lapsed lease or not, and a session split
	// across two local shards can publish neither half.
	if ok && instance != "" && instance != s.Instance() {
		return "", ErrIngestSessionElsewhere{kbID: kbID, instance: instance}
	}
	if ok && token != "" {
		_, err := s.mgr.Renew(ctx, &WriteLease{KBID: s.leaseKey(kbID), Token: token}, s.ttl)
		if err == nil {
			return sessionID, nil
		}
		if !errors.Is(err, ErrWriteLeaseConflict) {
			return "", err
		}
	}
	if ownedOnly {
		return "", ErrIngestSessionConflict{kbID: kbID}
	}
	held, err := s.mgr.Acquire(ctx, s.leaseKey(kbID), s.ttl)
	switch {
	case errors.Is(err, ErrWriteLeaseConflict):
		return "", ErrIngestSessionConflict{kbID: kbID}
	case err != nil:
		return "", err
	}
	return s.Instance() + ":" + held.Token, nil
}

// Peek reports the current holder, or nil. It takes and extends nothing.
func (s *IngestSessions) Peek(ctx context.Context, kbID string) (*WriteLease, error) {
	return s.mgr.Peek(ctx, s.leaseKey(kbID))
}

// Release frees the knowledge base so the next writer skips the TTL.
func (s *IngestSessions) Release(ctx context.Context, kbID, sessionID string) error {
	_, token, ok := strings.Cut(strings.TrimSpace(sessionID), ":")
	if !ok || token == "" {
		return nil
	}
	return s.mgr.Release(ctx, &WriteLease{KBID: s.leaseKey(kbID), Token: token})
}

// IngestSessionsFor returns the registry shared by the handler and the worker.
func (l *KB) IngestSessionsFor() *IngestSessions {
	l.ingestSessionsOnce.Do(func() {
		l.ingestSessions = NewIngestSessions(l.WriteLeaseManager, l.CacheDir)
	})
	return l.ingestSessions
}

// ReapAbandonedSessions publishes deferred rows whose client never came back.
// The lease is the liveness signal, so taking it means the rows are complete
// as of the last batch. Nothing else clears a crashed run's marker.
func (l *KB) ReapAbandonedSessions(ctx context.Context) (int, error) {
	if l.CacheDir == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(l.CacheDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	sessions := l.IngestSessionsFor()
	reaped := 0
	var failed []string
	for _, entry := range entries {
		if !entry.IsDir() || cacheevict.IsControlEntry(entry.Name()) {
			continue
		}
		kbID := entry.Name()
		if !HasPendingSession(filepath.Join(l.CacheDir, kbID)) {
			continue
		}
		// A conflict is a client still writing. Anything else is the backend.
		handle, err := sessions.Hold(ctx, kbID, "")
		if err != nil {
			var conflict ErrIngestSessionConflict
			if !errors.As(err, &conflict) {
				slog.Default().WarnContext(ctx, "could not check an ingest session for reaping",
					logKeyKBID, kbID, logKeyError, err)
				failed = append(failed, kbID)
			}
			continue
		}
		if err := l.CommitPreparedDocs(ctx, kbID); err != nil {
			slog.Default().WarnContext(ctx, "abandoned ingest session was not published",
				logKeyKBID, kbID, logKeyError, err)
			failed = append(failed, kbID)
		} else {
			slog.Default().InfoContext(ctx, "published an abandoned ingest session", logKeyKBID, kbID)
			reaped++
		}
		_ = sessions.Release(ctx, kbID, handle)
	}
	// So a tick that failed everything is not recorded as a clean run.
	if len(failed) > 0 {
		return reaped, fmt.Errorf("could not publish abandoned sessions for %v", failed)
	}
	return reaped, nil
}
