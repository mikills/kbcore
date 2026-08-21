package kb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/lease"
)

type sessionClock struct{ t time.Time }

func (c *sessionClock) Now() time.Time { return c.t }

func newTestSessions(t *testing.T) (*IngestSessions, *sessionClock) {
	t.Helper()
	clock := &sessionClock{t: time.Now().UTC()}
	mgr := lease.NewInMemoryManager()
	mgr.SetClock(clock)
	return NewIngestSessions(mgr, t.TempDir()), clock
}

func TestIngestSessionHold(t *testing.T) {
	ctx := context.Background()

	t.Run("the issued handle names the instance that holds the writes", func(t *testing.T) {
		s, _ := newTestSessions(t)
		handle, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		instance, token, ok := strings.Cut(handle, ":")
		if !ok || token == "" {
			t.Fatalf("handle %q does not name an instance", handle)
		}
		if instance != s.Instance() {
			t.Fatalf("handle names instance %q, want %q", instance, s.Instance())
		}
	})

	t.Run("a second client cannot write while another session is live", func(t *testing.T) {
		s, clock := newTestSessions(t)
		if _, err := s.Hold(ctx, "kb", ""); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(s.ttl - time.Second)
		var conflict ErrIngestSessionConflict
		if _, err := s.Hold(ctx, "kb", ""); !errors.As(err, &conflict) {
			t.Fatalf("second client was not rejected: %v", err)
		}
	})

	t.Run("the holder keeps writing and renews its own session", func(t *testing.T) {
		s, clock := newTestSessions(t)
		handle, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		for range 3 {
			clock.t = clock.t.Add(s.ttl - time.Second)
			again, err := s.Hold(ctx, "kb", handle)
			if err != nil {
				t.Fatalf("holder was rejected: %v", err)
			}
			if again != handle {
				t.Fatalf("holder was issued a new handle: %q then %q", handle, again)
			}
		}
		if _, err := s.Hold(ctx, "kb", ""); err == nil {
			t.Fatal("renewing did not keep the session live")
		}
	})

	t.Run("an abandoned session is taken over after the ttl", func(t *testing.T) {
		s, clock := newTestSessions(t)
		abandoned, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(s.ttl + time.Second)
		taken, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatalf("idle session was not taken over: %v", err)
		}
		if taken == abandoned {
			t.Fatal("takeover reused the abandoned handle")
		}
	})

	t.Run("a lapsed handle is renewed into a fresh session rather than refused", func(t *testing.T) {
		s, clock := newTestSessions(t)
		handle, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(s.ttl + time.Second)
		reopened, err := s.Hold(ctx, "kb", handle)
		if err != nil {
			t.Fatalf("the client was locked out of its own lapsed session: %v", err)
		}
		if reopened == handle {
			t.Fatal("a lapsed lease was reported as still held")
		}
	})

	t.Run("sessions do not span knowledge bases", func(t *testing.T) {
		s, _ := newTestSessions(t)
		if _, err := s.Hold(ctx, "kb-a", ""); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Hold(ctx, "kb-b", ""); err != nil {
			t.Fatalf("unrelated knowledge base was blocked: %v", err)
		}
	})
}

// The rows a session defers sit on one instance's disk, so any other instance
// has to refuse rather than write into a copy it cannot publish.
func TestIngestSessionAcrossInstances(t *testing.T) {
	ctx := context.Background()

	t.Run("an instance refuses a live session that belongs to another", func(t *testing.T) {
		clock := &sessionClock{t: time.Now().UTC()}
		mgr := lease.NewInMemoryManager()
		mgr.SetClock(clock)
		first := NewIngestSessions(mgr, t.TempDir())
		second := NewIngestSessions(mgr, t.TempDir())

		handle, err := first.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		var elsewhere ErrIngestSessionElsewhere
		if _, err := second.Hold(ctx, "kb", handle); !errors.As(err, &elsewhere) {
			t.Fatalf("a foreign session was accepted: %v", err)
		}
		if elsewhere.instance != first.Instance() {
			t.Fatalf("error named instance %q, want %q", elsewhere.instance, first.Instance())
		}
	})

	t.Run("a lapsed lease does not move a foreign session to this instance", func(t *testing.T) {
		clock := &sessionClock{t: time.Now().UTC()}
		mgr := lease.NewInMemoryManager()
		mgr.SetClock(clock)
		first := NewIngestSessions(mgr, t.TempDir())
		second := NewIngestSessions(mgr, t.TempDir())

		handle, err := first.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(first.ttl + time.Second)
		// The lease lapsing says nothing about where the rows are. Handing the
		// session to this instance would split it across two local shards, and
		// whichever committed last would publish a snapshot missing the other.
		var elsewhere ErrIngestSessionElsewhere
		if _, err := second.Hold(ctx, "kb", handle); !errors.As(err, &elsewhere) {
			t.Fatalf("a lapsed foreign session was adopted here: %v", err)
		}
		// The instance that owns the rows still resumes its own session.
		if _, err := first.Hold(ctx, "kb", handle); err != nil {
			t.Fatalf("the holding instance could not resume: %v", err)
		}
	})

	t.Run("an unwritable data directory still yields one stable identity", func(t *testing.T) {
		blocked := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		cacheDir := filepath.Join(blocked, "cache")

		mgr := lease.NewInMemoryManager()
		first := NewIngestSessions(mgr, cacheDir)
		second := NewIngestSessions(mgr, cacheDir)
		if first.Instance() == "" {
			t.Fatal("no identity was derived")
		}
		// A rotating identity would make the instance read its own live session
		// as another's and refuse to commit it.
		if first.Instance() != second.Instance() {
			t.Fatalf("identity rotated: %q then %q", first.Instance(), second.Instance())
		}
	})

	t.Run("the instance identity survives a restart", func(t *testing.T) {
		dir := t.TempDir()
		mgr := lease.NewInMemoryManager()
		before := NewIngestSessions(mgr, dir)
		handle, err := before.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		// A restart that minted a new identity would read its own live session
		// as another instance's and refuse to commit it.
		after := NewIngestSessions(mgr, dir)
		if _, err := after.Hold(ctx, "kb", handle); err != nil {
			t.Fatalf("a restart disowned its own session: %v", err)
		}
	})
}

func TestIngestSessionRelease(t *testing.T) {
	ctx := context.Background()

	t.Run("releasing frees the knowledge base for the next writer", func(t *testing.T) {
		s, _ := newTestSessions(t)
		handle, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Release(ctx, "kb", handle); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Hold(ctx, "kb", ""); err != nil {
			t.Fatalf("released knowledge base still blocked: %v", err)
		}
	})

	t.Run("a superseded writer cannot free the session that replaced it", func(t *testing.T) {
		s, clock := newTestSessions(t)
		stale, err := s.Hold(ctx, "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(s.ttl + time.Second)
		if _, err := s.Hold(ctx, "kb", ""); err != nil {
			t.Fatal(err)
		}
		if err := s.Release(ctx, "kb", stale); err != nil {
			t.Fatal(err)
		}
		var conflict ErrIngestSessionConflict
		if _, err := s.Hold(ctx, "kb", ""); !errors.As(err, &conflict) {
			t.Fatalf("a stale release freed the live session: %v", err)
		}
	})

	t.Run("releasing an unheld knowledge base is not an error", func(t *testing.T) {
		s, _ := newTestSessions(t)
		if err := s.Release(ctx, "kb", "instance:token"); err != nil {
			t.Fatalf("release without a session: %v", err)
		}
	})
}
