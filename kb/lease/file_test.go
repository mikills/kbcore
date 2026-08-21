package lease

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newFileManagerAt(t *testing.T, dir string, clock *stubClock) *FileManager {
	t.Helper()
	mgr, err := NewFileManager(dir)
	require.NoError(t, err)
	mgr.SetClock(clock)
	return mgr
}

func TestFileManager(t *testing.T) {
	ctx := context.Background()

	t.Run("a second caller is refused while the lease is live", func(t *testing.T) {
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, t.TempDir(), clock)
		held, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		_, err = mgr.Acquire(ctx, "kb", time.Minute)
		require.ErrorIs(t, err, ErrConflict)
		renewed, err := mgr.Renew(ctx, held, time.Minute)
		require.NoError(t, err)
		require.Equal(t, held.Token, renewed.Token)
		require.NoError(t, mgr.Release(ctx, renewed))
		_, err = mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
	})

	t.Run("the lease outlives the process that took it", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		held, err := newFileManagerAt(t, dir, clock).Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)

		restarted := newFileManagerAt(t, dir, clock)
		_, err = restarted.Acquire(ctx, "kb", time.Minute)
		require.ErrorIs(t, err, ErrConflict)
		_, err = restarted.Renew(ctx, held, time.Minute)
		require.NoError(t, err)
	})

	t.Run("an abandoned lease is taken over after it expires", func(t *testing.T) {
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, t.TempDir(), clock)
		abandoned, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		clock.t = clock.t.Add(2 * time.Minute)
		taken, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		require.NotEqual(t, abandoned.Token, taken.Token)
		_, err = mgr.Renew(ctx, abandoned, time.Minute)
		require.ErrorIs(t, err, ErrConflict)
	})

	t.Run("a superseded holder cannot release the new one", func(t *testing.T) {
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, t.TempDir(), clock)
		abandoned, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		clock.t = clock.t.Add(2 * time.Minute)
		taken, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		require.NoError(t, mgr.Release(ctx, abandoned))
		_, err = mgr.Renew(ctx, taken, time.Minute)
		require.NoError(t, err)
	})

	t.Run("keys that escape to the same name stay apart", func(t *testing.T) {
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, t.TempDir(), clock)
		_, err := mgr.Acquire(ctx, "scheduler:job/one", time.Minute)
		require.NoError(t, err)
		_, err = mgr.Acquire(ctx, "scheduler:job|one", time.Minute)
		require.NoError(t, err)
	})

	t.Run("a key never escapes the lease directory", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, dir, clock)
		_, err := mgr.Acquire(ctx, "../../escaped", time.Minute)
		require.NoError(t, err)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, dir, filepath.Dir(mgr.path("../../escaped")))
	})

	t.Run("the directory is not created until a lease is taken", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "leases")
		mgr, err := NewFileManager(dir)
		require.NoError(t, err)
		// Building a manager during a config check must not touch the volume.
		require.NoDirExists(t, dir)
		_, err = mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		require.DirExists(t, dir)
	})

	t.Run("two processes on one directory cannot both hold the lease", func(t *testing.T) {
		dir := t.TempDir()
		const racers = 8
		var wg sync.WaitGroup
		results := make([]error, racers)
		start := make(chan struct{})
		for i := range racers {
			// A separate manager per goroutine stands in for a separate process:
			// the in-process mutex cannot serialise them.
			mgr := newFileManagerAt(t, dir, &stubClock{t: time.Now().UTC()})
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, results[i] = mgr.Acquire(ctx, "kb", time.Minute)
			}()
		}
		close(start)
		wg.Wait()

		winners := 0
		for _, err := range results {
			if err == nil {
				winners++
				continue
			}
			require.ErrorIs(t, err, ErrConflict)
		}
		require.Equal(t, 1, winners, "more than one caller was granted the same lease")
	})

	t.Run("two processes cannot both take over the same expired lease", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		seed := newFileManagerAt(t, dir, clock)
		if _, err := seed.Acquire(ctx, "kb", time.Minute); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(2 * time.Minute)

		const racers = 8
		var wg sync.WaitGroup
		results := make([]error, racers)
		start := make(chan struct{})
		for i := range racers {
			// Separate managers stand in for separate processes: replacing the lapsed
			// record is a read-modify-write that no in-process mutex covers.
			mgr := newFileManagerAt(t, dir, clock)
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, results[i] = mgr.Acquire(ctx, "kb", time.Minute)
			}()
		}
		close(start)
		wg.Wait()

		winners := 0
		for _, err := range results {
			if err == nil {
				winners++
				continue
			}
			require.ErrorIs(t, err, ErrConflict)
		}
		require.Equal(t, 1, winners, "more than one caller took over the same expired lease")
	})

	t.Run("a takeover cannot replace a record that went live meanwhile", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		slow := newFileManagerAt(t, dir, clock)
		fast := newFileManagerAt(t, dir, clock)
		if _, err := slow.Acquire(ctx, "kb", time.Minute); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(2 * time.Minute)

		// Both see the lapsed record. The other process gets there first, so
		// this one must not remove the live record it left behind.
		var winner *Lease
		slow.beforeEvict = func() {
			slow.beforeEvict = nil
			taken, err := fast.Acquire(ctx, "kb", time.Minute)
			require.NoError(t, err)
			winner = taken
		}
		_, err := slow.Acquire(ctx, "kb", time.Minute)
		require.ErrorIs(t, err, ErrConflict, "a stale view of the record stole a live lease")

		seen := mustPeek(t, fast, "kb")
		require.NotNil(t, seen, "the live record was removed by the loser")
		require.Equal(t, winner.Token, seen.Token)
	})

	t.Run("a takeover abandoned mid-flight does not strand the lease", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, dir, clock)
		if _, err := mgr.Acquire(ctx, "kb", time.Minute); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(2 * time.Minute)
		// A caller that died between claiming the marker and removing it.
		marker := mgr.path("kb") + takeoverSuffix
		live := encodeLockPayload("takeover", clock.t.Add(takeoverTimeout))
		require.NoError(t, os.WriteFile(marker, live, 0o600))
		_, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.ErrorIs(t, err, ErrConflict, "a live takeover was ignored")
		// Both outcomes refuse. Only the marker says whether the live takeover
		// was honoured or cleared out from under the caller still inside it.
		held, readErr := os.ReadFile(marker)
		require.NoError(t, readErr, "a live takeover marker was removed")
		require.Equal(t, live, held, "a live takeover marker was replaced")

		expired := encodeLockPayload("takeover", clock.t.Add(-time.Minute))
		require.NoError(t, os.WriteFile(marker, expired, 0o600))
		_, err = mgr.Acquire(ctx, "kb", time.Minute)
		require.ErrorIs(t, err, ErrConflict, "the stale marker is reclaimed, not used")
		_, err = mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err, "a reclaimed marker did not free the lease")
	})

	t.Run("a release that lapses mid-write does not remove the new holder", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		holder := newFileManagerAt(t, dir, clock)
		other := newFileManagerAt(t, dir, clock)

		held, err := holder.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)

		// The release reads its own record, then the lease lapses and another
		// process takes it.
		var stolen error
		holder.beforeReleaseWrite = func() {
			clock.t = clock.t.Add(2 * time.Minute)
			_, stolen = other.Acquire(ctx, "kb", time.Minute)
		}
		require.NoError(t, holder.Release(ctx, held))
		holder.beforeReleaseWrite = nil

		require.ErrorIs(t, stolen, ErrConflict, "a takeover ran inside another caller's release")

		taken, err := other.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		seen := mustPeek(t, other, "kb")
		require.NotNil(t, seen, "the new holder's record was removed by the old one")
		require.Equal(t, taken.Token, seen.Token)
	})

	t.Run("a takeover only clears the marker it created", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, dir, clock)
		unlock, err := mgr.lockTakeover("kb", clock.t)
		require.NoError(t, err)

		// The first caller's marker lapsed and a second one replaced it.
		marker := mgr.path("kb") + takeoverSuffix
		other := encodeLockPayload("someone-else", clock.t.Add(takeoverTimeout))
		require.NoError(t, os.WriteFile(marker, other, 0o600))
		unlock()

		held, readErr := os.ReadFile(marker)
		require.NoError(t, readErr, "a takeover removed another caller's marker")
		require.Equal(t, other, held)
	})

	t.Run("peek reports the holder without extending it", func(t *testing.T) {
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, t.TempDir(), clock)
		require.Nil(t, mustPeek(t, mgr, "kb"))

		held, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		seen := mustPeek(t, mgr, "kb")
		require.NotNil(t, seen)
		require.Equal(t, held.Token, seen.Token)

		clock.t = clock.t.Add(2 * time.Minute)
		require.Nil(t, mustPeek(t, mgr, "kb"), "an expired lease was reported as held")
	})

	t.Run("an unreadable record is not a free lease", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, dir, clock)
		// A record that cannot be read says nothing about who holds the lease,
		// so granting a new one would double-grant it.
		require.NoError(t, os.MkdirAll(mgr.path("kb"), 0o755))
		_, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrConflict)
	})

	t.Run("a truncated record does not strand the knowledge base", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		mgr := newFileManagerAt(t, dir, clock)
		held, err := mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(mgr.path("kb"), []byte("garbage"), 0o600))
		_, err = mgr.Renew(ctx, held, time.Minute)
		require.ErrorIs(t, err, ErrConflict)
		_, err = mgr.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
	})

	t.Run("a renewal that lapses mid-write does not take the lease back", func(t *testing.T) {
		dir := t.TempDir()
		clock := &stubClock{t: time.Now().UTC()}
		holder := newFileManagerAt(t, dir, clock)
		other := newFileManagerAt(t, dir, clock)

		held, err := holder.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)

		// The renewal reads a live record, then the lease lapses and another process
		// takes it.
		var stolen error
		holder.beforeRenewWrite = func() {
			clock.t = clock.t.Add(2 * time.Minute)
			_, stolen = other.Acquire(ctx, "kb", time.Minute)
		}
		_, _ = holder.Renew(ctx, held, time.Minute)
		holder.beforeRenewWrite = nil

		require.ErrorIs(t, stolen, ErrConflict, "a takeover ran inside another caller's renewal")

		taken, err := other.Acquire(ctx, "kb", time.Minute)
		require.NoError(t, err)
		require.Equal(t, taken.Token, mustPeek(t, other, "kb").Token,
			"a stale renewal survived the takeover")
	})
}

type stubClock struct{ t time.Time }

func (c *stubClock) Now() time.Time { return c.t }

func mustPeek(t *testing.T, mgr *FileManager, kbID string) *Lease {
	t.Helper()
	seen, err := mgr.Peek(context.Background(), kbID)
	require.NoError(t, err)
	return seen
}
