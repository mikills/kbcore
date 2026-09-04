package memlimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPollNotifier(t *testing.T) {
	t.Run("waits the poll interval when asked to block", func(t *testing.T) {
		p := newPollNotifier()
		defer func() { _ = p.Close() }()
		start := time.Now()
		require.NoError(t, p.Wait(-1))
		// A blocking wait must not return at once, or the governor spins.
		require.GreaterOrEqual(t, time.Since(start), PollInterval-50*time.Millisecond)
	})

	t.Run("a shorter timeout wins", func(t *testing.T) {
		p := newPollNotifier()
		defer func() { _ = p.Close() }()
		start := time.Now()
		require.NoError(t, p.Wait(100))
		require.Less(t, time.Since(start), PollInterval)
	})

	t.Run("interrupt ends a blocked wait", func(t *testing.T) {
		p := newPollNotifier()
		done := make(chan error, 1)
		go func() { done <- p.Wait(-1) }()
		time.Sleep(20 * time.Millisecond)
		p.Interrupt()
		select {
		case err := <-done:
			require.ErrorIs(t, err, errClosed)
		case <-time.After(PollInterval):
			t.Fatal("Interrupt did not wake the waiter")
		}
		require.NoError(t, p.Close(), "closing after an interrupt must be safe")
	})

	t.Run("nothing is enforced by looking", func(t *testing.T) {
		require.False(t, newPollNotifier().Enforced())
		require.Equal(t, "poll", newPollNotifier().Source())
	})
}
