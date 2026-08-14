package tiered_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/blobstore/localjournal"
	"github.com/mikills/minnow/kb/blobstore/tiered"
	"github.com/stretchr/testify/require"
)

// TestTieredSeededRecoveryWorkload exercises the real persistent journal with
// seeded writes, deletes, S3 outages, and graceful stop/reopen cycles. The
// workload is reproducible, while replication scheduling remains deliberately
// concurrent; crash-boundary coverage lives in localjournal tests.
func TestTieredSeededRecoveryWorkload(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 100} {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ctx := context.Background()
			rng := rand.New(rand.NewSource(seed))
			remote := newMemoryRemote()
			journalDir := t.TempDir()
			model := map[string][]byte{}
			store := startSimStore(t, ctx, remote, journalDir)

			for step := 0; step < 40; step++ {
				key := fmt.Sprintf("kb-%d/object-%d", rng.Intn(3), rng.Intn(8))
				switch rng.Intn(10) {
				case 0, 1:
					remote.setUnavailable(true)
				case 2:
					remote.setUnavailable(false)
				case 3:
					require.NoErrorf(t, store.Stop(ctx), "seed=%d step=%d restart stop", seed, step)
					remote.mu.Lock()
					wasUnavailable := remote.unavailable
					remote.unavailable = false
					remote.mu.Unlock()
					store = startSimStore(t, ctx, remote, journalDir)
					remote.setUnavailable(wasUnavailable)
				case 4, 5:
					require.NoErrorf(t, store.Delete(ctx, key), "seed=%d step=%d delete=%s", seed, step, key)
					delete(model, key)
				default:
					value := []byte(fmt.Sprintf("seed=%d step=%d value=%d", seed, step, rng.Int63()))
					_, err := store.UploadBytesIfMatch(ctx, key, value, "")
					require.NoErrorf(t, err, "seed=%d step=%d put=%s", seed, step, key)
					model[key] = append([]byte(nil), value...)
				}
			}

			remote.setUnavailable(false)
			deadline := time.Now().Add(5 * time.Second)
			for {
				stats, statErr := store.Stats(ctx)
				if statErr == nil && stats.PendingEntries == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("seed=%d stats=%+v stats_error=%v replication_error=%v", seed, stats, statErr, store.ReplicationError())
				}
				time.Sleep(5 * time.Millisecond)
			}
			for key, expected := range model {
				actual, err := remote.DownloadBytes(ctx, key)
				require.NoErrorf(t, err, "seed=%d key=%s missing remotely", seed, key)
				require.Equalf(t, expected, actual, "seed=%d key=%s stale remotely", seed, key)
			}
			remoteObjects, err := remote.List(ctx, "")
			require.NoError(t, err)
			require.Lenf(t, remoteObjects, len(model), "seed=%d remote contains resurrected/deleted objects", seed)
			require.NoError(t, store.Stop(ctx))
		})
	}
}

func startSimStore(t *testing.T, ctx context.Context, remote *memoryRemote, dir string) *tiered.Store {
	t.Helper()
	store, err := tiered.New(remote, localjournal.New(dir, journal.Config{
		MaxPendingEntries: 1000,
		MaxPendingBytes:   16 << 20,
	}), tiered.Config{
		Durability:   tiered.DurabilityLocalJournal,
		PollInterval: time.Millisecond,
		RetryBase:    time.Millisecond,
		RetryMax:     10 * time.Millisecond,
		MaxAttempts:  1000,
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	return store
}
