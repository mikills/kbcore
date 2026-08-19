package scenarios

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/blobstore/localjournal"
	"github.com/mikills/minnow/kb/blobstore/tiered"
	"github.com/mikills/minnow/sim"
	"github.com/stretchr/testify/require"
)

// TieredStorageRecovery drives the real persistent journal and tiered worker
// through a seeded workload of writes, deletes, remote outages, and restarts.
// Workload choices are deterministic; background replication scheduling is not.
func TieredStorageRecovery(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	remote := sim.NewReplicaStore()
	journalDir := t.TempDir()
	model := make(map[string][]byte)
	store := startTieredStore(t, ctx, remote, journalDir)
	t.Cleanup(func() {
		remote.SetUnavailable(false)
		_ = store.Stop(context.Background())
	})

	for step := 0; step < 40; step++ {
		key := fmt.Sprintf("kb-%d/object-%d", rng.Intn(3), rng.Intn(8))
		switch rng.Intn(10) {
		case 0, 1:
			remote.SetUnavailable(true)
		case 2:
			remote.SetUnavailable(false)
		case 3:
			// A downed remote makes Stop fail to release the ownership claim and
			// leaves the store open on purpose, so shutdown can be retried. The
			// journal stays locked until it succeeds, so retry before restarting.
			if err := store.Stop(ctx); err != nil {
				require.ErrorIsf(t, err, sim.ErrReplicaUnavailable, "seed=%d step=%d restart stop", seed, step)
				remote.SetUnavailable(false)
				require.NoErrorf(t, store.Stop(ctx), "seed=%d step=%d restart stop retry", seed, step)
			} else {
				remote.SetUnavailable(false)
			}
			store = startTieredStore(t, ctx, remote, journalDir)
		case 4, 5:
			_, existed := model[key]
			require.NoErrorf(t, store.Delete(ctx, key), "seed=%d step=%d delete=%s", seed, step, key)
			delete(model, key)
			if existed {
				_, err := store.DownloadBytes(ctx, key)
				require.ErrorIsf(t, err, blobstore.ErrNotFound, "seed=%d step=%d deleted key remained visible=%s", seed, step, key)
			}
		default:
			value := []byte(fmt.Sprintf("seed=%d step=%d value=%d", seed, step, rng.Int63()))
			_, err := store.UploadBytesIfMatch(ctx, key, value, "")
			require.NoErrorf(t, err, "seed=%d step=%d put=%s", seed, step, key)
			model[key] = append([]byte(nil), value...)
			actual, readErr := store.DownloadBytes(ctx, key)
			require.NoErrorf(t, readErr, "seed=%d step=%d local read=%s", seed, step, key)
			require.Equalf(t, value, actual, "seed=%d step=%d local visibility=%s", seed, step, key)
		}
	}

	remote.SetUnavailable(false)
	drainTieredStore(t, ctx, seed, store)
	for key, expected := range model {
		actual, err := remote.DownloadBytes(ctx, key)
		require.NoErrorf(t, err, "seed=%d key=%s missing remotely", seed, key)
		require.Equalf(t, expected, actual, "seed=%d key=%s stale remotely", seed, key)
	}
	remoteObjects, err := remote.List(ctx, "")
	require.NoError(t, err)
	require.Lenf(t, remoteObjects, len(model), "seed=%d remote contains resurrected or deleted objects", seed)
	require.NoError(t, store.Stop(ctx))
}

func startTieredStore(t *testing.T, ctx context.Context, remote *sim.ReplicaStore, journalDir string) *tiered.Store {
	t.Helper()
	store, err := tiered.New(remote, localjournal.New(journalDir, journal.Config{
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

func drainTieredStore(t *testing.T, ctx context.Context, seed int64, store *tiered.Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := store.Stats(ctx)
		if err == nil && stats.PendingEntries == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed=%d stats=%+v stats_error=%v replication_error=%v", seed, stats, err, store.ReplicationError())
		}
		if err != nil && !errors.Is(err, journal.ErrClosed) {
			t.Logf("seed=%d waiting for tiered drain: %v", seed, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
