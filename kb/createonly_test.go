package kb

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// genericOnlyStore hides the conditional primitives (UploadBytesIfNotExists,
// *LocalBlobStore) behind the plain Store interface, forcing the generic
// lease-fenced Head-then-put fallback in uploadBytesCreateOnly.
type genericOnlyStore struct{ BlobStore }

func TestConcurrentCreateCrossProcess(t *testing.T) {
	t.Run("shared_store_single_winner", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		shared := &LocalBlobStore{Root: root}
		sharedLeases := NewInMemoryWriteLeaseManager()

		newInstance := func() *KB {
			k := NewKB(genericOnlyStore{BlobStore: shared}, t.TempDir())
			k.WriteLeaseManager = sharedLeases
			return k
		}
		kbA, kbB := newInstance(), newInstance()
		advSeedKB(t, kbA, "kb", map[string]string{"s": "x"})

		const racers = 8
		errs := make([]error, racers)
		var wg sync.WaitGroup
		for i := range racers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i%2 == 0 {
					_, errs[i] = kbA.CreateBackup(ctx, "kb", "b1")
				} else {
					_, errs[i] = kbB.CreateBackup(ctx, "kb", "b1")
				}
			}(i)
		}
		wg.Wait()
		wins := 0
		for _, err := range errs {
			if err == nil {
				wins++
				continue
			}
			require.ErrorIs(t, err, ErrBackupExists)
			require.ErrorIs(t, err, ErrBlobVersionMismatch)
		}
		require.Equal(t, 1, wins, "exactly one cross-process create must win")
		got, err := kbA.GetBackup(ctx, "kb", "b1")
		require.NoError(t, err)
		require.NoError(t, ValidateBackupDescriptor(got))
	})
}
