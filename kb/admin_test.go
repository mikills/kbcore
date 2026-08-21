package kb_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/require"
)

func TestDeleteKnowledgeBase(t *testing.T) {
	t.Run("rejects empty kb id", func(t *testing.T) {
		k := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir())
		err := k.DeleteKnowledgeBase(context.Background(), "")
		require.ErrorContains(t, err, "kb_id required")
	})

	t.Run("deletes manifest first then keeps going on cleanup errors", func(t *testing.T) {
		ctx := context.Background()
		blobRoot := t.TempDir()
		cacheDir := t.TempDir()

		shardKey := "shards/kb-1/seg-1.duckdb"
		shardPath := filepath.Join(blobRoot, shardKey)
		require.NoError(t, os.MkdirAll(filepath.Dir(shardPath), 0o755))
		require.NoError(t, os.WriteFile(shardPath, []byte("body"), 0o644))

		manifestStore := newRecordingManifestStore(SnapshotShardManifest{
			KBID:   "kb-1",
			Shards: []SnapshotShardMetadata{{Key: shardKey}},
		})
		blobStore := &faultyBlobStore{
			BlobStore: &LocalBlobStore{Root: blobRoot},
			deleteErr: errors.New("blob delete unavailable"),
			failKeys:  map[string]bool{shardKey: true},
		}
		mediaStore := &faultyMediaStore{
			MediaStore: NewInMemoryMediaStore(),
			listErr:    errors.New("media list unreachable"),
		}

		k := NewKB(blobStore, cacheDir,
			WithManifestStore(manifestStore),
			WithMediaStore(mediaStore),
		)

		writeCacheBytes(t, cacheDir, "kb-1", 100)
		writeCacheBytes(t, cacheDir, "kb-other", 50)
		err := k.DeleteKnowledgeBase(ctx, "kb-1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "deleted with cleanup errors")
		require.Contains(t, err.Error(), "blob delete unavailable")
		require.Contains(t, err.Error(), "media list unreachable")

		require.True(t, manifestStore.deleted, "manifest must be deleted before cleanup")
		require.Equal(t, 1, manifestStore.deleteCount)

		_, err = os.Stat(filepath.Join(cacheDir, "kb-1"))
		require.True(t, os.IsNotExist(err), "kb-1 cache should be removed")
		_, err = os.Stat(filepath.Join(cacheDir, "kb-other"))
		require.NoError(t, err, "kb-other cache must remain")

		metrics := k.CacheEvictionMetricsSnapshot()
		require.Less(t, metrics.CacheBytesCurrent, int64(150), "metric must drop after delete")
	})

	t.Run("succeeds when no manifest present", func(t *testing.T) {
		k := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(),
			WithManifestStore(&recordingManifestStore{notFound: true}),
		)
		require.NoError(t, k.DeleteKnowledgeBase(context.Background(), "kb-missing"))
	})

	t.Run("errors when manifest store missing", func(t *testing.T) {
		k := &KB{}
		err := k.DeleteKnowledgeBase(context.Background(), "kb-1")
		require.ErrorContains(t, err, "manifest store is not configured")
	})
}

func writeCacheBytes(t *testing.T, cacheDir, kbID string, n int) {
	t.Helper()
	dir := filepath.Join(cacheDir, kbID)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "blob"), make([]byte, n), 0o644))
}

type faultyBlobStore struct {
	BlobStore
	deleteErr error
	failKeys  map[string]bool
}

func (f *faultyBlobStore) Delete(ctx context.Context, key string) error {
	if f.failKeys[key] {
		return f.deleteErr
	}
	return f.BlobStore.Delete(ctx, key)
}

type faultyMediaStore struct {
	MediaStore
	listErr error
}

func (f *faultyMediaStore) List(ctx context.Context, kbID, prefix, after string, limit int) (MediaPage, error) {
	if f.listErr != nil {
		return MediaPage{}, f.listErr
	}
	return f.MediaStore.List(ctx, kbID, prefix, after, limit)
}

type recordingManifestStore struct {
	manifest    *ManifestDocument
	notFound    bool
	deleted     bool
	deleteCount int
}

func newRecordingManifestStore(m SnapshotShardManifest) *recordingManifestStore {
	return &recordingManifestStore{manifest: &ManifestDocument{Manifest: m, Version: "v1"}}
}

func (r *recordingManifestStore) Get(ctx context.Context, kbID string) (*ManifestDocument, error) {
	if r.notFound {
		return nil, ErrManifestNotFound
	}
	return r.manifest, nil
}

func (r *recordingManifestStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	return r.manifest.Version, nil
}

func (r *recordingManifestStore) UpsertIfMatch(
	ctx context.Context,
	kbID string,
	manifest SnapshotShardManifest,
	expectedVersion string,
) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (r *recordingManifestStore) Delete(ctx context.Context, kbID string) error {
	r.deleted = true
	r.deleteCount++
	return nil
}

func TestClearCache(t *testing.T) {
	t.Run("removes cached knowledge bases and keeps lease state", func(t *testing.T) {
		cacheDir := t.TempDir()
		kbPath := filepath.Join(cacheDir, "kb-a")
		require.NoError(t, os.MkdirAll(kbPath, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(kbPath, "blob"), []byte("abc"), 0o644))
		leasePath := filepath.Join(cacheDir, ".leases")
		require.NoError(t, os.MkdirAll(leasePath, 0o755))
		lockPath := filepath.Join(leasePath, "kb-a.lock")
		require.NoError(t, os.WriteFile(lockPath, []byte("held"), 0o600))
		instancePath := filepath.Join(cacheDir, ".instance-id")
		require.NoError(t, os.WriteFile(instancePath, []byte("instance-1"), 0o600))

		// Clearing is the operator's escape hatch for a knowledge base wedged by
		// an abandoned session, so it has to delete one that is still marked.
		require.NoError(t, MarkPendingSession(kbPath))

		k := NewKB(&LocalBlobStore{Root: t.TempDir()}, cacheDir)
		require.NoError(t, k.ClearCache())

		require.NoDirExists(t, kbPath)
		// Clearing cached shards must not drop the record of who is mid-write,
		// nor the identity that decides whose session this instance owns.
		require.FileExists(t, lockPath)
		require.FileExists(t, instancePath)
	})
}
