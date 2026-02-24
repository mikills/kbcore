package kb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlobLocal(t *testing.T) {
	t.Run("upload_if_match", testBlobLocalUploadIfMatch)
	t.Run("delete", testBlobLocalDelete)
	t.Run("list", testBlobLocalList)
}

func testBlobLocalUploadIfMatch(t *testing.T) {
	ctx := context.Background()
	harness := NewTestHarness(t, "kb-local-blob").Setup()
	defer harness.Cleanup()
	store := &LocalBlobStore{Root: harness.BlobRoot()}

	srcV1 := filepath.Join(harness.CacheDir(), "src-v1.duckdb")
	require.NoError(t, os.WriteFile(srcV1, []byte("v1"), 0o644))

	objV1, err := store.UploadIfMatch(ctx, "kb.duckdb", srcV1, "")
	require.NoError(t, err)
	require.NotEmpty(t, objV1.Version)

	srcV2 := filepath.Join(harness.CacheDir(), "src-v2.duckdb")
	require.NoError(t, os.WriteFile(srcV2, []byte("v2"), 0o644))

	_, err = store.UploadIfMatch(ctx, "kb.duckdb", srcV2, "stale-version")
	require.ErrorIs(t, err, ErrBlobVersionMismatch)

	objV2, err := store.UploadIfMatch(ctx, "kb.duckdb", srcV2, objV1.Version)
	require.NoError(t, err)
	require.NotEqual(t, objV1.Version, objV2.Version)

	content, err := os.ReadFile(filepath.Join(store.Root, "kb.duckdb"))
	require.NoError(t, err)
	require.Equal(t, "v2", string(content))
}

func testBlobLocalDelete(t *testing.T) {
	ctx := context.Background()
	harness := NewTestHarness(t, "kb-local-delete").Setup()
	defer harness.Cleanup()

	store := &LocalBlobStore{Root: harness.BlobRoot()}
	key := "nested/object.txt"
	path := filepath.Join(harness.BlobRoot(), key)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0o644))

	require.NoError(t, store.Delete(ctx, key))
	_, err := os.Stat(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, os.ErrNotExist))

	require.NoError(t, store.Delete(ctx, key))
}

func testBlobLocalList(t *testing.T) {
	ctx := context.Background()
	harness := NewTestHarness(t, "kb-local-list").Setup()
	defer harness.Cleanup()

	store := &LocalBlobStore{Root: harness.BlobRoot()}
	fixtures := map[string]string{
		"a.txt":     "a",
		"dir/b.txt": "bb",
		"dir/c.log": "ccc",
	}

	for key, contents := range fixtures {
		path := filepath.Join(harness.BlobRoot(), key)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
	}

	objects, err := store.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, objects, 3)

	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		keys = append(keys, obj.Key)
		require.NotEmpty(t, obj.Version)
		require.Greater(t, obj.Size, int64(0))
	}
	sort.Strings(keys)
	require.Equal(t, []string{"a.txt", "dir/b.txt", "dir/c.log"}, keys)

	filtered, err := store.List(ctx, "dir/")
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	require.Equal(t, "dir/b.txt", filtered[0].Key)
	require.Equal(t, "dir/c.log", filtered[1].Key)
}
