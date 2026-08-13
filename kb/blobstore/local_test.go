package blobstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlobLocal(t *testing.T) {
	t.Run("upload_if_match", testBlobLocalUploadIfMatch)
	t.Run("upload_if_match_concurrent", testBlobLocalUploadIfMatchConcurrent)
	t.Run("upload_if_not_exists_cross_instance", testBlobLocalUploadIfNotExistsCrossInstance)
	t.Run("delete", testBlobLocalDelete)
	t.Run("list", testBlobLocalList)
}

func testBlobLocalUploadIfMatch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cacheDir := t.TempDir()
	store := &LocalBlobStore{Root: root}

	srcV1 := filepath.Join(cacheDir, "src-v1.duckdb")
	require.NoError(t, os.WriteFile(srcV1, []byte("v1"), 0o644))

	objV1, err := store.UploadIfMatch(ctx, "kb.duckdb", srcV1, "")
	require.NoError(t, err)
	require.NotEmpty(t, objV1.Version)

	srcV2 := filepath.Join(cacheDir, "src-v2.duckdb")
	require.NoError(t, os.WriteFile(srcV2, []byte("v2"), 0o644))

	_, err = store.UploadIfMatch(ctx, "kb.duckdb", srcV2, "stale-version")
	require.ErrorIs(t, err, ErrVersionMismatch)

	objV2, err := store.UploadIfMatch(ctx, "kb.duckdb", srcV2, objV1.Version)
	require.NoError(t, err)
	require.NotEqual(t, objV1.Version, objV2.Version)

	content, err := os.ReadFile(filepath.Join(store.Root, "kb.duckdb"))
	require.NoError(t, err)
	require.Equal(t, "v2", string(content))
}

func testBlobLocalUploadIfMatchConcurrent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cacheDir := t.TempDir()
	store := &LocalBlobStore{Root: root}

	srcV1 := filepath.Join(cacheDir, "src-v1.duckdb")
	require.NoError(t, os.WriteFile(srcV1, []byte("v1"), 0o644))
	objV1, err := store.UploadIfMatch(ctx, "kb.duckdb", srcV1, "")
	require.NoError(t, err)

	srcA := filepath.Join(cacheDir, "src-a.duckdb")
	srcB := filepath.Join(cacheDir, "src-b.duckdb")
	require.NoError(t, os.WriteFile(srcA, []byte("v2-a"), 0o644))
	require.NoError(t, os.WriteFile(srcB, []byte("v2-b"), 0o644))

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)

	go func() {
		defer wg.Done()
		_, upErr := store.UploadIfMatch(ctx, "kb.duckdb", srcA, objV1.Version)
		errCh <- upErr
	}()
	go func() {
		defer wg.Done()
		_, upErr := store.UploadIfMatch(ctx, "kb.duckdb", srcB, objV1.Version)
		errCh <- upErr
	}()

	wg.Wait()
	close(errCh)

	var successCount int
	var conflictCount int
	for upErr := range errCh {
		switch {
		case upErr == nil:
			successCount++
		case errors.Is(upErr, ErrVersionMismatch):
			conflictCount++
		default:
			require.NoError(t, upErr)
		}
	}
	require.Equal(t, 1, successCount)
	require.Equal(t, 1, conflictCount)
}

func testBlobLocalUploadIfNotExistsCrossInstance(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	srcDir := t.TempDir()
	first := &LocalBlobStore{Root: root}
	second := &LocalBlobStore{Root: root}
	srcA := filepath.Join(srcDir, "a")
	srcB := filepath.Join(srcDir, "b")
	require.NoError(t, os.WriteFile(srcA, []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(srcB, []byte("b"), 0o644))

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := first.UploadIfNotExists(ctx, "staging/object", srcA)
		errs <- err
	}()
	go func() {
		<-start
		_, err := second.UploadIfNotExists(ctx, "staging/object", srcB)
		errs <- err
	}()
	close(start)
	var successes, conflicts int
	for range 2 {
		err := <-errs
		if err == nil {
			successes++
		} else if errors.Is(err, ErrVersionMismatch) {
			conflicts++
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
}

func testBlobLocalDelete(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := &LocalBlobStore{Root: root}
	key := "nested/object.txt"
	path := filepath.Join(root, key)
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
	root := t.TempDir()
	store := &LocalBlobStore{Root: root}
	fixtures := map[string]string{
		"a.txt":     "a",
		"dir/b.txt": "bb",
		"dir/c.log": "ccc",
	}

	for key, contents := range fixtures {
		path := filepath.Join(root, key)
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
