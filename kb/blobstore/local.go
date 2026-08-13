package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// LocalBlobStore implements BlobStore for local filesystem storage.
type LocalBlobStore struct {
	Root string

	keyLocks [256]sync.Mutex
}

func (l *LocalBlobStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(l.Root, key))
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", filepath.Join(l.Root, key), err)
	}
	return data, nil
}

func (l *LocalBlobStore) Download(ctx context.Context, key, dest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	src := filepath.Join(l.Root, key)
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("download %s: %w", src, err)
	}

	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("download %s: create %s: %w", src, dest, err)
	}

	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("download %s: copy to %s: %w", src, dest, err)
	}

	return nil
}

func (l *LocalBlobStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	path := filepath.Join(l.Root, key)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	version, err := FileContentSHA256(ctx, path)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:       key,
		Version:   version,
		UpdatedAt: info.ModTime().UTC(),
		Size:      info.Size(),
	}, nil
}

func (l *LocalBlobStore) UploadBytesIfMatch(
	ctx context.Context,
	key string,
	data []byte,
	expectedVersion string,
) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keyLock := l.lockForKey(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	dest := filepath.Join(l.Root, key)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}
	if err := l.checkUploadVersion(ctx, key, expectedVersion); err != nil {
		return nil, err
	}

	version := BytesSHA256(data)
	if err := replaceFileWithBytes(data, dest); err != nil {
		return nil, err
	}
	info, err := os.Stat(dest)
	if err != nil {
		return nil, err
	}
	return &ObjectInfo{Key: key, Version: version, UpdatedAt: info.ModTime().UTC(), Size: info.Size()}, nil
}

func (l *LocalBlobStore) UploadIfMatch(
	ctx context.Context,
	key string,
	src string,
	expectedVersion string,
) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keyLock := l.lockForKey(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	dest := filepath.Join(l.Root, key)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}

	if err := l.checkUploadVersion(ctx, key, expectedVersion); err != nil {
		return nil, err
	}

	srcHash, err := FileContentSHA256(ctx, src)
	if err != nil {
		return nil, err
	}

	if err := replaceFileWithCopy(src, dest); err != nil {
		return nil, err
	}

	// get file info without re-hashing
	info, err := os.Stat(dest)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{Key: key, Version: srcHash, UpdatedAt: info.ModTime().UTC(), Size: info.Size()}, nil
}

// UploadIfNotExists atomically creates key and rejects an existing object.
func (l *LocalBlobStore) UploadIfNotExists(ctx context.Context, key, src string) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keyLock := l.lockForKey(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	dest := filepath.Join(l.Root, key)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}
	srcHash, err := FileContentSHA256(ctx, src)
	if err != nil {
		return nil, err
	}
	in, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("%w: object already exists at %s", ErrVersionMismatch, key)
	}
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dest)
		return nil, err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dest)
		return nil, err
	}
	info, err := os.Stat(dest)
	if err != nil {
		return nil, err
	}
	return &ObjectInfo{Key: key, Version: srcHash, UpdatedAt: info.ModTime().UTC(), Size: info.Size()}, nil
}

func (l *LocalBlobStore) checkUploadVersion(ctx context.Context, key string, expectedVersion string) error {
	if expectedVersion == "" {
		return nil
	}
	current, err := l.Head(ctx, key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) || current == nil || current.Version != expectedVersion {
		return ErrVersionMismatch
	}
	return nil
}

// lockForKey uses fixed striping to serialize writes to the same key without
// retaining attacker- or workload-controlled blob keys for the process lifetime.
func (l *LocalBlobStore) lockForKey(key string) *sync.Mutex {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return &l.keyLocks[hash%uint32(len(l.keyLocks))]
}

func (l *LocalBlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if key == "" {
		return nil
	}

	keyLock := l.lockForKey(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	path := filepath.Join(l.Root, key)
	err := os.Remove(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (l *LocalBlobStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := os.Stat(l.Root); errors.Is(err, os.ErrNotExist) {
		return []ObjectInfo{}, nil
	} else if err != nil {
		return nil, err
	}

	items := make([]ObjectInfo, 0)
	err := filepath.WalkDir(l.Root, func(path string, d os.DirEntry, walkErr error) error {
		item, ok, err := l.blobListItem(ctx, prefix, path, d, walkErr)
		if err != nil || !ok {
			return err
		}
		items = append(items, item)
		return nil
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []ObjectInfo{}, nil
		}

		return nil, err
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items, nil
}

func (l *LocalBlobStore) blobListItem(
	ctx context.Context,
	prefix string,
	path string,
	d os.DirEntry,
	walkErr error,
) (ObjectInfo, bool, error) {
	if walkErr != nil {
		return ObjectInfo{}, false, walkErr
	}
	if d.IsDir() {
		return ObjectInfo{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, false, err
	}
	key, err := localBlobKey(l.Root, path)
	if err != nil || (prefix != "" && !strings.HasPrefix(key, prefix)) {
		return ObjectInfo{}, false, err
	}
	info, err := d.Info()
	if err != nil {
		return ObjectInfo{}, false, err
	}
	return localObjectInfo(key, info), true, nil
}

func localBlobKey(root string, path string) (string, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
}

func localObjectInfo(key string, info os.FileInfo) ObjectInfo {
	return ObjectInfo{
		Key:       key,
		Version:   fmt.Sprintf("%d-%d", info.ModTime().UnixNano(), info.Size()),
		UpdatedAt: info.ModTime().UTC(),
		Size:      info.Size(),
	}
}
