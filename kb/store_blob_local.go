package kb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LocalBlobStore implements BlobStore for local filesystem storage.
type LocalBlobStore struct {
	Root string
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

func (l *LocalBlobStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	path := filepath.Join(l.Root, key)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	version, err := fileContentSHA256(path)
	if err != nil {
		return nil, err
	}

	return &BlobObjectInfo{
		Key:       key,
		Version:   version,
		UpdatedAt: info.ModTime().UTC(),
		Size:      info.Size(),
	}, nil
}

func (l *LocalBlobStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	dest := filepath.Join(l.Root, key)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}

	// check version match before upload
	if expectedVersion != "" {
		current, err := l.Head(ctx, key)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if errors.Is(err, os.ErrNotExist) || current == nil || current.Version != expectedVersion {
			return nil, ErrBlobVersionMismatch
		}
	}

	// compute sha256 of source file before copy (avoids hashing twice)
	srcHash, err := fileContentSHA256(src)
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

	return &BlobObjectInfo{
		Key:       key,
		Version:   srcHash,
		UpdatedAt: info.ModTime().UTC(),
		Size:      info.Size(),
	}, nil
}

func (l *LocalBlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" {
		return nil
	}

	path := filepath.Join(l.Root, key)
	err := os.Remove(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (l *LocalBlobStore) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := os.Stat(l.Root); errors.Is(err, os.ErrNotExist) {
		return []BlobObjectInfo{}, nil
	} else if err != nil {
		return nil, err
	}

	items := make([]BlobObjectInfo, 0)
	err := filepath.WalkDir(l.Root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		rel, err := filepath.Rel(l.Root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		// use mtime+size as cheap version proxy instead of expensive sha256
		version := fmt.Sprintf("%d-%d", info.ModTime().UnixNano(), info.Size())

		items = append(items, BlobObjectInfo{
			Key:       key,
			Version:   version,
			UpdatedAt: info.ModTime().UTC(),
			Size:      info.Size(),
		})
		return nil
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []BlobObjectInfo{}, nil
		}
		return nil, err
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items, nil
}
