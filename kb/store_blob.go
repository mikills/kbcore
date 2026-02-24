package kb

import (
	"context"
	"time"
)

// BlobObjectInfo describes a blob object.
type BlobObjectInfo struct {
	Key       string
	Version   string
	UpdatedAt time.Time
	Size      int64
}

// BlobStore is the storage abstraction for KB snapshots.
type BlobStore interface {
	Head(ctx context.Context, key string) (*BlobObjectInfo, error)
	Download(ctx context.Context, key string, dest string) error
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]BlobObjectInfo, error)
}
