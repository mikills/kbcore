package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// ObjectInfo describes a blob object.
type ObjectInfo struct {
	Key       string
	Version   string
	UpdatedAt time.Time
	Size      int64
}

// Store is the storage abstraction for KB snapshots.
type Store interface {
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	DownloadBytes(ctx context.Context, key string) ([]byte, error)
	Download(ctx context.Context, key string, dest string) error
	UploadBytesIfMatch(ctx context.Context, key string, data []byte, expectedVersion string) (*ObjectInfo, error)
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// ReplicaPut carries an idempotency identity and an explicit remote
// precondition. Exactly one of CreateOnly and ExpectedVersion should be set for
// tiered-store mutations; this ensures stale journal owners cannot issue
// unfenced overwrites.
type ReplicaPut struct {
	Key             string
	Body            io.Reader
	Size            int64
	ExpectedVersion string
	CreateOnly      bool
	OperationID     string
	Checksum        string
}

type ReplicaInfo struct {
	ObjectInfo
	OperationID string
	Checksum    string
}

// ReplicationStore is the stronger remote contract required by tiered storage.
// OperationID must be persisted atomically with the object and returned by
// HeadReplica so an uncertain response can be reconciled without confusing an
// identical write from another actor. DeleteReplica must condition deletion on
// expectedVersion when it is non-empty.
type ReplicationStore interface {
	Store
	PutReplica(ctx context.Context, request ReplicaPut) (*ReplicaInfo, error)
	HeadReplica(ctx context.Context, key string) (*ReplicaInfo, error)
	DeleteReplica(ctx context.Context, key, expectedVersion string) error
	// ClaimReplicationOwner creates a non-expiring prefix owner record or
	// resumes it when the stored ownerID matches. A different owner must fail.
	ClaimReplicationOwner(ctx context.Context, key, ownerID string) (version string, err error)
	// ReleaseReplicationOwner conditionally removes only the exact claim and is
	// idempotent if that owner version is already absent or replaced.
	ReleaseReplicationOwner(ctx context.Context, key, ownerID, expectedVersion string) error
}

type Clock interface{ Now() time.Time }

var (
	ErrVersionMismatch = errors.New("blob version mismatch")
	ErrNotFound        = errors.New("blob not found")
)

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }
func nowFrom(c Clock) time.Time {
	if c == nil {
		return time.Now().UTC()
	}
	return c.Now()
}

func copyFileSync(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func replaceFileWithCopy(src, dest string) error {
	tmpDest := fmt.Sprintf("%s.tmp-%d", dest, time.Now().UnixNano())
	if err := copyFileSync(src, tmpDest); err != nil {
		return err
	}
	defer os.Remove(tmpDest)
	return os.Rename(tmpDest, dest)
}

func replaceFileWithBytes(data []byte, dest string) error {
	tmpDest := fmt.Sprintf("%s.tmp-%d", dest, time.Now().UnixNano())
	file, err := os.Create(tmpDest)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	if syncErr := file.Sync(); writeErr == nil {
		writeErr = syncErr
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return removeTempAfterError(tmpDest, writeErr)
	}
	if err := os.Rename(tmpDest, dest); err != nil {
		return removeTempAfterError(tmpDest, err)
	}
	return nil
}

func removeTempAfterError(path string, cause error) error {
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("%w; remove temp file: %v", cause, removeErr)
	}
	return cause
}

func BytesSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func FileContentSHA256(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
