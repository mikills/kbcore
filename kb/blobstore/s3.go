package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3BlobStore implements BlobStore using AWS S3.
// It provides S3-backed storage for KB snapshots with optimistic concurrency control.
type S3BlobStore struct {
	Client *s3.Client
	Bucket string
	Prefix string

	clockMu sync.RWMutex
	clock   Clock
}

// NewS3BlobStore creates a new S3-backed blob store.
// The prefix is optional and will be prepended to all keys.
func NewS3BlobStore(client *s3.Client, bucket, prefix string) *S3BlobStore {
	return &S3BlobStore{
		Client: client,
		Bucket: bucket,
		Prefix: prefix,
		clock:  realClock{},
	}
}

// SetClock replaces the store's Clock. Safe for concurrent use.
func (s *S3BlobStore) SetClock(c Clock) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	if c == nil {
		s.clock = realClock{}
		return
	}
	s.clock = c
}

func (s *S3BlobStore) now() time.Time {
	s.clockMu.RLock()
	defer s.clockMu.RUnlock()
	return nowFrom(s.clock)
}

// fullKey returns the full S3 key including prefix
func (s *S3BlobStore) fullKey(key string) string {
	if s.Prefix == "" {
		return key
	}
	return s.Prefix + key
}

// Head retrieves metadata for an object from S3.
// Returns ErrNotFound if the object doesn't exist.
func (s *S3BlobStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullKey := s.fullKey(key)

	result, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("head object %s: %w", key, err)
	}

	// Use ETag as version
	version := ""
	if result.ETag != nil {
		version = *result.ETag
	}

	updatedAt := s.now()
	if result.LastModified != nil {
		updatedAt = *result.LastModified
	}

	size := int64(0)
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	return &ObjectInfo{
		Key:       key,
		Version:   version,
		UpdatedAt: updatedAt,
		Size:      size,
	}, nil
}

func (s *S3BlobStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullKey := s.fullKey(key)
	result, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("download object %s: %w", key, err)
	}
	return data, nil
}

func (s *S3BlobStore) DownloadBytesWithInfo(ctx context.Context, key string) ([]byte, *ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	result, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("download object %s: %w", key, err)
	}
	version := ""
	if result.ETag != nil {
		version = *result.ETag
	}
	size := int64(len(data))
	updatedAt := s.now()
	if result.LastModified != nil {
		updatedAt = *result.LastModified
	}
	return data, &ObjectInfo{Key: key, Version: version, UpdatedAt: updatedAt, Size: size}, nil
}

// Download retrieves an object from S3 and writes it to the destination path.
func (s *S3BlobStore) Download(ctx context.Context, key string, dest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	fullKey := s.fullKey(key)

	result, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isS3NotFound(err) {
			return fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return fmt.Errorf("get object %s: %w", key, err)
	}
	defer result.Body.Close()

	file, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer file.Close()

	// Copy content
	if _, err := io.Copy(file, result.Body); err != nil {
		return fmt.Errorf("download object %s: %w", key, err)
	}

	return file.Sync()
}

func (s *S3BlobStore) UploadBytesIfMatch(
	ctx context.Context,
	key string,
	data []byte,
	expectedVersion string,
) (*ObjectInfo, error) {
	return s.putObjectIfMatch(ctx, key, bytes.NewReader(data), int64(len(data)), expectedVersion)
}

// UploadIfMatch uploads a file to S3 with optimistic concurrency control.
// If expectedVersion is empty, the upload is unconditional.
// If expectedVersion is provided, it must match the current ETag (version).
// Returns ErrVersionMismatch if the versions don't match.
func (s *S3BlobStore) UploadIfMatch(
	ctx context.Context,
	key string,
	src string,
	expectedVersion string,
) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(src)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat source file: %w", err)
	}
	return s.putObjectIfMatch(ctx, key, file, info.Size(), expectedVersion)
}

func (s *S3BlobStore) putObjectIfMatch(
	ctx context.Context,
	key string,
	body io.Reader,
	size int64,
	expectedVersion string,
) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.fullKey(key)), Body: body}
	if expectedVersion != "" {
		input.IfMatch = aws.String(expectedVersion)
	}
	result, err := s.Client.PutObject(ctx, input)
	if err != nil {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == 412 {
			return nil, fmt.Errorf("%w: version mismatch for %s", ErrVersionMismatch, key)
		}
		return nil, fmt.Errorf("put object %s: %w", key, err)
	}
	version := ""
	if result.ETag != nil {
		version = *result.ETag
	}
	return &ObjectInfo{Key: key, Version: version, UpdatedAt: s.now(), Size: size}, nil
}

// UploadIfNotExists uploads a file only if the key does not already exist.
func (s *S3BlobStore) UploadIfNotExists(ctx context.Context, key, src string) (*ObjectInfo, error) {
	const maxConflictAttempts = 4
	var lastErr error
	for attempt := 0; attempt < maxConflictAttempts; attempt++ {
		file, err := os.Open(src)
		if err != nil {
			return nil, fmt.Errorf("open source file: %w", err)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("stat source file: %w", statErr)
		}
		result, putErr := s.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.Bucket), Key: aws.String(s.fullKey(key)),
			Body: file, IfNoneMatch: aws.String("*"),
		})
		closeErr := file.Close()
		if putErr == nil {
			if closeErr != nil {
				return nil, closeErr
			}
			return &ObjectInfo{Key: key, Version: aws.ToString(result.ETag), UpdatedAt: s.now(), Size: info.Size()}, nil
		}
		var responseErr *smithyhttp.ResponseError
		if errors.As(putErr, &responseErr) {
			switch responseErr.HTTPStatusCode() {
			case http.StatusPreconditionFailed:
				return nil, fmt.Errorf("%w: object already exists at %s", ErrVersionMismatch, key)
			case http.StatusConflict:
				lastErr = putErr
				if attempt+1 < maxConflictAttempts {
					timer := time.NewTimer(time.Duration(attempt+1) * 25 * time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						return nil, ctx.Err()
					case <-timer.C:
					}
					continue
				}
			}
		}
		return nil, fmt.Errorf("put object %s: %w", key, putErr)
	}
	return nil, fmt.Errorf("put object %s after conditional conflicts: %w", key, lastErr)
}

// UploadBytesIfNotExists writes data only if the key does not already exist
// (uses If-None-Match: * conditional). Returns ErrVersionMismatch if the
// object already exists.
func (s *S3BlobStore) UploadBytesIfNotExists(ctx context.Context, key string, data []byte) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(s.fullKey(key)),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	}
	result, err := s.Client.PutObject(ctx, input)
	if err != nil {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == 412 {
			return nil, fmt.Errorf("%w: object already exists at %s", ErrVersionMismatch, key)
		}
		return nil, fmt.Errorf("put object %s: %w", key, err)
	}
	version := ""
	if result.ETag != nil {
		version = *result.ETag
	}
	return &ObjectInfo{Key: key, Version: version, UpdatedAt: s.now(), Size: int64(len(data))}, nil
}

func (s *S3BlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}

func (s *S3BlobStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullPrefix := s.fullKey(prefix)
	items := make([]ObjectInfo, 0)
	var token *string

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		out, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.Bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list objects for prefix %s: %w", prefix, err)
		}

		for _, obj := range out.Contents {
			fullKey := aws.ToString(obj.Key)
			key := fullKey
			if s.Prefix != "" {
				key = strings.TrimPrefix(fullKey, s.Prefix)
			}
			updatedAt := time.Time{}
			if obj.LastModified != nil {
				updatedAt = *obj.LastModified
			}
			items = append(items, ObjectInfo{
				Key:       key,
				Version:   aws.ToString(obj.ETag),
				UpdatedAt: updatedAt.UTC(),
				Size:      aws.ToInt64(obj.Size),
			})
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items, nil
}

// isS3NotFound returns true for both HeadObject (NotFound) and GetObject
// (NoSuchKey) 404 responses, which use different error types in the AWS SDK.
func isS3NotFound(err error) bool {
	var notFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	return errors.As(err, &notFound) || errors.As(err, &noSuchKey)
}
