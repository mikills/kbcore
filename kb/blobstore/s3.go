package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// Copy server-side copies srcKey to dstKey with CopyObject so shard bytes
// never transit the caller. CreateOnly maps to If-None-Match *, otherwise
// ExpectedVersion maps to If-Match. A 412 response surfaces as
// ErrVersionMismatch and a missing source as ErrNotFound.
func (s *S3BlobStore) Copy(ctx context.Context, srcKey, dstKey string, opts CopyOptions) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(srcKey) == "" || strings.TrimSpace(dstKey) == "" {
		return nil, errors.New("copy source and destination keys are required")
	}
	if srcKey == dstKey {
		return nil, errors.New("copy source and destination must differ")
	}
	if opts.CreateOnly && opts.ExpectedVersion != "" {
		return nil, errors.New("copy requires exactly one of create-only or an expected version")
	}
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(s.Bucket),
		Key:        aws.String(s.fullKey(dstKey)),
		CopySource: aws.String(s.Bucket + "/" + encodeCopySource(s.fullKey(srcKey))),
	}
	if opts.CreateOnly {
		input.IfNoneMatch = aws.String("*")
	} else if opts.ExpectedVersion != "" {
		input.IfMatch = aws.String(opts.ExpectedVersion)
	}
	if _, err := s.Client.CopyObject(ctx, input); err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: copy source %s: %w", ErrNotFound, srcKey, err)
		}
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusPreconditionFailed {
			return nil, fmt.Errorf("%w: copy precondition failed for %s", ErrVersionMismatch, dstKey)
		}
		return nil, fmt.Errorf("copy object %s to %s: %w", srcKey, dstKey, err)
	}
	head, err := s.Head(ctx, dstKey)
	if err != nil {
		return nil, fmt.Errorf("head copied object %s: %w", dstKey, err)
	}
	return head, nil
}

// CopyReplica server-side copies on the remote, preserving the fencing
// contract: the destination carries the new OperationID/checksum metadata so
// an uncertain response reconciles exactly like PutReplica.
func (s *S3BlobStore) CopyReplica(ctx context.Context, request ReplicaCopy) (*ReplicaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.SrcKey) == "" || strings.TrimSpace(request.DstKey) == "" {
		return nil, errors.New("replica copy source and destination are required")
	}
	if request.SrcKey == request.DstKey {
		return nil, errors.New("replica copy source and destination must differ")
	}
	if strings.TrimSpace(request.OperationID) == "" {
		return nil, errors.New("replica copy requires an operation ID")
	}
	if request.CreateOnly && request.ExpectedVersion != "" {
		return nil, errors.New("replica copy requires create-only or an expected remote version, not both")
	}
	if !request.CreateOnly && request.ExpectedVersion == "" {
		return nil, errors.New("replica copy requires create-only or an expected remote version")
	}
	checksum := request.Checksum
	if checksum == "" {
		current, err := s.HeadReplica(ctx, request.SrcKey)
		if err != nil {
			return nil, err
		}
		checksum = current.Checksum
	}
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(s.Bucket),
		Key:               aws.String(s.fullKey(request.DstKey)),
		CopySource:        aws.String(s.Bucket + "/" + encodeCopySource(s.fullKey(request.SrcKey))),
		MetadataDirective: types.MetadataDirectiveReplace,
		Metadata: map[string]string{
			replicaOperationMetadata: request.OperationID,
			replicaChecksumMetadata:  checksum,
		},
	}
	if request.CreateOnly {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(request.ExpectedVersion)
	}
	result, err := s.Client.CopyObject(ctx, input)
	if err != nil {
		if isS3NotFound(err) {
			return nil, ErrNotFound
		}
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusPreconditionFailed {
			return nil, fmt.Errorf("%w: replica copy precondition failed for %s", ErrVersionMismatch, request.DstKey)
		}
		return nil, fmt.Errorf("copy replica %s to %s: %w", request.SrcKey, request.DstKey, err)
	}
	version := ""
	if result.CopyObjectResult != nil {
		version = aws.ToString(result.CopyObjectResult.ETag)
	}
	if version == "" {
		head, headErr := s.Head(ctx, request.DstKey)
		if headErr != nil {
			return nil, fmt.Errorf("head copied replica %s: %w", request.DstKey, headErr)
		}
		version = head.Version
	}
	head, err := s.Head(ctx, request.DstKey)
	if err != nil {
		return nil, err
	}
	return &ReplicaInfo{ObjectInfo: *head, OperationID: request.OperationID, Checksum: checksum}, nil
}

// encodeCopySource escapes a full S3 key for the CopySource parameter,
// which is a URL path of the form bucket/key.
func encodeCopySource(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
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

const (
	replicaOperationMetadata = "minnow-operation-id"
	replicaChecksumMetadata  = "minnow-sha256"
)

func (s *S3BlobStore) PutReplica(ctx context.Context, request ReplicaPut) (*ReplicaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Body == nil || request.Size < 0 {
		return nil, errors.New("replica body and non-negative size are required")
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.Bucket),
		Key:           aws.String(s.fullKey(request.Key)),
		Body:          request.Body,
		ContentLength: aws.Int64(request.Size),
		Metadata: map[string]string{
			replicaOperationMetadata: request.OperationID,
			replicaChecksumMetadata:  request.Checksum,
		},
	}
	if request.CreateOnly {
		input.IfNoneMatch = aws.String("*")
	} else if request.ExpectedVersion != "" {
		input.IfMatch = aws.String(request.ExpectedVersion)
	} else {
		return nil, errors.New("replica put requires create-only or an expected remote version")
	}
	result, err := s.Client.PutObject(ctx, input)
	if err != nil {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusPreconditionFailed {
			return nil, fmt.Errorf("%w: replica precondition failed for %s", ErrVersionMismatch, request.Key)
		}
		return nil, fmt.Errorf("put replica %s: %w", request.Key, err)
	}
	version := aws.ToString(result.ETag)
	if version == "" {
		return nil, errors.New("replica put response is missing ETag")
	}
	return &ReplicaInfo{
		ObjectInfo: ObjectInfo{
			Key:       request.Key,
			Version:   version,
			UpdatedAt: s.now(),
			Size:      request.Size,
		},
		OperationID: request.OperationID,
		Checksum:    request.Checksum,
	}, nil
}

func (s *S3BlobStore) HeadReplica(ctx context.Context, key string) (*ReplicaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("head replica %s: %w", key, err)
	}
	version := aws.ToString(result.ETag)
	if version == "" {
		return nil, errors.New("replica head response is missing ETag")
	}
	return &ReplicaInfo{
		ObjectInfo: ObjectInfo{
			Key:       key,
			Version:   version,
			UpdatedAt: aws.ToTime(result.LastModified),
			Size:      aws.ToInt64(result.ContentLength),
		},
		OperationID: result.Metadata[replicaOperationMetadata],
		Checksum:    result.Metadata[replicaChecksumMetadata],
	}, nil
}

func (s *S3BlobStore) ClaimReplicationOwner(ctx context.Context, key, ownerID string) (string, error) {
	if strings.TrimSpace(ownerID) == "" {
		return "", errors.New("replication owner ID is required")
	}
	info, err := s.UploadBytesIfNotExists(ctx, key, []byte(ownerID))
	if err == nil {
		if info.Version == "" {
			return "", errors.New("replication owner response is missing ETag")
		}
		return info.Version, nil
	}
	if !errors.Is(err, ErrVersionMismatch) {
		return "", err
	}
	data, current, readErr := s.DownloadBytesWithInfo(ctx, key)
	if readErr != nil {
		return "", readErr
	}
	if string(data) != ownerID {
		return "", fmt.Errorf("%w: replication prefix is owned by another journal", ErrVersionMismatch)
	}
	if current.Version == "" {
		return "", errors.New("replication owner object is missing ETag")
	}
	return current.Version, nil
}

func (s *S3BlobStore) ReleaseReplicationOwner(ctx context.Context, key, ownerID, expectedVersion string) error {
	data, current, err := s.DownloadBytesWithInfo(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(data) != ownerID || current.Version != expectedVersion {
		// The original claim is already gone or changed. Never delete the
		// replacement; release is idempotently complete for this owner version.
		return nil
	}
	return s.DeleteReplica(ctx, key, expectedVersion)
}

func (s *S3BlobStore) DeleteReplica(ctx context.Context, key, expectedVersion string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if expectedVersion == "" {
		return errors.New("replica delete requires an expected remote version")
	}
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:  aws.String(s.Bucket),
		Key:     aws.String(s.fullKey(key)),
		IfMatch: aws.String(expectedVersion),
	})
	if err != nil {
		if isS3NotFound(err) {
			return ErrNotFound
		}
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusPreconditionFailed {
			return fmt.Errorf("%w: replica delete precondition failed for %s", ErrVersionMismatch, key)
		}
		return fmt.Errorf("delete replica %s: %w", key, err)
	}
	return nil
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

// isS3NotFound returns true for HeadObject (NotFound), GetObject
// (NoSuchKey), and CopyObject missing-source 404s. CopyObject surfaces a
// missing source through several shapes depending on path and SDK version —
// typed NotFound/NoSuchKey or a generic 404 ResponseError — so the generic
// 404 check comes last and covers the untyped variants.
func isS3NotFound(err error) bool {
	var notFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &notFound) || errors.As(err, &noSuchKey) {
		return true
	}
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}
