package kb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
}

// NewS3BlobStore creates a new S3-backed blob store.
// The prefix is optional and will be prepended to all keys.
func NewS3BlobStore(client *s3.Client, bucket, prefix string) *S3BlobStore {
	return &S3BlobStore{
		Client: client,
		Bucket: bucket,
		Prefix: prefix,
	}
}

// fullKey returns the full S3 key including prefix
func (s *S3BlobStore) fullKey(key string) string {
	if s.Prefix == "" {
		return key
	}
	return s.Prefix + key
}

// Head retrieves metadata for an object from S3.
// Returns ErrBlobNotFound if the object doesn't exist.
func (s *S3BlobStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullKey := s.fullKey(key)

	result, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		var notFoundErr *types.NotFound
		if errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
		}
		return nil, fmt.Errorf("head object %s: %w", key, err)
	}

	// Use ETag as version
	version := ""
	if result.ETag != nil {
		version = *result.ETag
	}

	updatedAt := time.Now().UTC()
	if result.LastModified != nil {
		updatedAt = *result.LastModified
	}

	size := int64(0)
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	return &BlobObjectInfo{
		Key:       key,
		Version:   version,
		UpdatedAt: updatedAt,
		Size:      size,
	}, nil
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
		var notFoundErr *types.NotFound
		if errors.As(err, &notFoundErr) {
			return fmt.Errorf("%w: %s", ErrBlobNotFound, key)
		}
		return fmt.Errorf("get object %s: %w", key, err)
	}
	defer result.Body.Close()

	// Create destination file
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

// UploadIfMatch uploads a file to S3 with optimistic concurrency control.
// If expectedVersion is empty, the upload is unconditional.
// If expectedVersion is provided, it must match the current ETag (version).
// Returns ErrBlobVersionMismatch if the versions don't match.
func (s *S3BlobStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullKey := s.fullKey(key)

	// Open source file
	file, err := os.Open(src)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	defer file.Close()

	// Prepare upload input
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(fullKey),
		Body:   file,
	}

	// Add conditional header if expectedVersion is provided
	if expectedVersion != "" {
		input.IfMatch = aws.String(expectedVersion)
	}

	// Upload
	result, err := s.Client.PutObject(ctx, input)
	if err != nil {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == 412 {
			return nil, fmt.Errorf("%w: version mismatch for %s", ErrBlobVersionMismatch, key)
		}
		return nil, fmt.Errorf("put object %s: %w", key, err)
	}

	// Get metadata for response
	version := ""
	if result.ETag != nil {
		version = *result.ETag
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat source file: %w", err)
	}

	return &BlobObjectInfo{
		Key:       key,
		Version:   version,
		UpdatedAt: time.Now().UTC(),
		Size:      info.Size(),
	}, nil
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

func (s *S3BlobStore) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullPrefix := s.fullKey(prefix)
	items := make([]BlobObjectInfo, 0)
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
			items = append(items, BlobObjectInfo{
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
