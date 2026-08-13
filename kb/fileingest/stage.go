package fileingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/media"
)

const MaxFilesPerIngest = 1000

type Store interface {
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*blobstore.ObjectInfo, error)
	Download(ctx context.Context, key string, dest string) error
	Delete(ctx context.Context, key string) error
}

type createOnlyStore interface {
	UploadIfNotExists(ctx context.Context, key string, src string) (*blobstore.ObjectInfo, error)
}

type Upload struct {
	FileID      string
	DocumentID  string
	Filename    string
	ContentType string
	Metadata    map[string]any
	Body        io.Reader
}

type Result struct {
	FileID      string         `json:"file_id"`
	DocumentID  string         `json:"document_id,omitempty"`
	MediaID     string         `json:"media_id,omitempty"`
	Filename    string         `json:"filename"`
	ContentType string         `json:"content_type,omitempty"`
	Status      string         `json:"status"`
	Error       string         `json:"error,omitempty"`
	PageCount   int            `json:"page_count,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type Source struct {
	FileID        string         `json:"file_id"`
	DocumentID    string         `json:"document_id"`
	MediaID       string         `json:"media_id"`
	Filename      string         `json:"filename"`
	ContentType   string         `json:"content_type"`
	StagedBlobKey string         `json:"staged_blob_key"`
	SizeBytes     int64          `json:"size_bytes"`
	Checksum      string         `json:"checksum"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type StagedUpload struct {
	Source  Source
	Cleanup func(context.Context)
}

type StageInput struct {
	Store          Store
	KBID           string
	Files          []Upload
	DocumentCount  int
	MaxBytes       int64
	MediaID        func() string
	StagingBlobKey func(kbID, fileID, filename string) string
	MaxUploadBytes int64
}

func ValidateInput(store Store, fileCount int, documentCount int) error {
	if store == nil {
		return errors.New("ingest: BlobStore not configured")
	}
	if fileCount == 0 && documentCount == 0 {
		return errors.New("ingest: at least one file or document is required")
	}
	if total := fileCount + documentCount; total > MaxFilesPerIngest {
		return fmt.Errorf("ingest: too many items (%d): max %d per request", total, MaxFilesPerIngest)
	}
	return nil
}

func StageUploads(ctx context.Context, input StageInput) ([]StagedUpload, func(), error) {
	staged := make([]StagedUpload, 0, len(input.Files))
	cleanupAll := func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		CleanupStagedUploads(cleanupCtx, staged)
	}
	for i, file := range input.Files {
		s, err := StageUpload(ctx, input, file, i)
		if err != nil {
			cleanupAll()
			return nil, nil, err
		}
		staged = append(staged, s)
	}
	return staged, cleanupAll, nil
}

func CleanupStagedUploads(ctx context.Context, staged []StagedUpload) {
	for _, s := range staged {
		if s.Cleanup != nil {
			s.Cleanup(ctx)
		}
	}
}

func StageUpload(ctx context.Context, input StageInput, file Upload, index int) (StagedUpload, error) {
	cleanName, ct, maxBytes, err := ValidateUpload(input.KBID, file, input.MaxBytes, input.MaxUploadBytes)
	if err != nil {
		return StagedUpload{}, err
	}
	tmp, err := media.WriteTempCapped(file.Body, maxBytes)
	if err != nil {
		return StagedUpload{}, err
	}
	if tmp.Size() == 0 {
		tmp.Cleanup()
		return StagedUpload{}, errors.New("ingest: empty file rejected")
	}
	fileID, err := ID(file.FileID, index)
	if err != nil {
		tmp.Cleanup()
		return StagedUpload{}, err
	}
	mediaID := input.MediaID()
	documentID := strings.TrimSpace(file.DocumentID)
	if documentID == "" {
		documentID = GenerateDocumentID(cleanName)
	}
	// The generated media ID makes each request's staging path unique, so an
	// idempotent retry can never overwrite bytes referenced by an older event.
	stagedBlobKey := input.StagingBlobKey(input.KBID, mediaID, cleanName)
	createOnly, ok := input.Store.(createOnlyStore)
	if !ok {
		tmp.Cleanup()
		return StagedUpload{}, errors.New("ingest: blob store does not support create-only staging uploads")
	}
	_, uploadErr := createOnly.UploadIfNotExists(ctx, stagedBlobKey, tmp.Path())
	if uploadErr != nil {
		err := uploadErr
		tmp.Cleanup()
		return StagedUpload{}, fmt.Errorf("ingest: stage file upload: %w", err)
	}
	tmp.Cleanup()
	return StagedUpload{
		Source: Source{
			FileID:        fileID,
			DocumentID:    documentID,
			MediaID:       mediaID,
			Filename:      cleanName,
			ContentType:   ct,
			StagedBlobKey: stagedBlobKey,
			SizeBytes:     tmp.Size(),
			Checksum:      tmp.SHA256(),
			Metadata:      CloneMap(file.Metadata),
		},
		Cleanup: func(ctx context.Context) {
			if err := input.Store.Delete(ctx, stagedBlobKey); err != nil {
				slog.Default().Warn("file ingest staged blob cleanup failed", "error", err)
			}
		},
	}, nil
}

func ValidateUpload(kbID string, file Upload, maxBytes int64, defaultMaxBytes int64) (string, string, int64, error) {
	if strings.TrimSpace(kbID) == "" {
		return "", "", 0, errors.New("ingest: kb_id required")
	}
	if file.Body == nil {
		return "", "", 0, errors.New("ingest: file body required")
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	cleanName, err := media.SanitizeMediaFilename(file.Filename)
	if err != nil {
		return "", "", 0, err
	}
	ct := InferContentType(file.ContentType, cleanName)
	if !SupportedContentType(ct, cleanName) {
		return "", "", 0, fmt.Errorf("ingest: content type %q for %q is not supported", ct, cleanName)
	}
	return cleanName, ct, maxBytes, nil
}

func CloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	maps.Copy(out, in)
	return out
}
