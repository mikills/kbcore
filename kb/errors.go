package kb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/lease"
	"github.com/mikills/minnow/kb/manifest"
	"github.com/mikills/minnow/kb/openaiembedder"
)

var (
	ErrBlobVersionMismatch = blobstore.ErrVersionMismatch
	ErrBlobNotFound        = blobstore.ErrNotFound

	ErrCacheBudgetExceeded   = errors.New("cache budget exceeded")
	ErrGraphUnavailable      = errors.New("graph extraction is not configured")
	ErrGraphQueryUnavailable = errors.New("graph query requested but graph data is unavailable")
	ErrKBUninitialized       = errors.New("kb is not initialized")

	ErrInvalidEmbeddingDimension  = openaiembedder.ErrInvalidEmbeddingDimension
	ErrEmbeddingDimensionMismatch = errors.New("embedding dimension mismatch")
	ErrInvalidArtifactFormat      = errors.New("invalid artifact format")

	ErrManifestNotFound            = manifest.ErrNotFound
	ErrInvalidManifestStore        = manifest.ErrInvalidStore
	ErrWriteLeaseConflict          = lease.ErrConflict
	ErrInvalidQueryRequest         = errors.New("invalid query request")
	ErrArtifactFormatNotConfigured = errors.New("artifact format is not configured")
	ErrFormatNotRegistered         = errors.New("artifact format not registered for this KB's format_kind")
	ErrUnsupportedOperation        = errors.New("operation not supported by this artifact format")
	ErrScopeNotFound               = errors.New("scope not found")
	ErrScopedDocuments             = errors.New("documents are referenced by a scope")
	ErrScopeDocumentsMissing       = errors.New("scope documents are not published")
)

func WrapEmbeddingDimensionMismatch(err error, operation string) error {
	if err == nil {
		return nil
	}
	if !isEmbeddingDimensionMismatchErr(err) {
		return err
	}
	detail := strings.TrimSpace(operation)
	if detail == "" {
		detail = "embedding dimension mismatch"
	}
	return fmt.Errorf(
		"%w: %s; existing KB vectors were built with a different embedding configuration, rebuild/re-ingest this KB: %v",
		ErrEmbeddingDimensionMismatch,
		detail,
		err,
	)
}

func isEmbeddingDimensionMismatchErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if isArrayDistanceMismatch(msg) {
		return true
	}
	return strings.Contains(msg, "float[") && isFloatDimensionMismatchMessage(msg)
}

func isArrayDistanceMismatch(msg string) bool {
	return strings.Contains(msg, "array_distance") && containsAnySubstring(msg, "size", "cast", "argument")
}

func isFloatDimensionMismatchMessage(msg string) bool {
	return strings.Contains(msg, "cannot cast") ||
		(strings.Contains(msg, "array") && strings.Contains(msg, "mismatch")) ||
		(strings.Contains(msg, "different") && strings.Contains(msg, "size"))
}

func containsAnySubstring(msg string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}
