package kb

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrBlobVersionMismatch = errors.New("blob version mismatch")
	ErrBlobNotFound        = errors.New("blob not found")

	ErrCacheBudgetExceeded   = errors.New("cache budget exceeded")
	ErrGraphUnavailable      = errors.New("graph extraction is not configured")
	ErrGraphQueryUnavailable = errors.New("graph query requested but graph data is unavailable")
	ErrKBUninitialized       = errors.New("kb is not initialized")

	ErrInvalidEmbeddingDimension  = errors.New("invalid embedding dimension")
	ErrEmbeddingDimensionMismatch = errors.New("embedding dimension mismatch")

	ErrManifestNotFound            = errors.New("manifest not found")
	ErrWriteLeaseConflict          = errors.New("write lease conflict")
	ErrInvalidQueryRequest         = errors.New("invalid query request")
	ErrArtifactFormatNotConfigured = errors.New("artifact format is not configured")
	ErrUnsupportedOperation        = errors.New("operation not supported by this artifact format")
)

func wrapEmbeddingDimensionMismatch(err error, operation string) error {
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
	return fmt.Errorf("%w: %s; existing KB vectors were built with a different embedding configuration, rebuild/re-ingest this KB: %v", ErrEmbeddingDimensionMismatch, detail, err)
}

func isEmbeddingDimensionMismatchErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	hasFloatArray := strings.Contains(msg, "float[")
	if !hasFloatArray {
		return false
	}
	if strings.Contains(msg, "array_distance") && (strings.Contains(msg, "cast") || strings.Contains(msg, "size")) {
		return true
	}
	if strings.Contains(msg, "cannot cast") {
		return true
	}
	if strings.Contains(msg, "array") && strings.Contains(msg, "mismatch") {
		return true
	}
	if strings.Contains(msg, "different") && strings.Contains(msg, "size") {
		return true
	}
	return false
}
