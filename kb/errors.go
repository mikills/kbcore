package kb

import "errors"

var (
	ErrBlobVersionMismatch = errors.New("blob version mismatch")
	ErrBlobNotFound        = errors.New("blob not found")

	ErrCacheBudgetExceeded   = errors.New("cache budget exceeded")
	ErrGraphUnavailable      = errors.New("graph extraction is not configured")
	ErrGraphQueryUnavailable = errors.New("graph query requested but graph data is unavailable")
	ErrKBUninitialized       = errors.New("kb is not initialized")

	ErrWriteLeaseConflict = errors.New("write lease conflict")
)
