package sim

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	kb "github.com/mikills/minnow/kb"
)

// BlobFaults configures injected failures for FaultableBlobStore. Rates are
// 0.0 (never) to 1.0 (always). Zero values mean "no fault".
type BlobFaults struct {
	UploadFailRate   float64
	DownloadFailRate float64
	HeadFailRate     float64
	DeleteFailRate   float64
	ListFailRate     float64
}

// ErrInjected is returned by the faultable store when a random roll triggers
// a failure. Scenarios can test for this to assert retry behaviour.
var ErrInjected = errors.New("sim: injected fault")

// FaultableBlobStore wraps a kb.BlobStore with a seeded RNG that decides
// whether each operation should fail. All underlying I/O still hits the inner
// store when the roll passes.
type FaultableBlobStore struct {
	inner  kb.BlobStore
	mu     sync.Mutex
	faults BlobFaults
	rng    *rand.Rand
}

// NewFaultableBlobStore wraps inner with seeded fault injection.
func NewFaultableBlobStore(inner kb.BlobStore, faults BlobFaults, rng *rand.Rand) *FaultableBlobStore {
	if rng == nil {
		rng = rand.New(rand.NewSource(1))
	}
	return &FaultableBlobStore{inner: inner, faults: faults, rng: rng}
}

// SetFaults replaces the active fault configuration mid-run.
func (s *FaultableBlobStore) SetFaults(f BlobFaults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = f
}

// rollFault atomically reads the named rate and rolls the RNG under the
// lock. Keeps the rate read and the roll in the same critical section so
// SetFaults cannot race with an in-flight op.
func (s *FaultableBlobStore) rollFault(pick func(BlobFaults) float64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rate := pick(s.faults)
	if rate <= 0 {
		return false
	}
	return s.rng.Float64() < rate
}

func (s *FaultableBlobStore) Head(ctx context.Context, key string) (*kb.BlobObjectInfo, error) {
	if s.rollFault(func(f BlobFaults) float64 { return f.HeadFailRate }) {
		return nil, ErrInjected
	}
	return s.inner.Head(ctx, key)
}

func (s *FaultableBlobStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	if s.rollFault(func(f BlobFaults) float64 { return f.DownloadFailRate }) {
		return nil, ErrInjected
	}
	return s.inner.DownloadBytes(ctx, key)
}

func (s *FaultableBlobStore) Download(ctx context.Context, key string, dest string) error {
	if s.rollFault(func(f BlobFaults) float64 { return f.DownloadFailRate }) {
		return ErrInjected
	}
	return s.inner.Download(ctx, key, dest)
}

func (s *FaultableBlobStore) UploadBytesIfMatch(
	ctx context.Context,
	key string,
	data []byte,
	expectedVersion string,
) (*kb.BlobObjectInfo, error) {
	if s.rollFault(func(f BlobFaults) float64 { return f.UploadFailRate }) {
		return nil, ErrInjected
	}
	return s.inner.UploadBytesIfMatch(ctx, key, data, expectedVersion)
}

func (s *FaultableBlobStore) UploadIfMatch(
	ctx context.Context,
	key string,
	src string,
	expectedVersion string,
) (*kb.BlobObjectInfo, error) {
	if s.rollFault(func(f BlobFaults) float64 { return f.UploadFailRate }) {
		return nil, ErrInjected
	}
	return s.inner.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (s *FaultableBlobStore) UploadIfNotExists(
	ctx context.Context,
	key string,
	src string,
) (*kb.BlobObjectInfo, error) {
	if s.rollFault(func(f BlobFaults) float64 { return f.UploadFailRate }) {
		return nil, ErrInjected
	}
	store, ok := s.inner.(interface {
		UploadIfNotExists(context.Context, string, string) (*kb.BlobObjectInfo, error)
	})
	if !ok {
		return nil, errors.New("create-only upload unsupported")
	}
	return store.UploadIfNotExists(ctx, key, src)
}

func (s *FaultableBlobStore) Delete(ctx context.Context, key string) error {
	if s.rollFault(func(f BlobFaults) float64 { return f.DeleteFailRate }) {
		return ErrInjected
	}
	return s.inner.Delete(ctx, key)
}

func (s *FaultableBlobStore) List(ctx context.Context, prefix string) ([]kb.BlobObjectInfo, error) {
	if s.rollFault(func(f BlobFaults) float64 { return f.ListFailRate }) {
		return nil, ErrInjected
	}
	return s.inner.List(ctx, prefix)
}
