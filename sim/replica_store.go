package sim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

var ErrReplicaUnavailable = errors.New("sim: replica unavailable")

type replicaObject struct {
	data        []byte
	version     string
	updatedAt   time.Time
	operationID string
	checksum    string
}

// ReplicaStore is an in-memory, faultable implementation of the production
// ReplicationStore contract. It uses deterministic versions and timestamps so
// seeded tiered-storage scenarios are reproducible.
type ReplicaStore struct {
	mu           sync.Mutex
	objects      map[string]replicaObject
	sequence     uint64
	unavailable  bool
	ownerID      string
	ownerVersion string
}

func NewReplicaStore() *ReplicaStore {
	return &ReplicaStore{objects: make(map[string]replicaObject)}
}

func (s *ReplicaStore) SetUnavailable(unavailable bool) {
	s.mu.Lock()
	s.unavailable = unavailable
	s.mu.Unlock()
}

func (s *ReplicaStore) Head(ctx context.Context, key string) (*blobstore.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, err := s.objectLocked(key)
	if err != nil {
		return nil, err
	}
	return objectInfo(key, object), nil
}

func (s *ReplicaStore) HeadReplica(ctx context.Context, key string) (*blobstore.ReplicaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, err := s.objectLocked(key)
	if err != nil {
		return nil, err
	}
	return &blobstore.ReplicaInfo{
		ObjectInfo:  *objectInfo(key, object),
		OperationID: object.operationID,
		Checksum:    object.checksum,
	}, nil
}

func (s *ReplicaStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, err := s.objectLocked(key)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), object.data...), nil
}

func (s *ReplicaStore) Download(ctx context.Context, key, destination string) error {
	data, err := s.DownloadBytes(ctx, key)
	if err != nil {
		return err
	}
	return os.WriteFile(destination, data, 0o600)
}

func (s *ReplicaStore) UploadBytesIfMatch(ctx context.Context, key string, data []byte, expectedVersion string) (*blobstore.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return nil, err
	}
	if err := s.checkExpectedLocked(key, expectedVersion); err != nil {
		return nil, err
	}
	object := s.putLocked(data, "", blobstore.BytesSHA256(data))
	s.objects[key] = object
	return objectInfo(key, object), nil
}

func (s *ReplicaStore) UploadIfMatch(ctx context.Context, key, source, expectedVersion string) (*blobstore.ObjectInfo, error) {
	data, err := os.ReadFile(source)
	if err != nil {
		return nil, err
	}
	return s.UploadBytesIfMatch(ctx, key, data, expectedVersion)
}

func (s *ReplicaStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return err
	}
	delete(s.objects, key)
	return nil
}

func (s *ReplicaStore) Copy(ctx context.Context, srcKey, dstKey string, opts blobstore.CopyOptions) (*blobstore.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(srcKey) == "" || strings.TrimSpace(dstKey) == "" || srcKey == dstKey {
		return nil, errors.New("sim: copy keys must be non-empty and distinct")
	}
	src, found := s.objects[srcKey]
	if !found {
		return nil, blobstore.ErrNotFound
	}
	if current, exists := s.objects[dstKey]; exists {
		if opts.CreateOnly {
			return nil, blobstore.ErrVersionMismatch
		}
		if opts.ExpectedVersion != "" && current.version != opts.ExpectedVersion {
			return nil, blobstore.ErrVersionMismatch
		}
	} else if opts.ExpectedVersion != "" {
		return nil, blobstore.ErrVersionMismatch
	}
	object := s.putLocked(src.data, "", blobstore.BytesSHA256(src.data))
	s.objects[dstKey] = object
	return objectInfo(dstKey, object), nil
}

func (s *ReplicaStore) CopyReplica(ctx context.Context, request blobstore.ReplicaCopy) (*blobstore.ReplicaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.SrcKey) == "" || strings.TrimSpace(request.DstKey) == "" || request.SrcKey == request.DstKey {
		return nil, errors.New("sim: replica copy keys must be non-empty and distinct")
	}
	if strings.TrimSpace(request.OperationID) == "" {
		return nil, errors.New("sim: replica copy requires an operation ID")
	}
	src, found := s.objects[request.SrcKey]
	if !found {
		return nil, blobstore.ErrNotFound
	}
	if current, exists := s.objects[request.DstKey]; exists {
		if request.CreateOnly {
			return nil, blobstore.ErrVersionMismatch
		}
		if request.ExpectedVersion == "" || current.version != request.ExpectedVersion {
			return nil, blobstore.ErrVersionMismatch
		}
	} else if !request.CreateOnly {
		return nil, blobstore.ErrVersionMismatch
	}
	checksum := request.Checksum
	if checksum == "" {
		checksum = src.checksum
	}
	object := s.putLocked(src.data, request.OperationID, checksum)
	s.objects[request.DstKey] = object
	return &blobstore.ReplicaInfo{
		ObjectInfo:  *objectInfo(request.DstKey, object),
		OperationID: object.operationID,
		Checksum:    object.checksum,
	}, nil
}

func (s *ReplicaStore) List(ctx context.Context, prefix string) ([]blobstore.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return nil, err
	}
	result := make([]blobstore.ObjectInfo, 0, len(s.objects))
	for key, object := range s.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, *objectInfo(key, object))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *ReplicaStore) PutReplica(ctx context.Context, request blobstore.ReplicaPut) (*blobstore.ReplicaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != request.Size || blobstore.BytesSHA256(data) != request.Checksum || request.OperationID == "" {
		return nil, errors.New("sim: invalid replica payload metadata")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return nil, err
	}
	current, found := s.objects[request.Key]
	switch {
	case request.CreateOnly && found:
		return nil, blobstore.ErrVersionMismatch
	case !request.CreateOnly && (!found || request.ExpectedVersion == "" || current.version != request.ExpectedVersion):
		return nil, blobstore.ErrVersionMismatch
	}
	object := s.putLocked(data, request.OperationID, request.Checksum)
	s.objects[request.Key] = object
	return &blobstore.ReplicaInfo{
		ObjectInfo:  *objectInfo(request.Key, object),
		OperationID: object.operationID,
		Checksum:    object.checksum,
	}, nil
}

func (s *ReplicaStore) DeleteReplica(ctx context.Context, key, expectedVersion string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return err
	}
	current, found := s.objects[key]
	if !found {
		return blobstore.ErrNotFound
	}
	if expectedVersion == "" || current.version != expectedVersion {
		return blobstore.ErrVersionMismatch
	}
	delete(s.objects, key)
	return nil
}

func (s *ReplicaStore) ClaimReplicationOwner(ctx context.Context, _ string, ownerID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return "", err
	}
	if s.ownerID == "" {
		s.sequence++
		s.ownerID = ownerID
		s.ownerVersion = fmt.Sprintf("owner-%d", s.sequence)
		return s.ownerVersion, nil
	}
	if s.ownerID != ownerID {
		return "", blobstore.ErrVersionMismatch
	}
	return s.ownerVersion, nil
}

func (s *ReplicaStore) ReleaseReplicationOwner(ctx context.Context, _ string, ownerID, expectedVersion string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.availableLocked(); err != nil {
		return err
	}
	if s.ownerID == ownerID && s.ownerVersion == expectedVersion {
		s.ownerID = ""
		s.ownerVersion = ""
	}
	return nil
}

func (s *ReplicaStore) objectLocked(key string) (replicaObject, error) {
	if err := s.availableLocked(); err != nil {
		return replicaObject{}, err
	}
	object, found := s.objects[key]
	if !found {
		return replicaObject{}, blobstore.ErrNotFound
	}
	return object, nil
}

func (s *ReplicaStore) availableLocked() error {
	if s.unavailable {
		return ErrReplicaUnavailable
	}
	return nil
}

func (s *ReplicaStore) checkExpectedLocked(key, expectedVersion string) error {
	if expectedVersion == "" {
		return nil
	}
	current, found := s.objects[key]
	if !found || current.version != expectedVersion {
		return blobstore.ErrVersionMismatch
	}
	return nil
}

func (s *ReplicaStore) putLocked(data []byte, operationID, checksum string) replicaObject {
	s.sequence++
	return replicaObject{
		data:        append([]byte(nil), data...),
		version:     fmt.Sprintf("v%d", s.sequence),
		updatedAt:   time.Date(2026, 1, 1, 0, 0, 0, int(s.sequence), time.UTC),
		operationID: operationID,
		checksum:    checksum,
	}
}

func objectInfo(key string, object replicaObject) *blobstore.ObjectInfo {
	return &blobstore.ObjectInfo{
		Key:       key,
		Version:   object.version,
		UpdatedAt: object.updatedAt,
		Size:      int64(len(object.data)),
	}
}
