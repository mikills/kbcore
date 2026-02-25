package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// BlobManifestStore implements ManifestStore on top of a BlobStore.
// It absorbs all temp-file mechanics for manifest serialization.
type BlobManifestStore struct {
	Store BlobStore
}

func (s *BlobManifestStore) manifestKey(kbID string) string {
	return shardManifestKey(kbID)
}

func (s *BlobManifestStore) Get(ctx context.Context, kbID string) (*ManifestDocument, error) {
	key := s.manifestKey(kbID)

	info, err := s.Store.Head(ctx, key)
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil, ErrManifestNotFound
		}
		return nil, err
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-manifest-get-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	manifestPath := filepath.Join(tmpDir, "manifest.json")
	if err := s.Store.Download(ctx, key, manifestPath); err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil, ErrManifestNotFound
		}
		return nil, err
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}

	var manifest SnapshotShardManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &ManifestDocument{
		Manifest: manifest,
		Version:  info.Version,
	}, nil
}

func (s *BlobManifestStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	info, err := s.Store.Head(ctx, s.manifestKey(kbID))
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return info.Version, nil
}

func (s *BlobManifestStore) UpsertIfMatch(ctx context.Context, kbID string, manifest SnapshotShardManifest, expectedVersion string) (string, error) {
	tmpDir, err := os.MkdirTemp("", "kbcore-manifest-upsert-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	manifestPath := filepath.Join(tmpDir, "manifest.json")
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return "", err
	}

	info, err := s.Store.UploadIfMatch(ctx, s.manifestKey(kbID), manifestPath, expectedVersion)
	if err != nil {
		return "", err
	}
	return info.Version, nil
}

func (s *BlobManifestStore) Delete(ctx context.Context, kbID string) error {
	err := s.Store.Delete(ctx, s.manifestKey(kbID))
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("delete manifest for %s: %w", kbID, err)
	}
	return nil
}
