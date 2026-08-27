// Publish model: Write-Audit-Publish (WAP), as practiced by Apache Iceberg
// and Delta Lake.
//
//  1. Write: workers produce immutable, content-addressed staged artifacts
//     (shards). Visible in blob storage but not referenced by any manifest.
//  2. Audit: the publish path validates that all artifacts exist and the
//     candidate manifest is internally consistent.
//  3. Publish: single CAS update on the manifest blob via
//     ManifestStore.UpsertIfMatch flips visibility atomically.
//
// On CAS conflict the convention is to rebase, not abort: re-read the manifest,
// re-validate, and retry the CAS up to a bounded number of attempts. Because
// staged artifacts are content-addressed, re-publishing the same set is
// idempotent at the blob layer.

package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mikills/minnow/kb/manifest"
)

const DefaultSnapshotShardSize int64 = 16 * 1024 * 1024

const ShardManifestLayoutDuckDBs = manifest.ShardManifestLayoutDuckDBs

type SnapshotShardMetadata = manifest.ShardMetadata
type SnapshotShardManifest = manifest.ShardManifest

func ShardManifestKey(kbID string) string { return manifest.ShardManifestKey(kbID) }

// UploadSnapshotShardedIfMatch uploads a DB snapshot as immutable shard objects
// and updates the manifest with optimistic version matching.
func (l *KB) UploadSnapshotShardedIfMatch(
	ctx context.Context,
	kbID, localDBPath, expectedManifestVersion string,
	partSize int64,
) (*BlobObjectInfo, error) {
	if err := validateSnapshotShardUpload(kbID, localDBPath); err != nil {
		return nil, err
	}
	partSize = normalizeSnapshotPartSize(partSize)

	leaseManager, lease, err := l.AcquireWriteLease(ctx, kbID)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := leaseManager.Release(context.WithoutCancel(ctx), lease); err != nil {
			slog.Default().Warn("snapshot shard lease release failed", logKeyKBID, kbID, logKeyError, err)
		}
	}()

	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}

	artifacts, err := format.BuildArtifacts(ctx, kbID, localDBPath, partSize)
	if err != nil {
		return nil, err
	}

	manifest := l.snapshotShardManifest(kbID, format, artifacts)

	newVersion, err := l.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			l.recordManifestCASConflict(kbID)
		}
		return nil, err
	}

	l.recordShardCount(kbID, len(artifacts))
	if err := l.EnqueueOrphanedShardBlobs(ctx, kbID, artifacts, time.Time{}); err != nil {
		slog.Default().WarnContext(ctx, "orphaned shard scan failed", logKeyKBID, kbID, logKeyError, err)
	}
	return &BlobObjectInfo{Key: ShardManifestKey(kbID), Version: newVersion}, nil
}

func validateSnapshotShardUpload(kbID string, localDBPath string) error {
	if kbID == "" {
		return fmt.Errorf("kbID cannot be empty")
	}
	if localDBPath == "" {
		return fmt.Errorf("localDBPath cannot be empty")
	}
	return nil
}

func normalizeSnapshotPartSize(partSize int64) int64 {
	if partSize <= 0 {
		return DefaultSnapshotShardSize
	}
	return partSize
}

func (l *KB) snapshotShardManifest(
	kbID string,
	format ArtifactFormat,
	artifacts []SnapshotShardMetadata,
) SnapshotShardManifest {
	return SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         ShardManifestLayoutDuckDBs,
		FormatKind:     format.Kind(),
		FormatVersion:  format.Version(),
		KBID:           kbID,
		CreatedAt:      l.Clock.Now(),
		TotalSizeBytes: snapshotShardTotalSize(artifacts),
		Shards:         artifacts,
	}
}

func snapshotShardTotalSize(artifacts []SnapshotShardMetadata) int64 {
	totalSize := int64(0)
	for _, artifact := range artifacts {
		totalSize += artifact.SizeBytes
	}
	return totalSize
}
