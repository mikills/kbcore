package kb

import "context"

// ManifestDocument pairs a parsed manifest with its version for CAS.
type ManifestDocument struct {
	Manifest SnapshotShardManifest
	Version  string
}

// ManifestStore abstracts manifest CRUD with CAS update semantics.
type ManifestStore interface {
	// Get downloads and parses the manifest. Returns ErrManifestNotFound if absent.
	Get(ctx context.Context, kbID string) (*ManifestDocument, error)

	// HeadVersion returns the current version without downloading the body.
	HeadVersion(ctx context.Context, kbID string) (string, error)

	// UpsertIfMatch publishes a manifest with CAS protection.
	// Empty expectedVersion means "create if absent".
	UpsertIfMatch(ctx context.Context, kbID string, manifest SnapshotShardManifest, expectedVersion string) (string, error)

	// Delete removes the manifest. Returns nil if already absent.
	Delete(ctx context.Context, kbID string) error
}
