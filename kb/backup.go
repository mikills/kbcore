// Phase 1 automated backups and constant-time KB branching.
//
// What Phase 1 provides:
//   - Immutable backup descriptors (v1 JSON) stored at
//     <kb>.backups/<backup-id>.backup.json via CreateOnly semantics.
//   - Same-KB snapshots: a manifest copy under the same KBID prefix
//     (<kb>.snapshots/<snapshot-id>.manifest.json). Zero-copy: shard bytes
//     are never copied, and the live manifest is never mutated.
//   - New-KB clone/restore: per-shard Download+Upload byte copy into the
//     target KB prefix, then a CreateOnly manifest publish with
//     verify-before-publish. The source KB is never mutated. Pointer
//     sharing (referencing source shard keys from the clone manifest) is
//     explicitly NOT done here; it is deferred to Phase 2.
//
// Format migration note (v1 -> v2):
// manifests read through manifest/blob.go applyManifestReadDefaults default
// a missing format_version to 1, while the current DuckDB reader requires
// exactly DuckDBFormatVersion=2 (kb/duckdb/artifact_format.go). A v1
// manifest predates the content-addressed SHA guarantees that make
// ValidateBackupDescriptor meaningful, so the backup and restore paths in
// this file REJECT v1 manifests with ErrBackupLegacyFormat instead of
// silently migrating them. Operators migrate by re-ingesting/re-sealing,
// which rebuilds every shard and publishes a v2 manifest. No automatic
// migration happens in Phase 1.
//
// SHA256 population prerequisite:
// the seal path (kb/duckdb/snapshot_shards.go buildAndUploadOneSnapshotShard)
// and the compaction path (kb/duckdb/compaction.go
// buildAndUploadCompactionReplacement) already populate SizeBytes, Version,
// and SHA256 on every SnapshotShardMetadata they produce. The backup path
// below enforces that contract: a shard with an empty key, zero size, or
// empty SHA/version is rejected instead of backed up. Verify-before-publish
// on the clone path re-checks Head size and re-hashes downloaded bytes, so
// a corrupt object fails the restore before any manifest is published.
//
// Deferred to Phase 2 (not implemented here): zero-copy clones via
// pointer-sharing, a server-side Store.Copy primitive, reachability GC that
// understands shared shard references, and retention automation beyond the
// SelectForRetention helper.
package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/manifest"
)

const (
	// BackupDescriptorVersion is the only descriptor version Phase 1 writes.
	BackupDescriptorVersion = 1
	// BackupMinReader advertises the minimum reader that understands v1.
	BackupMinReader = "minnow>=phase1-backup-v1"
	// BackupSupportedFormatVersion is the only manifest format_version the
	// backup/restore path accepts. See the package note on v1 -> v2.
	BackupSupportedFormatVersion = 2

	backupKeyInfix   = ".backups/"
	backupKeySuffix  = ".backup.json"
	snapshotKeyInfix = ".snapshots/"
	snapshotSuffix   = ".manifest.json"
	originKeySuffix  = ".origin.json"

	maxBackupIDLen = 128
)

var backupIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

var (
	// ErrBackupExists is returned when a backup, snapshot, or clone-target
	// manifest already exists. It always wraps ErrBlobVersionMismatch as well,
	// so errors.Is(err, ErrBackupExists) and errors.Is(err,
	// ErrBlobVersionMismatch) are both true for every path that reports it.
	ErrBackupExists = errors.New("backup already exists")
	// ErrBackupNotFound is returned when a named backup or snapshot is absent.
	ErrBackupNotFound = errors.New("backup not found")
	// ErrBackupLegacyFormat is returned when a manifest predates the
	// supported format version. Re-ingest/re-seal to migrate; Phase 1 does
	// not auto-migrate.
	ErrBackupLegacyFormat = errors.New("manifest uses a legacy format version; re-ingest to migrate")
	// ErrBackupCorrupt is returned when stored bytes fail validation.
	ErrBackupCorrupt = errors.New("backup is corrupt")
	// ErrDeleteBlockedByBackups is returned by DeleteKnowledgeBase when the
	// KB still owns backup or snapshot markers.
	ErrDeleteBlockedByBackups = errors.New("knowledge base has backups or snapshots; delete them first")
)

// BackupShardRef is the integrity record for one shard at backup time.
type BackupShardRef struct {
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Version   string `json:"version"`
}

// BackupDescriptor is the immutable v1 backup record.
type BackupDescriptor struct {
	DescriptorVersion     int                   `json:"descriptor_version"`
	BackupID              string                `json:"backup_id"`
	SourceKBID            string                `json:"source_kb_id"`
	SourceManifestVersion string                `json:"source_manifest_version"`
	SourceManifestSHA     string                `json:"source_manifest_sha256"`
	CreatedAt             time.Time             `json:"created_at"`
	FormatKind            string                `json:"format_kind"`
	FormatVersion         int                   `json:"format_version"`
	MinReader             string                `json:"min_reader"`
	Shards                []BackupShardRef      `json:"shards"`
	ManifestSnapshot      SnapshotShardManifest `json:"manifest_snapshot"`
	DescriptorSHA256      string                `json:"descriptor_sha256"`
}

// SnapshotRecord is a same-KB, zero-copy manifest copy with parent lineage.
// Shard bytes are shared by key reference; nothing is copied.
type SnapshotRecord struct {
	SnapshotVersion       int                   `json:"snapshot_version"`
	SnapshotID            string                `json:"snapshot_id"`
	SourceKBID            string                `json:"source_kb_id"`
	SourceManifestVersion string                `json:"source_manifest_version"`
	ParentBackupID        string                `json:"parent_backup_id,omitempty"`
	CreatedAt             time.Time             `json:"created_at"`
	Manifest              SnapshotShardManifest `json:"manifest"`
	RecordSHA256          string                `json:"record_sha256"`
}

// CloneOriginRecord marks a KB created by CloneKBFromBackup.
type CloneOriginRecord struct {
	RecordVersion   int       `json:"record_version"`
	KBID            string    `json:"kb_id"`
	SourceKBID      string    `json:"source_kb_id"`
	SourceBackupID  string    `json:"source_backup_id,omitempty"`
	SourceBranchID  string    `json:"source_branch_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	ManifestVersion string    `json:"manifest_version"`
}

// BackupDescriptorKey returns the blob key for a backup descriptor.
func BackupDescriptorKey(kbID, backupID string) string {
	return kbID + backupKeyInfix + backupID + backupKeySuffix
}

// SnapshotRecordKey returns the blob key for a same-KB snapshot record.
func SnapshotRecordKey(kbID, snapshotID string) string {
	return kbID + snapshotKeyInfix + snapshotID + snapshotSuffix
}

// CloneOriginKey returns the blob key for a clone lineage marker.
func CloneOriginKey(kbID string) string { return kbID + originKeySuffix }

// BackupPrefix returns the listing prefix owning a KB's backup descriptors.
func BackupPrefix(kbID string) string { return kbID + backupKeyInfix }

// SnapshotPrefix returns the listing prefix owning a KB's snapshot records.
func SnapshotPrefix(kbID string) string { return kbID + snapshotKeyInfix }

func validateBackupID(what, id string) error {
	if id == "" {
		return fmt.Errorf("%s id is required", what)
	}
	if len(id) > maxBackupIDLen {
		return fmt.Errorf("%s id exceeds %d characters", what, maxBackupIDLen)
	}
	if !backupIDPattern.MatchString(id) {
		return fmt.Errorf("%s id %q must match [A-Za-z0-9_-]+", what, id)
	}
	return nil
}

func checkManifestFormatSupported(manifest *SnapshotShardManifest) error {
	if manifest == nil {
		return fmt.Errorf("manifest is required")
	}
	if manifest.FormatVersion != BackupSupportedFormatVersion {
		return fmt.Errorf("%w: got format_version %d, want %d (format_kind %q)",
			ErrBackupLegacyFormat, manifest.FormatVersion, BackupSupportedFormatVersion, manifest.FormatKind)
	}
	if strings.TrimSpace(manifest.FormatKind) == "" {
		return fmt.Errorf("%w: manifest format_kind is empty", ErrBackupCorrupt)
	}
	return nil
}

func checkShardIntegrity(shards []SnapshotShardMetadata) error {
	if len(shards) == 0 {
		return fmt.Errorf("%w: manifest has no shards", ErrBackupCorrupt)
	}
	seen := make(map[string]struct{}, len(shards))
	for i, shard := range shards {
		if strings.TrimSpace(shard.ShardID) == "" {
			return fmt.Errorf("%w: shard %d has empty shard_id", ErrBackupCorrupt, i)
		}
		if strings.TrimSpace(shard.Key) == "" {
			return fmt.Errorf("%w: shard %q has empty key", ErrBackupCorrupt, shard.ShardID)
		}
		if err := validateShardKeyForBackup(shard.Key); err != nil {
			return fmt.Errorf("%w: %w", ErrBackupCorrupt, err)
		}
		if shard.SizeBytes <= 0 {
			return fmt.Errorf("%w: shard %q has non-positive size_bytes", ErrBackupCorrupt, shard.ShardID)
		}
		if strings.TrimSpace(shard.SHA256) == "" {
			return fmt.Errorf("%w: shard %q is missing sha256; re-seal to populate integrity metadata", ErrBackupCorrupt, shard.ShardID)
		}
		if strings.TrimSpace(shard.Version) == "" {
			return fmt.Errorf("%w: shard %q is missing version; re-seal to populate integrity metadata", ErrBackupCorrupt, shard.ShardID)
		}
		if _, dup := seen[shard.Key]; dup {
			return fmt.Errorf("%w: duplicate shard key %q", ErrBackupCorrupt, shard.Key)
		}
		seen[shard.Key] = struct{}{}
	}
	return nil
}

// ValidateBackupDescriptor checks every structural invariant of a descriptor,
// including the self-checksum. It reports tampering, truncation, and shards
// that cannot be integrity-checked on restore.
func ValidateBackupDescriptor(desc *BackupDescriptor) error {
	if desc == nil {
		return fmt.Errorf("%w: descriptor is nil", ErrBackupCorrupt)
	}
	if desc.DescriptorVersion != BackupDescriptorVersion {
		return fmt.Errorf("%w: unknown descriptor_version %d, want %d", ErrBackupCorrupt, desc.DescriptorVersion, BackupDescriptorVersion)
	}
	if err := validateBackupID("backup", desc.BackupID); err != nil {
		return fmt.Errorf("%w: %w", ErrBackupCorrupt, err)
	}
	if strings.TrimSpace(desc.SourceKBID) == "" {
		return fmt.Errorf("%w: source_kb_id is required", ErrBackupCorrupt)
	}
	if strings.TrimSpace(desc.SourceManifestVersion) == "" {
		return fmt.Errorf("%w: source_manifest_version is required", ErrBackupCorrupt)
	}
	if desc.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at is required", ErrBackupCorrupt)
	}
	if strings.TrimSpace(desc.MinReader) == "" {
		return fmt.Errorf("%w: min_reader is required", ErrBackupCorrupt)
	}
	if desc.FormatVersion != BackupSupportedFormatVersion {
		return fmt.Errorf("%w: descriptor format_version %d is not supported (want %d)",
			ErrBackupLegacyFormat, desc.FormatVersion, BackupSupportedFormatVersion)
	}
	if strings.TrimSpace(desc.FormatKind) == "" {
		return fmt.Errorf("%w: format_kind is required", ErrBackupCorrupt)
	}
	if len(desc.Shards) == 0 {
		return fmt.Errorf("%w: descriptor has no shards", ErrBackupCorrupt)
	}
	seen := make(map[string]struct{}, len(desc.Shards))
	for i, shard := range desc.Shards {
		if strings.TrimSpace(shard.Key) == "" {
			return fmt.Errorf("%w: shard %d has empty key", ErrBackupCorrupt, i)
		}
		if err := validateShardKeyForBackup(shard.Key); err != nil {
			return fmt.Errorf("%w: %w", ErrBackupCorrupt, err)
		}
		if shard.SizeBytes <= 0 {
			return fmt.Errorf("%w: shard %q has non-positive size_bytes", ErrBackupCorrupt, shard.Key)
		}
		if strings.TrimSpace(shard.SHA256) == "" {
			return fmt.Errorf("%w: shard %q is missing sha256", ErrBackupCorrupt, shard.Key)
		}
		if strings.TrimSpace(shard.Version) == "" {
			return fmt.Errorf("%w: shard %q is missing version", ErrBackupCorrupt, shard.Key)
		}
		if _, dup := seen[shard.Key]; dup {
			return fmt.Errorf("%w: duplicate shard key %q", ErrBackupCorrupt, shard.Key)
		}
		seen[shard.Key] = struct{}{}
	}
	if desc.ManifestSnapshot.KBID != desc.SourceKBID {
		return fmt.Errorf("%w: manifest_snapshot kb_id %q does not match source %q",
			ErrBackupCorrupt, desc.ManifestSnapshot.KBID, desc.SourceKBID)
	}
	if len(desc.ManifestSnapshot.Shards) != len(desc.Shards) {
		return fmt.Errorf("%w: manifest_snapshot shard count %d does not match refs %d",
			ErrBackupCorrupt, len(desc.ManifestSnapshot.Shards), len(desc.Shards))
	}
	want, err := backupDescriptorChecksum(desc)
	if err != nil {
		return err
	}
	if !hmacEqual(want, desc.DescriptorSHA256) {
		return fmt.Errorf("%w: descriptor_sha256 mismatch (tampered or truncated)", ErrBackupCorrupt)
	}
	return nil
}

func hmacEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

func manifestSHA(manifest SnapshotShardManifest) (string, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func backupDescriptorChecksum(desc *BackupDescriptor) (string, error) {
	shadow := *desc
	shadow.DescriptorSHA256 = ""
	data, err := json.Marshal(shadow)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func snapshotRecordChecksum(rec *SnapshotRecord) (string, error) {
	shadow := *rec
	shadow.RecordSHA256 = ""
	data, err := json.Marshal(shadow)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// uploadBytesCreateOnly writes data only if key is absent.
//
// Race safety depends on the store:
//   - S3 / tiered: native UploadBytesIfNotExists (If-None-Match *) is atomic,
//     so concurrent creators across processes resolve to exactly one winner.
//   - Local: O_EXCL create via UploadIfNotExists is atomic on one host.
//   - Generic Store fallback: Head-then-put fenced by the per-key write lease
//     (AcquireWriteLease around Head-then-put) plus the in-process key stripe,
//     so concurrent creators sharing the store AND the lease manager resolve
//     to one winner across processes. A lease conflict itself reports
//     ErrBackupExists (a rival is creating the same key).
//
// Residual risk: stores with neither a conditional put nor a shared
// cross-process lease manager keep a Head-then-put race: two creators in
// different processes (or sharing only the store, not the lease manager) can
// both miss the Head and the second put silently overwrites (last-writer-
// wins). Such stores must not be relied on for cross-process CreateOnly
// fencing; pair them with a shared lease manager (file/S3/Redis) instead.
//
// Every ErrBackupExists returned here wraps ErrBlobVersionMismatch, so
// errors.Is(err, ErrBackupExists) and errors.Is(err, ErrBlobVersionMismatch)
// are both true.
func (l *KB) uploadBytesCreateOnly(ctx context.Context, key string, data []byte) (*BlobObjectInfo, error) {
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ex, ok := l.BlobStore.(interface {
		UploadBytesIfNotExists(ctx context.Context, key string, data []byte) (*blobstore.ObjectInfo, error)
	}); ok {
		info, err := ex.UploadBytesIfNotExists(ctx, key, data)
		if err != nil {
			if errors.Is(err, ErrBlobVersionMismatch) {
				return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, err)
			}
			return nil, err
		}
		return info, nil
	}
	if local, ok := l.BlobStore.(*LocalBlobStore); ok {
		tmp, err := os.CreateTemp("", "minnow-createonly-*")
		if err != nil {
			return nil, err
		}
		tmpName := tmp.Name()
		if _, err := tmp.Write(data); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return nil, err
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpName)
			return nil, err
		}
		defer os.Remove(tmpName)
		info, err := local.UploadIfNotExists(ctx, key, tmpName)
		if err != nil {
			if errors.Is(err, ErrBlobVersionMismatch) {
				return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, err)
			}
			return nil, err
		}
		return info, nil
	}
	// Generic Store fallback: lease-fenced Head-then-put. The write lease
	// serializes creators that share the lease manager (cross-process when
	// the manager itself is shared, e.g. file/S3/Redis); the key stripe
	// additionally serializes threads sharing this process. See the doc
	// comment above for the residual risk without a shared lease.
	mgr, lease, leaseErr := l.AcquireWriteLease(ctx, "backup-createonly/"+key)
	if leaseErr != nil {
		if errors.Is(leaseErr, ErrWriteLeaseConflict) {
			return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, ErrBlobVersionMismatch)
		}
		// No lease available: degrade to the legacy in-process stripe
		// rather than failing the write outright.
		return l.uploadBytesCreateOnlyStriped(ctx, key, data)
	}
	defer func() { _ = mgr.Release(context.Background(), lease) }()
	mu := l.LockFor("backup-createonly/" + key)
	mu.Lock()
	defer mu.Unlock()
	if _, err := l.BlobStore.Head(ctx, key); err == nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, ErrBlobVersionMismatch)
	} else if !errors.Is(err, ErrBlobNotFound) && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	info, err := l.BlobStore.UploadBytesIfMatch(ctx, key, data, "")
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, err)
		}
		return nil, err
	}
	return info, nil
}

// uploadBytesCreateOnlyStriped is the legacy in-process-only fallback used
// when no write lease can be acquired: safe against threads sharing this KB
// instance, last-writer-wins across processes.
func (l *KB) uploadBytesCreateOnlyStriped(ctx context.Context, key string, data []byte) (*BlobObjectInfo, error) {
	mu := l.LockFor("backup-createonly/" + key)
	mu.Lock()
	defer mu.Unlock()
	if _, err := l.BlobStore.Head(ctx, key); err == nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, ErrBlobVersionMismatch)
	} else if !errors.Is(err, ErrBlobNotFound) && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	info, err := l.BlobStore.UploadBytesIfMatch(ctx, key, data, "")
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			return nil, fmt.Errorf("%w: %s: %w", ErrBackupExists, key, err)
		}
		return nil, err
	}
	return info, nil
}

// validateKBID rejects identifiers that could escape the kbID-namespaced blob
// layout (kbID+".backups/", ".snapshots/", ".duckdb.manifest.json",
// ".duckdb.shards/"). All backup, snapshot, and clone entry points call this;
// without it a kbID containing "/" or ".." would let Head/List/Upload/Delete
// wander outside the KB's own prefix.
func validateKBID(kbID string) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kb_id required")
	}
	if strings.Contains(kbID, "/") || strings.Contains(kbID, "\\") ||
		strings.Contains(kbID, "..") || kbID == "." || kbID == ".." ||
		strings.HasPrefix(kbID, "/") ||
		strings.Contains(kbID, backupKeyInfix) || strings.Contains(kbID, snapshotKeyInfix) ||
		strings.Contains(kbID, ".duckdb.") || strings.Contains(kbID, ".manifest.json") {
		return fmt.Errorf("kb_id %q is not allowed: must not contain /, .., or reserved infixes (.backups/, .snapshots/, .duckdb.)", kbID)
	}
	return nil
}

// validateShardKeyForBackup rejects shard keys that escape the blob namespace.
// Absolute keys and parent-directory segments would let a crafted manifest
// redirect backup verification, clone staging, or GC deletion outside the
// KB's prefix.
func validateShardKeyForBackup(key string) error {
	if strings.HasPrefix(key, "/") || strings.Contains(key, "..") || strings.Contains(key, "\\") {
		return fmt.Errorf("shard key %q escapes the blob namespace", key)
	}
	return nil
}

// CreateBackup captures an immutable descriptor of the KB's current
// manifest. Shard bytes are not copied; integrity refs pin them.
func (l *KB) CreateBackup(ctx context.Context, kbID, backupID string) (*BackupDescriptor, error) {
	if err := validateKBID(kbID); err != nil {
		return nil, err
	}
	if err := validateBackupID("backup", backupID); err != nil {
		return nil, err
	}
	if l.ManifestStore == nil {
		return nil, fmt.Errorf("manifest store is not configured")
	}
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			return nil, fmt.Errorf("backup %q: %w", kbID, err)
		}
		return nil, err
	}
	if err := checkManifestFormatSupported(&doc.Manifest); err != nil {
		return nil, err
	}
	if err := checkShardIntegrity(doc.Manifest.Shards); err != nil {
		return nil, err
	}
	manifestSHA, err := manifestSHA(doc.Manifest)
	if err != nil {
		return nil, err
	}
	refs := make([]BackupShardRef, 0, len(doc.Manifest.Shards))
	for _, shard := range doc.Manifest.Shards {
		refs = append(refs, BackupShardRef{
			Key:       shard.Key,
			SizeBytes: shard.SizeBytes,
			SHA256:    shard.SHA256,
			Version:   shard.Version,
		})
	}
	desc := &BackupDescriptor{
		DescriptorVersion:     BackupDescriptorVersion,
		BackupID:              backupID,
		SourceKBID:            kbID,
		SourceManifestVersion: doc.Version,
		SourceManifestSHA:     manifestSHA,
		CreatedAt:             nowFrom(l.Clock),
		FormatKind:            doc.Manifest.FormatKind,
		FormatVersion:         doc.Manifest.FormatVersion,
		MinReader:             BackupMinReader,
		Shards:                refs,
		ManifestSnapshot:      doc.Manifest,
	}
	sum, err := backupDescriptorChecksum(desc)
	if err != nil {
		return nil, err
	}
	desc.DescriptorSHA256 = sum
	data, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}
	if _, err := l.uploadBytesCreateOnly(ctx, BackupDescriptorKey(kbID, backupID), data); err != nil {
		return nil, err
	}
	return desc, nil
}

// GetBackup reads and validates a stored descriptor.
func (l *KB) GetBackup(ctx context.Context, kbID, backupID string) (*BackupDescriptor, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("backup", backupID); err != nil {
		return nil, err
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	data, err := l.BlobStore.DownloadBytes(ctx, BackupDescriptorKey(kbID, backupID))
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s/%s", ErrBackupNotFound, kbID, backupID)
		}
		return nil, err
	}
	var desc BackupDescriptor
	if err := json.Unmarshal(data, &desc); err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", ErrBackupCorrupt, kbID, backupID, err)
	}
	if err := ValidateBackupDescriptor(&desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

// ListBackupIDs lists backup ids for a KB, newest first.
func (l *KB) ListBackupIDs(ctx context.Context, kbID string) ([]string, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	objects, err := l.BlobStore.List(ctx, BackupPrefix(kbID))
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(objects))
	for _, obj := range objects {
		rest, found := strings.CutPrefix(obj.Key, BackupPrefix(kbID))
		if !found {
			continue
		}
		id, found := strings.CutSuffix(rest, backupKeySuffix)
		if !found || id == "" || strings.Contains(id, "/") {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// DeleteBackup removes one backup descriptor. Shard bytes are untouched.
func (l *KB) DeleteBackup(ctx context.Context, kbID, backupID string) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("backup", backupID); err != nil {
		return err
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	return l.BlobStore.Delete(ctx, BackupDescriptorKey(kbID, backupID))
}

// SelectForRetention returns the backups to delete to keep the newest `keep`
// valid descriptors. The newest valid descriptor is never deleted, even when
// keep <= 0. Descriptors that fail validation are always selected.
func SelectForRetention(descs []BackupDescriptor, keep int) []BackupDescriptor {
	if len(descs) == 0 {
		return nil
	}
	sorted := append([]BackupDescriptor(nil), descs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].BackupID < sorted[j].BackupID
		}
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	if keep < 1 {
		keep = 1
	}
	var drop []BackupDescriptor
	keptValid := 0
	for _, desc := range sorted {
		d := desc
		if ValidateBackupDescriptor(&d) != nil {
			drop = append(drop, desc)
			continue
		}
		if keptValid < keep {
			keptValid++
			continue
		}
		drop = append(drop, desc)
	}
	return drop
}

// CreateSnapshot records a zero-copy manifest copy under the same KBID
// prefix. No shard bytes are copied and the live manifest is untouched.
func (l *KB) CreateSnapshot(ctx context.Context, kbID, snapshotID string) (*SnapshotRecord, error) {
	return l.CreateSnapshotFrom(ctx, kbID, snapshotID, "")
}

// CreateSnapshotFrom is CreateSnapshot with an optional parent backup link.
func (l *KB) CreateSnapshotFrom(ctx context.Context, kbID, snapshotID, parentBackupID string) (*SnapshotRecord, error) {
	if err := validateKBID(kbID); err != nil {
		return nil, err
	}
	if err := validateBackupID("snapshot", snapshotID); err != nil {
		return nil, err
	}
	if parentBackupID != "" {
		if err := validateBackupID("parent backup", parentBackupID); err != nil {
			return nil, err
		}
	}
	if l.ManifestStore == nil {
		return nil, fmt.Errorf("manifest store is not configured")
	}
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			return nil, fmt.Errorf("snapshot %q: %w", kbID, err)
		}
		return nil, err
	}
	if err := checkManifestFormatSupported(&doc.Manifest); err != nil {
		return nil, err
	}
	if err := checkShardIntegrity(doc.Manifest.Shards); err != nil {
		return nil, err
	}
	rec := &SnapshotRecord{
		SnapshotVersion:       1,
		SnapshotID:            snapshotID,
		SourceKBID:            kbID,
		SourceManifestVersion: doc.Version,
		ParentBackupID:        parentBackupID,
		CreatedAt:             nowFrom(l.Clock),
		Manifest:              doc.Manifest,
	}
	sum, err := snapshotRecordChecksum(rec)
	if err != nil {
		return nil, err
	}
	rec.RecordSHA256 = sum
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	if _, err := l.uploadBytesCreateOnly(ctx, SnapshotRecordKey(kbID, snapshotID), data); err != nil {
		return nil, err
	}
	return rec, nil
}

// GetSnapshot reads a snapshot record and verifies its self-checksum.
func (l *KB) GetSnapshot(ctx context.Context, kbID, snapshotID string) (*SnapshotRecord, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("snapshot", snapshotID); err != nil {
		return nil, err
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	data, err := l.BlobStore.DownloadBytes(ctx, SnapshotRecordKey(kbID, snapshotID))
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: snapshot %s/%s", ErrBackupNotFound, kbID, snapshotID)
		}
		return nil, err
	}
	var rec SnapshotRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("%w: snapshot %s/%s: %w", ErrBackupCorrupt, kbID, snapshotID, err)
	}
	if rec.SnapshotVersion != 1 {
		return nil, fmt.Errorf("%w: unknown snapshot_version %d", ErrBackupCorrupt, rec.SnapshotVersion)
	}
	want, err := snapshotRecordChecksum(&rec)
	if err != nil {
		return nil, err
	}
	if !hmacEqual(want, rec.RecordSHA256) {
		return nil, fmt.Errorf("%w: snapshot record_sha256 mismatch", ErrBackupCorrupt)
	}
	return &rec, nil
}

// ListSnapshotIDs lists snapshot ids for a KB, sorted for determinism.
func (l *KB) ListSnapshotIDs(ctx context.Context, kbID string) ([]string, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	objects, err := l.BlobStore.List(ctx, SnapshotPrefix(kbID))
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(objects))
	for _, obj := range objects {
		rest, found := strings.CutPrefix(obj.Key, SnapshotPrefix(kbID))
		if !found {
			continue
		}
		id, found := strings.CutSuffix(rest, snapshotSuffix)
		if !found || id == "" || strings.Contains(id, "/") {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// DeleteSnapshot removes one snapshot record. Shared shard bytes are untouched.
func (l *KB) DeleteSnapshot(ctx context.Context, kbID, snapshotID string) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("snapshot", snapshotID); err != nil {
		return err
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	return l.BlobStore.Delete(ctx, SnapshotRecordKey(kbID, snapshotID))
}

// HasBackupsOrBranches reports whether a KB owns backup descriptors,
// snapshot records, or branch markers. DeleteKnowledgeBase refuses while
// this is true.
//
// This is a guard, not a fence: it runs before DeleteKnowledgeBase deletes
// the manifest, so a concurrent CreateBackup/CreateSnapshot/BranchKB that
// lands between the check and the delete is not stopped (guard→delete
// TOCTOU). Likewise a direct ManifestStore.Delete bypasses this guard
// entirely — only DeleteKnowledgeBase enforces it. Callers that need a hard
// guarantee must quiesce backup/snapshot/branch writers first.
func (l *KB) HasBackupsOrBranches(ctx context.Context, kbID string) (bool, error) {
	if err := validateKBID(kbID); err != nil {
		return false, err
	}
	if l.BlobStore == nil {
		// Fail closed: without a store we cannot prove the KB owns no
		// markers, so refusing to claim "no backups" keeps
		// DeleteKnowledgeBase from deleting on an unverified assumption.
		// Callers that intentionally run without a blob store must handle
		// this error.
		return false, fmt.Errorf("blob store is not configured")
	}
	backups, err := l.BlobStore.List(ctx, BackupPrefix(kbID))
	if err != nil {
		return false, fmt.Errorf("list backups for %q: %w", kbID, err)
	}
	if len(backups) > 0 {
		return true, nil
	}
	snapshots, err := l.BlobStore.List(ctx, SnapshotPrefix(kbID))
	if err != nil {
		return false, fmt.Errorf("list snapshots for %q: %w", kbID, err)
	}
	if len(snapshots) > 0 {
		return true, nil
	}
	branches, err := l.BlobStore.List(ctx, BranchPrefix(kbID))
	if err != nil {
		return false, fmt.Errorf("list branches for %q: %w", kbID, err)
	}
	return len(branches) > 0, nil
}

func remapShardKey(srcKBID, dstKBID, srcKey string) string {
	// Only strip the source prefix on a "." delimiter. A bare CutPrefix would
	// also strip "kb" from "kb2.duckdb.shards/..." (rest "2.duckdb..."),
	// landing the clone at "dst2.duckdb..." outside the target namespace.
	if rest, found := strings.CutPrefix(srcKey, srcKBID); found && strings.HasPrefix(rest, ".") {
		return dstKBID + rest
	}
	// Keys that do not carry the source prefix still land under the target
	// prefix so a clone never writes outside its own namespace.
	trimmed := strings.TrimPrefix(srcKey, "/")
	return dstKBID + ".duckdb.shards/restored/" + trimmed
}

// backupPinnedShardKeys returns every shard key referenced by a KB's backup
// descriptors, snapshot records, and branch markers. Shard GC must treat
// these as live: backups and zero-copy snapshots/branches pin shard bytes
// by key, and deleting a pinned shard would corrupt restores long after the
// live manifest stopped referencing it.
//
// List failures are returned (the caller retries the GC entry) while single
// unreadable markers are skipped best-effort: one corrupt descriptor must not
// wedge GC for the whole KB. Unreadable markers fail closed nowhere —
// operators should delete or repair corrupt markers.
func (l *KB) backupPinnedShardKeys(ctx context.Context, kbID string) (map[string]struct{}, error) {
	pinned := make(map[string]struct{})
	if l.BlobStore == nil {
		return pinned, nil
	}
	markerKeys := make([]string, 0)
	for _, prefix := range []string{BackupPrefix(kbID), SnapshotPrefix(kbID), BranchPrefix(kbID)} {
		objects, err := l.BlobStore.List(ctx, prefix)
		if err != nil {
			return nil, fmt.Errorf("list backup markers for shard gc %q: %w", kbID, err)
		}
		for _, obj := range objects {
			markerKeys = append(markerKeys, obj.Key)
		}
	}
	for _, key := range markerKeys {
		data, err := l.BlobStore.DownloadBytes(ctx, key)
		if err != nil {
			continue
		}
		var doc struct {
			Shards []struct {
				Key string `json:"key"`
			} `json:"shards"`
			Manifest struct {
				Shards []struct {
					Key string `json:"key"`
				} `json:"shards"`
			} `json:"manifest"`
			ManifestSnapshot struct {
				Shards []struct {
					Key string `json:"key"`
				} `json:"shards"`
			} `json:"manifest_snapshot"`
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			continue
		}
		for _, s := range doc.Shards {
			if s.Key != "" {
				pinned[s.Key] = struct{}{}
			}
		}
		for _, s := range doc.Manifest.Shards {
			if s.Key != "" {
				pinned[s.Key] = struct{}{}
			}
		}
		for _, s := range doc.ManifestSnapshot.Shards {
			if s.Key != "" {
				pinned[s.Key] = struct{}{}
			}
		}
	}
	return pinned, nil
}

// CloneKBFromBackup byte-copies every shard pinned by a backup descriptor
// into a new KB prefix and publishes the target manifest with
// verify-before-publish. The source KB is never mutated.
//
// Concurrency fencing: the target namespace is occupancy-checked up front
// (manifest Head plus a List of the destination prefix), every staged shard
// is written CreateOnly, and the manifest publish itself is CreateOnly — a
// second concurrent clone to the same target fails with ErrBackupExists
// instead of overwriting the winner. A version-mismatch on publish is never
// retried with an unconditional put, because that retry would overwrite the
// winner's manifest.
//
// Rollback: failures before the manifest publish remove only the staged
// shards this attempt created (cleanup errors are joined into the returned
// error, never swallowed). The target manifest is deleted only when this
// attempt published it; an "already exists" failure never deletes the
// winner's manifest.
func (l *KB) CloneKBFromBackup(ctx context.Context, srcKBID, backupID, dstKBID string) error {
	if err := validateKBID(srcKBID); err != nil {
		return fmt.Errorf("source %v", err)
	}
	if err := validateKBID(dstKBID); err != nil {
		return fmt.Errorf("target %v", err)
	}
	if srcKBID == dstKBID {
		return fmt.Errorf("clone target must differ from source")
	}
	if err := validateBackupID("backup", backupID); err != nil {
		return err
	}
	if l.ManifestStore == nil {
		return fmt.Errorf("manifest store is not configured")
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	desc, err := l.GetBackup(ctx, srcKBID, backupID)
	if err != nil {
		return err
	}
	if err := checkManifestFormatSupported(&desc.ManifestSnapshot); err != nil {
		return err
	}
	if err := l.rejectTombstonedTarget(ctx, dstKBID); err != nil {
		return err
	}
	// Pre-publish fencing: refuse a target namespace that already holds a
	// manifest or any staged/leftover object. The manifest CreateOnly put
	// below remains the atomic commit point; this check is the fast path
	// that also keeps a retry from silently reusing another attempt's
	// leftovers.
	if occupied, err := l.cloneTargetOccupied(ctx, dstKBID); err != nil {
		return err
	} else if occupied {
		return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
	}
	// Every staged copy must land on a distinct key; two refs remapping to
	// one destination would let the second silently overwrite the first.
	seenDst := make(map[string]struct{}, len(desc.Shards))
	for _, ref := range desc.Shards {
		dstKey := remapShardKey(srcKBID, dstKBID, ref.Key)
		if _, dup := seenDst[dstKey]; dup {
			return fmt.Errorf("%w: clone remap collides on target key %q", ErrBackupCorrupt, dstKey)
		}
		seenDst[dstKey] = struct{}{}
	}
	copied := make([]SnapshotShardMetadata, 0, len(desc.Shards))
	copiedKeys := make([]string, 0, len(desc.Shards))
	published := false
	fail := func(mainErr error) error {
		errs := []error{mainErr}
		if cleanupErr := l.deleteStagedShards(ctx, copiedKeys); cleanupErr != nil {
			errs = append(errs, cleanupErr)
		}
		// Only unwind a manifest this attempt published. Deleting on an
		// "already exists" failure would destroy the winner's manifest.
		if published {
			if err := l.ManifestStore.Delete(ctx, dstKBID); err != nil {
				errs = append(errs, fmt.Errorf("rollback target manifest %q: %w", dstKBID, err))
			}
		}
		return errors.Join(errs...)
	}
	for _, ref := range desc.Shards {
		dstKey := remapShardKey(srcKBID, dstKBID, ref.Key)
		// Server-side copy with CreateOnly: a concurrent clone staging the
		// same target loses here with ErrBackupExists instead of
		// overwriting our bytes later. Staged bytes are still re-hashed by
		// verifyStagedClone below, so a corrupt source fails the restore
		// before any manifest is published.
		info, err := l.copyShardCreateOnly(ctx, ref.Key, dstKey)
		if err != nil {
			if errors.Is(err, ErrBackupExists) {
				return fail(fmt.Errorf("%w: clone target %q is already being staged: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch))
			}
			if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
				return fail(fmt.Errorf("%w: source shard %s is missing: %w", ErrBackupCorrupt, ref.Key, err))
			}
			return fail(fmt.Errorf("copy source shard %s: %w", ref.Key, err))
		}
		copiedKeys = append(copiedKeys, dstKey)
		copied = append(copied, SnapshotShardMetadata{
			Key:       dstKey,
			Version:   info.Version,
			SizeBytes: info.Size,
			SHA256:    ref.SHA256,
		})
	}
	// Verify-before-publish: re-Head every staged object for size, then
	// re-hash bytes. A corrupt object fails the restore before any manifest
	// is published.
	if err := l.verifyStagedClone(ctx, copied); err != nil {
		return fail(err)
	}
	srcManifest := desc.ManifestSnapshot
	// A short stage is corruption, never a prefix to publish: fail instead of
	// silently dropping the tail of the manifest.
	if len(copied) != len(srcManifest.Shards) {
		return fail(fmt.Errorf("%w: staged %d shards for %d manifest entries",
			ErrBackupCorrupt, len(copied), len(srcManifest.Shards)))
	}
	clonedShards := make([]SnapshotShardMetadata, 0, len(srcManifest.Shards))
	for i, shard := range srcManifest.Shards {
		if i >= len(copied) {
			return fail(fmt.Errorf("%w: staged shards truncated at index %d of %d",
				ErrBackupCorrupt, i, len(srcManifest.Shards)))
		}
		next := shard
		next.Key = copied[i].Key
		next.Version = copied[i].Version
		next.SizeBytes = copied[i].SizeBytes
		next.SHA256 = copied[i].SHA256
		clonedShards = append(clonedShards, next)
	}
	var totalSize int64
	for _, shard := range clonedShards {
		totalSize += shard.SizeBytes
	}
	nextManifest := SnapshotShardManifest{
		SchemaVersion:  srcManifest.SchemaVersion,
		Layout:         srcManifest.Layout,
		FormatKind:     srcManifest.FormatKind,
		FormatVersion:  srcManifest.FormatVersion,
		KBID:           dstKBID,
		CreatedAt:      nowFrom(l.Clock),
		TotalSizeBytes: totalSize,
		Shards:         clonedShards,
	}
	// Re-check the manifest after the (slow) stage: a concurrent clone may
	// have committed while we were copying. This must be a manifest-only
	// check — our own staged shards already sit under the destination prefix,
	// so a prefix List here would always self-trigger. Publishing CreateOnly
	// remains the atomic commit; this is just the fast path around a doomed
	// publish.
	if head, err := l.ManifestStore.HeadVersion(ctx, dstKBID); err != nil {
		return fail(fmt.Errorf("head target manifest %q: %w", dstKBID, err))
	} else if head != "" {
		return fail(fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch))
	}
	if err := l.publishCloneManifestCreateOnly(ctx, dstKBID, nextManifest); err != nil {
		return fail(err)
	}
	published = true
	l.recordCloneOrigin(ctx, dstKBID, srcKBID, backupID)
	return nil
}

// cloneTargetOccupied reports whether the destination namespace already holds
// a manifest or any other object. It is a fast-path fence only: the atomic
// commit point is the CreateOnly manifest publish.
func (l *KB) cloneTargetOccupied(ctx context.Context, dstKBID string) (bool, error) {
	head, err := l.ManifestStore.HeadVersion(ctx, dstKBID)
	if err != nil {
		return false, fmt.Errorf("head target manifest %q: %w", dstKBID, err)
	}
	if head != "" {
		return true, nil
	}
	objects, err := l.BlobStore.List(ctx, dstKBID)
	if err != nil {
		return false, fmt.Errorf("list clone target %q: %w", dstKBID, err)
	}
	return len(objects) > 0, nil
}

// publishCloneManifestCreateOnly publishes the clone manifest exactly once.
// An existing target reports ErrBackupExists (wrapping ErrBlobVersionMismatch)
// and is never overwritten: a publish conflict means a concurrent clone won,
// so the loser fails instead of rebasing and stomping the winner.
func (l *KB) publishCloneManifestCreateOnly(ctx context.Context, dstKBID string, next SnapshotShardManifest) error {
	key := manifest.ShardManifestKey(dstKBID)
	if ms, ok := l.ManifestStore.(*BlobManifestStore); ok && ms != nil {
		data, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if ex, ok := ms.Store.(interface {
			UploadBytesIfNotExists(ctx context.Context, key string, data []byte) (*blobstore.ObjectInfo, error)
		}); ok {
			if _, err := ex.UploadBytesIfNotExists(ctx, key, data); err != nil {
				if errors.Is(err, blobstore.ErrVersionMismatch) {
					return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
				}
				return fmt.Errorf("publish target manifest %q: %w", dstKBID, err)
			}
			return nil
		}
		if local, ok := ms.Store.(*blobstore.LocalBlobStore); ok {
			tmp, err := os.CreateTemp("", "minnow-clonemanifest-*")
			if err != nil {
				return err
			}
			tmpName := tmp.Name()
			if _, err := tmp.Write(data); err != nil {
				_ = tmp.Close()
				_ = os.Remove(tmpName)
				return err
			}
			if err := tmp.Close(); err != nil {
				_ = os.Remove(tmpName)
				return err
			}
			defer os.Remove(tmpName)
			if _, err := local.UploadIfNotExists(ctx, key, tmpName); err != nil {
				if errors.Is(err, blobstore.ErrVersionMismatch) {
					return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
				}
				return fmt.Errorf("publish target manifest %q: %w", dstKBID, err)
			}
			return nil
		}
		// Generic blob-backed manifest store: lease-fenced Head-then-put.
		// The write lease serializes publishers sharing the lease manager
		// (cross-process when the manager is shared); the stripe covers
		// threads sharing this process. A cross-process rival can still win
		// the race without a shared lease, in which case the put below must
		// surface the conflict instead of overwriting (same residual as
		// uploadBytesCreateOnly).
		mgr, lease, leaseErr := l.AcquireWriteLease(ctx, "backup-createonly/"+key)
		if leaseErr != nil {
			if errors.Is(leaseErr, ErrWriteLeaseConflict) {
				return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
			}
			return fmt.Errorf("acquire publish lease for %q: %w", dstKBID, leaseErr)
		}
		defer func() { _ = mgr.Release(context.Background(), lease) }()
		mu := l.LockFor("backup-createonly/" + key)
		mu.Lock()
		defer mu.Unlock()
		if cur, err := ms.HeadVersion(ctx, dstKBID); err != nil {
			return fmt.Errorf("head target manifest %q: %w", dstKBID, err)
		} else if cur != "" {
			return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
		}
		if _, err := ms.UpsertIfMatch(ctx, dstKBID, next, ""); err != nil {
			if errors.Is(err, ErrBlobVersionMismatch) {
				return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
			}
			return fmt.Errorf("publish target manifest %q: %w", dstKBID, err)
		}
		return nil
	}
	// Mongo-backed (and other version-fenced) manifest stores treat a single
	// UpsertIfMatch with an empty expected version as insert-only: a duplicate
	// key reports ErrVersionMismatch atomically, so one call is the fence.
	// Custom stores without an atomic insert remain best-effort here.
	if cur, err := l.ManifestStore.HeadVersion(ctx, dstKBID); err != nil {
		return fmt.Errorf("head target manifest %q: %w", dstKBID, err)
	} else if cur != "" {
		return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
	}
	if _, err := l.ManifestStore.UpsertIfMatch(ctx, dstKBID, next, ""); err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			return fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
		}
		return fmt.Errorf("publish target manifest %q: %w", dstKBID, err)
	}
	return nil
}

// deleteStagedShards removes staged clone objects created by this attempt.
// Errors are joined (never swallowed) so operators see partial rollback.
func (l *KB) deleteStagedShards(ctx context.Context, keys []string) error {
	var errs []error
	for _, key := range keys {
		if err := l.BlobStore.Delete(ctx, key); err != nil {
			errs = append(errs, fmt.Errorf("rollback staged shard %s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// RestoreBackupToNewKB is CloneKBFromBackup under the restore name: a
// restore always targets a new KB id and never mutates the source.
func (l *KB) RestoreBackupToNewKB(ctx context.Context, srcKBID, backupID, dstKBID string) error {
	return l.CloneKBFromBackup(ctx, srcKBID, backupID, dstKBID)
}

func (l *KB) verifyStagedClone(ctx context.Context, staged []SnapshotShardMetadata) error {
	for _, shard := range staged {
		info, err := l.BlobStore.Head(ctx, shard.Key)
		if err != nil {
			return fmt.Errorf("%w: staged shard %s is missing: %w", ErrBackupCorrupt, shard.Key, err)
		}
		if info.Size != shard.SizeBytes {
			return fmt.Errorf("%w: staged shard %s size %d does not match %d",
				ErrBackupCorrupt, shard.Key, info.Size, shard.SizeBytes)
		}
		raw, err := l.BlobStore.DownloadBytes(ctx, shard.Key)
		if err != nil {
			return fmt.Errorf("re-read staged shard %s: %w", shard.Key, err)
		}
		if got := blobstore.BytesSHA256(raw); got != shard.SHA256 {
			return fmt.Errorf("%w: staged shard %s failed sha256 verification", ErrBackupCorrupt, shard.Key)
		}
	}
	return nil
}

// recordCloneOrigin writes the clone lineage marker best-effort. A marker
// failure never fails the clone; the manifest publish above is the commit
// point and rollback covers the staged shards plus a manifest this attempt
// published (never a rival winner's manifest).
func (l *KB) recordCloneOrigin(ctx context.Context, dstKBID, srcKBID, backupID string) {
	head, err := l.ManifestStore.HeadVersion(ctx, dstKBID)
	if err != nil {
		head = ""
	}
	rec := CloneOriginRecord{
		RecordVersion:   1,
		KBID:            dstKBID,
		SourceKBID:      srcKBID,
		SourceBackupID:  backupID,
		CreatedAt:       nowFrom(l.Clock),
		ManifestVersion: head,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_, _ = l.uploadBytesCreateOnly(ctx, CloneOriginKey(dstKBID), data)
}
