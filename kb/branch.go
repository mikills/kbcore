// Phase 2 zero-copy branch and restore.
//
// A branch creates a new KB whose manifest references the source shard keys
// verbatim: no shard bytes are copied. A zero-copy restore publishes a new
// KB from a backup descriptor the same way. Both are gated on reachability
// GC (shard_gc.go): the live set unions live manifests, backup descriptors,
// branch markers, journal pendings, and reader pins, so a shard the live
// manifest drops but a branch still names is never deleted.
//
// Cross-prefix references (same-KB-only refs until a global ref table
// exists): shard keys carry their owner's KB prefix, so a branch manifest in
// the target prefix names keys under the source prefix. Every branch/restore
// writes ONE marker under the source prefix plus one owner entry per shard
// key in the global ref table (ref_table.go); GC unions the ref table with
// legacy per-owner fan-out markers on read. A future change could drop the
// legacy read path once no old markers remain; until then same-KB refs are
// the common case and cross-prefix refs are explicit in the ref table.
//
// Publish model is Write-Audit-Publish (WAP) like the snapshot path: the
// pin marker is written first (audit: pins exist before the new manifest
// points at shared bytes), then the target manifest publishes CreateOnly.
// On an ErrVersionMismatch conflict the loser fails with ErrBackupExists
// (wrapping ErrBlobVersionMismatch) and never rebases over the winner:
// branch targets are create-only namespaces.
package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

const (
	// BranchRecordVersion is the only branch marker version Phase 2 writes.
	BranchRecordVersion = 1

	branchKeyInfix  = ".branches/"
	branchKeySuffix = ".branch.json"
)

// ErrKBTombstoned is returned when a write targets a tombstoned KB id.
var ErrKBTombstoned = errors.New("knowledge base is tombstoned")

// BranchRecord is the immutable per-owner pin marker for one zero-copy
// branch or restore. Shard bytes are shared by key reference.
type BranchRecord struct {
	RecordVersion         int              `json:"record_version"`
	BranchID              string           `json:"branch_id"`
	SourceKBID            string           `json:"source_kb_id"`
	TargetKBID            string           `json:"target_kb_id"`
	ParentBranchID        string           `json:"parent_branch_id,omitempty"`
	SourceManifestVersion string           `json:"source_manifest_version"`
	CreatedAt             time.Time        `json:"created_at"`
	Shards                []BackupShardRef `json:"shards"`
	RecordSHA256          string           `json:"record_sha256"`
}

// BranchRecordKey returns the blob key for a branch marker owned by ownerKBID.
func BranchRecordKey(ownerKBID, branchID string) string {
	return ownerKBID + branchKeyInfix + branchID + branchKeySuffix
}

// BranchPrefix returns the listing prefix owning a KB's branch markers.
func BranchPrefix(kbID string) string { return kbID + branchKeyInfix }

func branchRecordChecksum(rec *BranchRecord) (string, error) {
	shadow := *rec
	shadow.RecordSHA256 = ""
	data, err := json.Marshal(shadow)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateBranchRecord checks the structural invariants and self-checksum.
func ValidateBranchRecord(rec *BranchRecord) error {
	if rec == nil {
		return fmt.Errorf("%w: branch record is nil", ErrBackupCorrupt)
	}
	if rec.RecordVersion != BranchRecordVersion {
		return fmt.Errorf("%w: unknown branch record_version %d", ErrBackupCorrupt, rec.RecordVersion)
	}
	if err := validateBackupID("branch", rec.BranchID); err != nil {
		return fmt.Errorf("%w: %w", ErrBackupCorrupt, err)
	}
	if strings.TrimSpace(rec.SourceKBID) == "" || strings.TrimSpace(rec.TargetKBID) == "" {
		return fmt.Errorf("%w: branch source and target kb ids are required", ErrBackupCorrupt)
	}
	if rec.SourceKBID == rec.TargetKBID {
		return fmt.Errorf("%w: branch source and target must differ", ErrBackupCorrupt)
	}
	if len(rec.Shards) == 0 {
		return fmt.Errorf("%w: branch record has no shards", ErrBackupCorrupt)
	}
	seen := make(map[string]struct{}, len(rec.Shards))
	for _, shard := range rec.Shards {
		if strings.TrimSpace(shard.Key) == "" {
			return fmt.Errorf("%w: branch shard has empty key", ErrBackupCorrupt)
		}
		if _, dup := seen[shard.Key]; dup {
			return fmt.Errorf("%w: duplicate branch shard key %q", ErrBackupCorrupt, shard.Key)
		}
		seen[shard.Key] = struct{}{}
	}
	want, err := branchRecordChecksum(rec)
	if err != nil {
		return err
	}
	if !hmacEqual(want, rec.RecordSHA256) {
		return fmt.Errorf("%w: branch record_sha256 mismatch (tampered or truncated)", ErrBackupCorrupt)
	}
	return nil
}

// branchKeyOwners returns the distinct blob-namespace owners of keys: the KB
// prefix parsed from each shard key, or srcKBID when a key carries no
// parseable owner. Markers fan out to every owner so per-prefix GC scans
// stay correct for cross-prefix references.
func branchKeyOwners(srcKBID string, keys []string) []string {
	owners := make(map[string]struct{})
	for _, key := range keys {
		if owner, ok := kbIDFromShardKey(key); ok {
			owners[owner] = struct{}{}
			continue
		}
		owners[srcKBID] = struct{}{}
	}
	out := make([]string, 0, len(owners))
	for owner := range owners {
		out = append(out, owner)
	}
	sort.Strings(out)
	return out
}

// BranchKB creates dstKBID as a zero-copy branch of srcKBID: the new
// manifest references the source shard keys verbatim and no shard bytes are
// copied. parentBranchID links branch-of-branch lineage (empty for a direct
// branch); it is recorded, not resolved.
func (l *KB) BranchKB(ctx context.Context, srcKBID, branchID, dstKBID string) (*BranchRecord, error) {
	return l.BranchKBFrom(ctx, srcKBID, branchID, dstKBID, "")
}

// BranchKBFrom is BranchKB with an explicit parent branch link.
func (l *KB) BranchKBFrom(ctx context.Context, srcKBID, branchID, dstKBID, parentBranchID string) (*BranchRecord, error) {
	if err := validateKBID(srcKBID); err != nil {
		return nil, fmt.Errorf("source %v", err)
	}
	if err := validateKBID(dstKBID); err != nil {
		return nil, fmt.Errorf("target %v", err)
	}
	if srcKBID == dstKBID {
		return nil, fmt.Errorf("branch target must differ from source")
	}
	if err := validateBackupID("branch", branchID); err != nil {
		return nil, err
	}
	if parentBranchID != "" {
		if err := validateBackupID("parent branch", parentBranchID); err != nil {
			return nil, err
		}
	}
	if l.ManifestStore == nil {
		return nil, fmt.Errorf("manifest store is not configured")
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	doc, err := l.ManifestStore.Get(ctx, srcKBID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			return nil, fmt.Errorf("branch %q: %w", srcKBID, err)
		}
		return nil, err
	}
	if err := checkManifestFormatSupported(&doc.Manifest); err != nil {
		return nil, err
	}
	if err := checkShardIntegrity(doc.Manifest.Shards); err != nil {
		return nil, err
	}
	if err := l.verifyBranchSourceShards(ctx, doc.Manifest.Shards); err != nil {
		return nil, err
	}
	if err := l.rejectTombstonedTarget(ctx, dstKBID); err != nil {
		return nil, err
	}
	if occupied, err := l.cloneTargetOccupied(ctx, dstKBID); err != nil {
		return nil, err
	} else if occupied {
		return nil, fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
	}
	refs := make([]BackupShardRef, 0, len(doc.Manifest.Shards))
	for _, shard := range doc.Manifest.Shards {
		refs = append(refs, BackupShardRef{Key: shard.Key, SizeBytes: shard.SizeBytes, SHA256: shard.SHA256, Version: shard.Version})
	}
	now, err := l.clockNow()
	if err != nil {
		return nil, err
	}
	rec := &BranchRecord{
		RecordVersion:         BranchRecordVersion,
		BranchID:              branchID,
		SourceKBID:            srcKBID,
		TargetKBID:            dstKBID,
		ParentBranchID:        parentBranchID,
		SourceManifestVersion: doc.Version,
		CreatedAt:             now,
		Shards:                refs,
	}
	sum, err := branchRecordChecksum(rec)
	if err != nil {
		return nil, err
	}
	rec.RecordSHA256 = sum
	// Marker-first: pins exist before the new manifest points at shared
	// bytes, so no GC sweep can observe an unpinned reference.
	written, err := l.writeBranchMarkers(ctx, rec)
	if err != nil {
		return nil, err
	}
	rollback := func(mainErr error) error {
		errs := []error{mainErr}
		for _, key := range written {
			if delErr := l.BlobStore.Delete(ctx, key); delErr != nil {
				errs = append(errs, fmt.Errorf("rollback branch marker %s: %w", key, delErr))
			}
		}
		return errors.Join(errs...)
	}
	next := doc.Manifest
	next.KBID = dstKBID
	if now, err := l.clockNow(); err != nil {
		return nil, rollback(err)
	} else {
		next.CreatedAt = now
	}
	if err := l.publishCloneManifestCreateOnly(ctx, dstKBID, next); err != nil {
		return nil, rollback(err)
	}
	l.recordBranchOrigin(ctx, dstKBID, srcKBID, branchID)
	return rec, nil
}

// RestoreBackupZeroCopy publishes dstKBID from a backup descriptor without
// copying shard bytes: the new manifest references the source shard keys
// verbatim and a branch marker under the source prefix pins them. It shares
// the byte-copy clone's fencing (occupancy check plus CreateOnly publish;
// conflicts report ErrBackupExists wrapping ErrBlobVersionMismatch and never
// overwrite the winner).
func (l *KB) RestoreBackupZeroCopy(ctx context.Context, srcKBID, backupID, dstKBID, restoreID string) (*BranchRecord, error) {
	if err := validateKBID(srcKBID); err != nil {
		return nil, fmt.Errorf("source %v", err)
	}
	if err := validateKBID(dstKBID); err != nil {
		return nil, fmt.Errorf("target %v", err)
	}
	if srcKBID == dstKBID {
		return nil, fmt.Errorf("restore target must differ from source")
	}
	if err := validateBackupID("backup", backupID); err != nil {
		return nil, err
	}
	if err := validateBackupID("restore", restoreID); err != nil {
		return nil, err
	}
	if l.ManifestStore == nil {
		return nil, fmt.Errorf("manifest store is not configured")
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	desc, err := l.GetBackup(ctx, srcKBID, backupID)
	if err != nil {
		return nil, err
	}
	if err := checkManifestFormatSupported(&desc.ManifestSnapshot); err != nil {
		return nil, err
	}
	if err := l.verifyBackupSourceShards(ctx, desc.Shards); err != nil {
		return nil, err
	}
	if err := l.rejectTombstonedTarget(ctx, dstKBID); err != nil {
		return nil, err
	}
	if occupied, err := l.cloneTargetOccupied(ctx, dstKBID); err != nil {
		return nil, err
	} else if occupied {
		return nil, fmt.Errorf("%w: target manifest %q already exists: %w", ErrBackupExists, dstKBID, ErrBlobVersionMismatch)
	}
	now, err := l.clockNow()
	if err != nil {
		return nil, err
	}
	rec := &BranchRecord{
		RecordVersion:         BranchRecordVersion,
		BranchID:              restoreID,
		SourceKBID:            srcKBID,
		TargetKBID:            dstKBID,
		ParentBranchID:        backupID,
		SourceManifestVersion: desc.SourceManifestVersion,
		CreatedAt:             now,
		Shards:                append([]BackupShardRef(nil), desc.Shards...),
	}
	sum, err := branchRecordChecksum(rec)
	if err != nil {
		return nil, err
	}
	rec.RecordSHA256 = sum
	written, err := l.writeBranchMarkers(ctx, rec)
	if err != nil {
		return nil, err
	}
	rollback := func(mainErr error) error {
		errs := []error{mainErr}
		for _, key := range written {
			if delErr := l.BlobStore.Delete(ctx, key); delErr != nil {
				errs = append(errs, fmt.Errorf("rollback restore marker %s: %w", key, delErr))
			}
		}
		return errors.Join(errs...)
	}
	next := desc.ManifestSnapshot
	next.KBID = dstKBID
	if now, err := l.clockNow(); err != nil {
		return nil, rollback(err)
	} else {
		next.CreatedAt = now
	}
	if err := l.publishCloneManifestCreateOnly(ctx, dstKBID, next); err != nil {
		return nil, rollback(err)
	}
	l.recordBranchOrigin(ctx, dstKBID, srcKBID, restoreID)
	return rec, nil
}

// verifyBranchSourceShards Heads every shard a zero-copy branch would
// reference before any marker or manifest is published. A missing source
// fails with ErrBackupCorrupt (never a publish of a dangling reference);
// size mismatches against the manifest record fail the same way. SHA
// integrity of the bytes themselves is enforced on read (shardcache
// VerifyDownloaded) rather than by re-downloading every source here.
func (l *KB) verifyBranchSourceShards(ctx context.Context, shards []SnapshotShardMetadata) error {
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	for _, shard := range shards {
		if strings.TrimSpace(shard.Key) == "" {
			return fmt.Errorf("%w: branch source shard has empty key", ErrBackupCorrupt)
		}
		info, err := l.BlobStore.Head(ctx, shard.Key)
		if err != nil {
			if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: branch source shard %s is missing: %w", ErrBackupCorrupt, shard.Key, err)
			}
			return fmt.Errorf("head branch source shard %s: %w", shard.Key, err)
		}
		if shard.SizeBytes > 0 && info.Size != shard.SizeBytes {
			return fmt.Errorf("%w: branch source shard %s size %d does not match manifest %d",
				ErrBackupCorrupt, shard.Key, info.Size, shard.SizeBytes)
		}
	}
	return nil
}

// verifyBackupSourceShards is verifyBranchSourceShards for backup-descriptor
// refs used by zero-copy restore.
func (l *KB) verifyBackupSourceShards(ctx context.Context, refs []BackupShardRef) error {
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	for _, ref := range refs {
		if strings.TrimSpace(ref.Key) == "" {
			return fmt.Errorf("%w: restore source shard has empty key", ErrBackupCorrupt)
		}
		info, err := l.BlobStore.Head(ctx, ref.Key)
		if err != nil {
			if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: restore source shard %s is missing: %w", ErrBackupCorrupt, ref.Key, err)
			}
			return fmt.Errorf("head restore source shard %s: %w", ref.Key, err)
		}
		if ref.SizeBytes > 0 && info.Size != ref.SizeBytes {
			return fmt.Errorf("%w: restore source shard %s size %d does not match descriptor %d",
				ErrBackupCorrupt, ref.Key, info.Size, ref.SizeBytes)
		}
	}
	return nil
}

// writeBranchMarkers persists ONE record under the source prefix with
// CreateOnly semantics plus one owner entry per shard key in the global
// ref table (ref_table.go). This replaces the old per-owner fan-out (one
// marker under every key-owner prefix): GC consults the ref table instead
// of requiring a marker under each prefix. Old fan-out markers are still
// honored on read (backupPinnedShardKeys lists every marker); only new
// writes go to the table. A concurrent branch with the same id fails with
// ErrBackupExists (wrapping ErrBlobVersionMismatch) instead of overwriting.
func (l *KB) writeBranchMarkers(ctx context.Context, rec *BranchRecord) ([]string, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	key := BranchRecordKey(rec.SourceKBID, rec.BranchID)
	if _, err := l.uploadBytesCreateOnly(ctx, key, data); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(rec.Shards))
	for _, shard := range rec.Shards {
		keys = append(keys, shard.Key)
	}
	if err := l.AddRefOwners(ctx, keys, branchRefOwner(rec)); err != nil {
		_ = l.BlobStore.Delete(ctx, key)
		return nil, fmt.Errorf("record shard references for branch %q: %w", rec.BranchID, err)
	}
	return []string{key}, nil
}

// GetBranch reads a branch marker and verifies its self-checksum.
func (l *KB) GetBranch(ctx context.Context, ownerKBID, branchID string) (*BranchRecord, error) {
	if strings.TrimSpace(ownerKBID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("branch", branchID); err != nil {
		return nil, err
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	data, err := l.BlobStore.DownloadBytes(ctx, BranchRecordKey(ownerKBID, branchID))
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: branch %s/%s", ErrBackupNotFound, ownerKBID, branchID)
		}
		return nil, err
	}
	var rec BranchRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("%w: branch %s/%s: %w", ErrBackupCorrupt, ownerKBID, branchID, err)
	}
	if err := ValidateBranchRecord(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// ListBranchIDs lists branch marker ids for a KB prefix, sorted.
func (l *KB) ListBranchIDs(ctx context.Context, ownerKBID string) ([]string, error) {
	if strings.TrimSpace(ownerKBID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	objects, err := l.BlobStore.List(ctx, BranchPrefix(ownerKBID))
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(objects))
	for _, obj := range objects {
		rest, found := strings.CutPrefix(obj.Key, BranchPrefix(ownerKBID))
		if !found {
			continue
		}
		id, found := strings.CutSuffix(rest, branchKeySuffix)
		if !found || id == "" || strings.Contains(id, "/") {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// DeleteBranch removes the branch marker and releases its global ref-table
// entries. Legacy fan-out markers (one per key-owner prefix, written before
// the ref table existed) are removed best-effort from every derived owner
// prefix so migration leaves no leaked pins. When the record itself is
// unreadable the ref-table owner is still released best-effort and every
// owner prefix is swept by branch-ID suffix, so a corrupt marker cannot
// leave other-owner legacy markers behind. Shared shard bytes are untouched; GC reclaims them once no
// live manifest, backup, or remaining branch names them.
func (l *KB) DeleteBranch(ctx context.Context, ownerKBID, branchID string) error {
	if strings.TrimSpace(ownerKBID) == "" {
		return fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("branch", branchID); err != nil {
		return err
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	if rec, err := l.GetBranch(ctx, ownerKBID, branchID); err == nil && rec != nil {
		return l.deleteBranchMarkers(ctx, ownerKBID, rec)
	}
	// Unreadable marker: still release the ref-table owner best-effort so a
	// corrupt or concurrently-deleted marker cannot leak pins forever, and
	// sweep every owner prefix for legacy fan-out markers with this branch
	// ID (the record's shard keys are unavailable, so enumerate by suffix).
	_, _ = l.RemoveRefOwner(ctx, legacyBranchRefOwner(ownerKBID, branchID))
	sweepErr := l.sweepBranchMarkersByID(ctx, branchID)
	delErr := l.BlobStore.Delete(ctx, BranchRecordKey(ownerKBID, branchID))
	return errors.Join(sweepErr, delErr)
}

// sweepBranchMarkersByID best-effort deletes every branch marker with the
// given branch ID across all owner prefixes (legacy fan-out cleanup on the
// corrupt path where the record's shard keys are unreadable). A list
// failure is returned so the caller can surface it; per-key deletes are
// best-effort and joined.
func (l *KB) sweepBranchMarkersByID(ctx context.Context, branchID string) error {
	suffix := branchKeyInfix + branchID + branchKeySuffix
	objects, err := l.BlobStore.List(ctx, "")
	if err != nil {
		return fmt.Errorf("list branch markers for sweep %q: %w", branchID, err)
	}
	var errs []error
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, suffix) {
			continue
		}
		if err := l.BlobStore.Delete(ctx, obj.Key); err != nil {
			errs = append(errs, fmt.Errorf("delete branch marker %s: %w", obj.Key, err))
		}
	}
	return errors.Join(errs...)
}

// deleteBranchMarkers removes the source marker, releases the ref-table
// owner, and best-effort deletes any legacy fan-out markers under the other
// owner prefixes derived from the record's shard keys.
func (l *KB) deleteBranchMarkers(ctx context.Context, lookupOwner string, rec *BranchRecord) error {
	var errs []error
	if _, err := l.RemoveRefOwner(ctx, branchRefOwner(rec)); err != nil {
		errs = append(errs, fmt.Errorf("release shard references for branch %q: %w", rec.BranchID, err))
	}
	keys := make([]string, 0, len(rec.Shards))
	for _, shard := range rec.Shards {
		keys = append(keys, shard.Key)
	}
	owners := branchKeyOwners(rec.SourceKBID, keys)
	seen := make(map[string]struct{}, len(owners)+1)
	for _, owner := range owners {
		seen[owner] = struct{}{}
	}
	seen[lookupOwner] = struct{}{}
	seen[rec.SourceKBID] = struct{}{}
	for owner := range seen {
		if err := l.BlobStore.Delete(ctx, BranchRecordKey(owner, rec.BranchID)); err != nil {
			errs = append(errs, fmt.Errorf("delete branch marker %s: %w", BranchRecordKey(owner, rec.BranchID), err))
		}
	}
	return errors.Join(errs...)
}

// recordBranchOrigin writes the branch lineage marker best-effort. The
// manifest publish is the commit point; a marker failure never fails the
// branch.
func (l *KB) recordBranchOrigin(ctx context.Context, dstKBID, srcKBID, branchID string) {
	head, err := l.ManifestStore.HeadVersion(ctx, dstKBID)
	if err != nil {
		head = ""
	}
	now, err := l.clockNow()
	if err != nil {
		return
	}
	rec := CloneOriginRecord{
		RecordVersion:   1,
		KBID:            dstKBID,
		SourceKBID:      srcKBID,
		SourceBackupID:  "",
		SourceBranchID:  branchID,
		CreatedAt:       now,
		ManifestVersion: head,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_, _ = l.uploadBytesCreateOnly(ctx, CloneOriginKey(dstKBID), data)
}

// copyShardCreateOnly server-side copies one shard into the clone target
// with CreateOnly fencing. A concurrent clone staging the same target loses
// with ErrBackupExists (wrapping ErrBlobVersionMismatch) instead of
// overwriting. Staged bytes are still verified by verifyStagedClone before
// any manifest publishes, so a corrupt source fails the restore.
func (l *KB) copyShardCreateOnly(ctx context.Context, srcKey, dstKey string) (*blobstore.ObjectInfo, error) {
	info, err := l.BlobStore.Copy(ctx, srcKey, dstKey, blobstore.CopyOptions{CreateOnly: true})
	if err != nil {
		if errors.Is(err, blobstore.ErrVersionMismatch) {
			return nil, fmt.Errorf("%w: clone target key %q is already being staged: %w", ErrBackupExists, dstKey, ErrBlobVersionMismatch)
		}
		return nil, err
	}
	return info, nil
}
