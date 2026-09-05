package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/cacheevict"
)

const kbTombstoneSuffix = ".tombstone.json"

// KBTombstoneKey returns the blob key for a KB's deletion tombstone.
func KBTombstoneKey(kbID string) string { return kbID + kbTombstoneSuffix }

// KBTombstone marks a KB id as deleted. While a tombstone is present the
// shard reconcile skips the KBID entirely: leftover blobs under its prefixes
// are delete-pending but ownership is uncertain (a branch marker may have
// raced the delete guard), so GC keeps its hands off until an operator
// clears the tombstone. DeleteKnowledgeBase writes the tombstone before the
// manifest delete and removes it again when cleanup fully succeeds, so a
// surviving tombstone always means "delete incomplete, investigate, then
// ClearTombstone plus reconcile to drain verified leftovers".
type KBTombstone struct {
	Version   int       `json:"version"`
	KBID      string    `json:"kb_id"`
	DeletedAt time.Time `json:"deleted_at"`
	Reason    string    `json:"reason,omitempty"`
}

// TombstoneKnowledgeBase records a deletion tombstone for kbID.
func (l *KB) TombstoneKnowledgeBase(ctx context.Context, kbID, reason string) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kb_id required")
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	rec := KBTombstone{Version: 1, KBID: kbID, DeletedAt: nowFrom(l.Clock), Reason: reason}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = l.BlobStore.UploadBytesIfMatch(ctx, KBTombstoneKey(kbID), data, "")
	return err
}

// IsTombstoned reports whether kbID carries a deletion tombstone. A missing
// blob store means "unknown": it returns false so callers without storage
// keep their existing behavior, while reconcile (which always has a store)
// fails closed on list errors instead.
func (l *KB) IsTombstoned(ctx context.Context, kbID string) (bool, error) {
	if strings.TrimSpace(kbID) == "" {
		return false, fmt.Errorf("kb_id required")
	}
	if l.BlobStore == nil {
		return false, nil
	}
	_, err := l.BlobStore.Head(ctx, KBTombstoneKey(kbID))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("head tombstone for %q: %w", kbID, err)
}

// ClearTombstone removes a KB's deletion tombstone, re-admitting the KBID
// to reconcile and to clone/branch/restore targets.
func (l *KB) ClearTombstone(ctx context.Context, kbID string) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kb_id required")
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	return l.BlobStore.Delete(ctx, KBTombstoneKey(kbID))
}

// rejectTombstonedTarget refuses to publish a new manifest under a
// tombstoned KB id: leftovers from the deleted generation may still sit
// under its prefixes, and resurrecting the id would orphan or confuse them.
func (l *KB) rejectTombstonedTarget(ctx context.Context, kbID string) error {
	tombstoned, err := l.IsTombstoned(ctx, kbID)
	if err != nil {
		return err
	}
	if tombstoned {
		return fmt.Errorf("%w: %q", ErrKBTombstoned, kbID)
	}
	return nil
}

// DeleteKnowledgeBase removes the manifest and known shard/cache/media state for
// a KB. Event history is intentionally retained for auditability.
//
// Guard limits: HasBackupsOrBranches runs before the manifest delete, so a
// backup/snapshot/branch created concurrently between the check and the
// delete is not stopped (guard→delete TOCTOU) — quiesce backup writers before
// deleting a KB when that matters. A direct ManifestStore.Delete bypasses the
// guard entirely; only this method enforces it. Shard deletes additionally
// re-check the pin set after the manifest delete and skip pinned bytes, so
// a marker that lands in the race still keeps its bytes; GC reclaims them
// once the markers are gone.
//
// Tombstone: the delete writes a tombstone marker BEFORE the manifest delete
// and removes it after fully successful cleanup. A surviving tombstone means
// the delete was partial; while present, reconcile skips the KBID (no new
// orphan queueing) and clone/branch/restore refuse it as a target. Tombstoned
// entries already in the sweep queue are dropped (reconcile re-derives them
// after the operator clears the tombstone), so the queue cannot retry them
// forever. Operators investigate, then ClearTombstone plus reconcile to drain
// verified leftovers.
//
// Ordering: the manifest is deleted FIRST so callers see a consistent "gone"
// state even if downstream blob/cache/media cleanup partially fails. After the
// manifest is deleted, shard, cache, and media cleanup are best-effort: every
// step runs to completion and any failures are joined into the returned error
// so operators see the full picture instead of just the first failure. Orphan
// blobs left behind by a partial failure can be reclaimed by the GC sweep.
func (l *KB) DeleteKnowledgeBase(ctx context.Context, kbID string) error {
	if kbID == "" {
		return fmt.Errorf("kb_id required")
	}
	if l.ManifestStore == nil {
		return fmt.Errorf("manifest store is not configured")
	}

	manifest, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil && !errors.Is(err, ErrManifestNotFound) {
		return fmt.Errorf("read manifest %q: %w", kbID, err)
	}

	// Backups, same-KB snapshots, and branch markers pin shard bytes by key.
	// Deleting the KB while markers exist would orphan those records, so
	// refuse until the operator deletes backups/snapshots/branches first.
	if blocked, guardErr := l.HasBackupsOrBranches(ctx, kbID); guardErr != nil {
		return fmt.Errorf("check backups for %q: %w", kbID, guardErr)
	} else if blocked {
		return fmt.Errorf("%w: %q", ErrDeleteBlockedByBackups, kbID)
	}

	var tombstoneErr error
	tombstoned := false
	if l.BlobStore != nil {
		if tombstoneErr = l.TombstoneKnowledgeBase(ctx, kbID, "delete"); tombstoneErr == nil {
			tombstoned = true
		}
	}

	if err := l.ManifestStore.Delete(ctx, kbID); err != nil {
		if tombstoned {
			if clearErr := l.ClearTombstone(ctx, kbID); clearErr != nil {
				return errors.Join(
					fmt.Errorf("delete manifest %q: %w", kbID, err),
					fmt.Errorf("rollback tombstone for %q: %w", kbID, clearErr),
				)
			}
		}
		return fmt.Errorf("delete manifest %q: %w", kbID, err)
	}

	cleanupErrs := l.cleanupDeletedKB(ctx, kbID, manifest)
	if tombstoneErr != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("write tombstone for %q: %w", kbID, tombstoneErr))
	}
	// Fully successful cleanup leaves no tombstone: the id is clean and
	// immediately reusable. A surviving tombstone always signals partial
	// cleanup for reconcile to skip.
	if len(cleanupErrs) == 0 && tombstoned {
		_ = l.ClearTombstone(ctx, kbID)
	}

	if len(cleanupErrs) > 0 {
		return fmt.Errorf("knowledge base %q deleted with cleanup errors: %w", kbID, errors.Join(cleanupErrs...))
	}
	return nil
}

func (l *KB) cleanupDeletedKB(ctx context.Context, kbID string, manifest *ManifestDocument) []error {
	var cleanupErrs []error
	cleanupErrs = append(cleanupErrs, l.deleteManifestShards(ctx, kbID, manifest)...)
	cleanupErrs = append(cleanupErrs, l.deleteKBCache(kbID)...)
	cleanupErrs = append(cleanupErrs, l.deleteKBMedia(ctx, kbID)...)
	cleanupErrs = append(cleanupErrs, l.deleteKBScopes(ctx, kbID)...)
	return cleanupErrs
}

// deleteManifestShards removes shard bytes named by a deleted manifest,
// except bytes still pinned by backup/snapshot/branch markers, journal
// pendings, or reader pins. The manifest is already gone at this point, so
// a marker that won the guard→delete race must still pin its bytes here:
// pinned keys are skipped and left for GC. A pin-list failure fails closed
// (no shard deletes) so a transient list error cannot orphan pinned bytes.
func (l *KB) deleteManifestShards(ctx context.Context, kbID string, manifest *ManifestDocument) []error {
	if manifest == nil {
		return nil
	}
	if l.BlobStore == nil {
		return nil
	}
	pinned, err := l.pinnedKeysForGC(ctx, kbID, nil)
	if err != nil {
		return []error{fmt.Errorf("read pinned shards for kb delete %q: %w", kbID, err)}
	}
	var errs []error
	for _, shard := range manifest.Manifest.Shards {
		if shard.Key == "" {
			continue
		}
		if _, ok := pinned[shard.Key]; ok {
			continue
		}
		if err := l.BlobStore.Delete(ctx, shard.Key); err != nil {
			errs = append(errs, fmt.Errorf("delete shard %s: %w", shard.Key, err))
		}
	}
	return errs
}

func (l *KB) deleteKBCache(kbID string) []error {
	if l.CacheDir == "" {
		return nil
	}
	var errs []error
	path := filepath.Join(l.CacheDir, kbID)
	if !l.removeCacheEntry(cacheevict.Entry{KBID: kbID, Path: path}, true) {
		errs = append(errs, fmt.Errorf("remove cache dir for %q", kbID))
	}
	_, current, held := cacheevict.ScanEntriesWithHeld(l.CacheDir, l.cacheEntryHoldsLiveState)
	l.recordCacheUsage(current, held)
	return errs
}

func (l *KB) deleteKBMedia(ctx context.Context, kbID string) []error {
	if l.MediaStore == nil {
		return nil
	}
	var errs []error
	for after := ""; ; {
		page, listErr := l.MediaStore.List(ctx, kbID, "", after, 500)
		if listErr != nil {
			return append(errs, fmt.Errorf("list media for kb delete: %w", listErr))
		}
		errs = append(errs, l.deleteMediaPage(ctx, page.Items)...)
		if page.NextToken == "" {
			return errs
		}
		after = page.NextToken
	}
}

func (l *KB) deleteMediaPage(ctx context.Context, items []MediaObject) []error {
	var errs []error
	for _, item := range items {
		if err := l.MediaStore.Delete(ctx, item.ID); err != nil {
			errs = append(errs, fmt.Errorf("delete media %s: %w", item.ID, err))
		}
	}
	return errs
}

// TombstoneMedia marks a media object as deleted without removing its metadata.
func (l *KB) TombstoneMedia(ctx context.Context, mediaID string) error {
	if mediaID == "" {
		return fmt.Errorf("media_id required")
	}
	if l.MediaStore == nil {
		return fmt.Errorf("media subsystem not configured")
	}
	return l.MediaStore.UpdateState(ctx, mediaID, MediaStateTombstoned, l.Clock.Now().UnixMilli())
}

// ClearCache removes all local cache entries regardless of TTL or size policy.
func (l *KB) ClearCache() error {
	if l.CacheDir == "" {
		return nil
	}
	entries, err := os.ReadDir(l.CacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if cacheevict.IsControlEntry(entry.Name()) {
			continue
		}
		path := filepath.Join(l.CacheDir, entry.Name())
		if !l.removeCacheEntry(cacheevict.Entry{KBID: entry.Name(), Path: path}, true) {
			return fmt.Errorf("remove cache entry %q", entry.Name())
		}
	}
	l.recordCacheUsage(0, 0)
	return nil
}
