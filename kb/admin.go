package kb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mikills/minnow/kb/cacheevict"
)

// DeleteKnowledgeBase removes the manifest and known shard/cache/media state for
// a KB. Event history is intentionally retained for auditability.
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

	if err := l.ManifestStore.Delete(ctx, kbID); err != nil {
		return fmt.Errorf("delete manifest %q: %w", kbID, err)
	}

	cleanupErrs := l.cleanupDeletedKB(ctx, kbID, manifest)

	if len(cleanupErrs) > 0 {
		return fmt.Errorf("knowledge base %q deleted with cleanup errors: %w", kbID, errors.Join(cleanupErrs...))
	}
	return nil
}

func (l *KB) cleanupDeletedKB(ctx context.Context, kbID string, manifest *ManifestDocument) []error {
	var cleanupErrs []error
	cleanupErrs = append(cleanupErrs, l.deleteManifestShards(ctx, manifest)...)
	cleanupErrs = append(cleanupErrs, l.deleteKBCache(kbID)...)
	cleanupErrs = append(cleanupErrs, l.deleteKBMedia(ctx, kbID)...)
	return cleanupErrs
}

func (l *KB) deleteManifestShards(ctx context.Context, manifest *ManifestDocument) []error {
	if manifest == nil {
		return nil
	}
	var errs []error
	for _, shard := range manifest.Manifest.Shards {
		if shard.Key == "" {
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
	_, total := cacheevict.ScanEntries(l.CacheDir)
	l.recordCacheBytesCurrent(total)
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
	l.recordCacheBytesCurrent(0)
	return nil
}
