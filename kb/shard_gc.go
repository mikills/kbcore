// Shard GC handles garbage collection of replaced shard files after compaction
// or other shard-replacing operations.
//
// When compaction merges multiple shards into one replacement shard, the old
// shard files cannot be deleted immediately because in-flight readers may still
// be accessing them. This file implements a delayed GC mechanism that safely
// cleans up old shards after a configurable grace period.
//
// System fit:
//
//   - After compaction publishes a new manifest, replaced shards are enqueued
//     for GC via enqueueReplacedShardsForGC rather than deleted immediately.
//   - A background sweep (SweepDelayedShardGC) processes the queue, deleting
//     shards only after the grace window has elapsed.
//   - Before deletion, the sweep verifies the shard is no longer referenced in
//     the current manifest, preventing accidental deletion of active shards.
//
// Safety guarantees:
//
//   - Grace window (defaultShardGCGraceWindow) ensures in-flight readers have
//     time to finish before shard files are removed.
//   - Manifest check prevents deleting shards that are still active (e.g., if
//     compaction was rolled back or a concurrent writer re-added the shard).
//   - Idempotent: treats ErrNotExist/ErrBlobNotFound as success.
//   - Retry mechanism handles transient delete failures with backoff.
//
// Failure modes:
//
//   - Manifest download failure: entry is re-queued with retry delay.
//   - Delete failure: entry is re-queued with retry delay; first error is
//     returned but sweep continues processing remaining entries.
//   - Shard still referenced: entry is re-queued (may have been re-added by
//     concurrent operation).

package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// defaultShardGCGraceWindow is the minimum time to wait after a shard is
// replaced before it can be deleted. This allows in-flight readers to finish.
const defaultShardGCGraceWindow = 2 * time.Minute

// defaultShardGCRetryDelay is the delay before retrying a failed GC operation
// (manifest download failure, delete failure, or shard still referenced).
const defaultShardGCRetryDelay = 10 * time.Second

// delayedShardGCEntry represents a shard queued for delayed garbage collection.
type delayedShardGCEntry struct {
	KBID      string                // knowledge base the shard belongs to
	Shard     SnapshotShardMetadata // metadata of the shard to delete
	NotBefore time.Time             // earliest time the shard can be deleted
}

// shardObjectDeleter is an optional interface for blob stores that support
// direct object deletion.
type shardObjectDeleter interface {
	Delete(ctx context.Context, key string) error
}

// ShardGCSweepResult summarizes the outcome of one delayed GC sweep.
//
// Deleted is the count of shards successfully removed. Retried is the count of
// shards that failed and were re-queued. Pending is the total count of entries
// remaining in the queue after the sweep.
type ShardGCSweepResult struct {
	Deleted int
	Retried int
	Pending int
}

// enqueueReplacedShardsForGC adds replaced shards to the GC queue with a grace
// window before they can be deleted.
//
// If a shard is already in the queue, its NotBefore time is extended rather
// than creating a duplicate entry. This handles cases where the same shard is
// replaced multiple times before GC runs.
//
// Called by compaction after successfully publishing a new manifest.
func (l *KB) enqueueReplacedShardsForGC(kbID string, shards []SnapshotShardMetadata, now time.Time) {
	if kbID == "" || len(shards) == 0 {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	notBefore := now.Add(defaultShardGCGraceWindow)

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, shard := range shards {
		if shard.Key == "" {
			continue
		}
		replaced := false
		for i := range l.shardGC {
			entry := &l.shardGC[i]
			if entry.KBID == kbID && entry.Shard.Key == shard.Key {
				if notBefore.After(entry.NotBefore) {
					entry.NotBefore = notBefore
				}
				replaced = true
				break
			}
		}
		if replaced {
			continue
		}
		l.shardGC = append(l.shardGC, delayedShardGCEntry{
			KBID:      kbID,
			Shard:     shard,
			NotBefore: notBefore,
		})
	}
}

// deleteShardObject removes a shard file from blob storage.
//
// It first attempts to use the shardObjectDeleter interface if the blob store
// implements it, otherwise falls back to direct file removal for LocalBlobStore.
// Returns nil if the file is already deleted (idempotent).
func (l *KB) deleteShardObject(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	if deleter, ok := l.BlobStore.(shardObjectDeleter); ok {
		err := deleter.Delete(ctx, key)
		if err == nil || errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrBlobNotFound) {
			return nil
		}
		return err
	}
	local, ok := l.BlobStore.(*LocalBlobStore)
	if !ok {
		return nil
	}
	path := filepath.Join(local.Root, key)
	err := os.Remove(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// SweepDelayedShardGC processes the GC queue and deletes replaced shard files
// that have passed their grace window.
//
// For each queued entry:
//  1. Skip if still within grace window (NotBefore > now).
//  2. Download the current manifest to verify shard is no longer referenced.
//  3. If shard is still in manifest, re-queue with retry delay (may have been
//     re-added by concurrent operation).
//  4. Delete the shard file from blob storage.
//  5. On delete failure, re-queue with retry delay.
//
// The sweep is atomic with respect to the queue: it takes a snapshot at the
// start and replaces the queue with remaining entries at the end.
//
// Returns the first error encountered (but continues processing all entries).
func (l *KB) SweepDelayedShardGC(ctx context.Context, now time.Time) (ShardGCSweepResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	l.mu.Lock()
	queue := append([]delayedShardGCEntry(nil), l.shardGC...)
	l.mu.Unlock()

	if len(queue) == 0 {
		return ShardGCSweepResult{}, nil
	}

	activeKeysByKB := make(map[string]map[string]struct{})
	var firstErr error
	next := make([]delayedShardGCEntry, 0, len(queue))
	result := ShardGCSweepResult{}

	for _, entry := range queue {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if now.Before(entry.NotBefore) {
			slog.Default().InfoContext(ctx, "deferred shard GC pending grace window", "kb_id", entry.KBID, "reason", "grace_window", "shard_key", entry.Shard.Key, "not_before", entry.NotBefore)
			next = append(next, entry)
			continue
		}

		activeKeys, ok := activeKeysByKB[entry.KBID]
		if !ok {
			manifest, err := l.downloadShardManifest(ctx, entry.KBID)
			if err != nil {
				slog.Default().WarnContext(ctx, "deferred shard GC manifest download failed", "kb_id", entry.KBID, "reason", "manifest_download_failed", "shard_key", entry.Shard.Key, "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("download manifest for shard gc: %w", err)
				}
				entry.NotBefore = now.Add(defaultShardGCRetryDelay)
				next = append(next, entry)
				result.Retried++
				continue
			}
			activeKeys = make(map[string]struct{}, len(manifest.Shards))
			for _, shard := range manifest.Shards {
				if shard.Key != "" {
					activeKeys[shard.Key] = struct{}{}
				}
			}
			activeKeysByKB[entry.KBID] = activeKeys
		}

		if _, stillReferenced := activeKeys[entry.Shard.Key]; stillReferenced {
			slog.Default().InfoContext(ctx, "deferred shard GC skipped referenced shard", "kb_id", entry.KBID, "reason", "still_referenced", "shard_key", entry.Shard.Key)
			entry.NotBefore = now.Add(defaultShardGCRetryDelay)
			next = append(next, entry)
			result.Retried++
			continue
		}

		if err := l.deleteShardObject(ctx, entry.Shard.Key); err != nil {
			slog.Default().WarnContext(ctx, "deferred shard GC delete failed", "kb_id", entry.KBID, "reason", "delete_failed", "shard_key", entry.Shard.Key, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("delete replaced shard %s: %w", entry.Shard.Key, err)
			}
			entry.NotBefore = now.Add(defaultShardGCRetryDelay)
			next = append(next, entry)
			result.Retried++
			continue
		}

		result.Deleted++
		slog.Default().InfoContext(ctx, "deferred shard GC deleted shard", "kb_id", entry.KBID, "reason", "deleted", "shard_key", entry.Shard.Key)
	}

	result.Pending = len(next)
	l.mu.Lock()
	l.shardGC = next
	l.mu.Unlock()

	if result.Deleted > 0 || result.Retried > 0 || result.Pending > 0 {
		slog.Default().InfoContext(ctx, "completed deferred shard GC sweep", "reason", "gc_sweep", "deleted", result.Deleted, "retried", result.Retried, "pending", result.Pending)
	}

	return result, firstErr
}

// shardGCPendingCount returns the number of shards currently queued for GC.
// Used primarily for testing and metrics.
func (l *KB) shardGCPendingCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.shardGC)
}
