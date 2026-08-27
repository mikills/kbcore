package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// DefaultShardGCGraceWindow is the minimum time to wait after a shard is
// replaced before it can be deleted. This allows in-flight readers to finish.
const DefaultShardGCGraceWindow = 2 * time.Minute

// DefaultShardGCRetryDelay is the delay before retrying a failed GC operation
// (manifest download failure, delete failure, or shard still referenced).
const DefaultShardGCRetryDelay = 10 * time.Second

// delayedShardGCEntry represents a shard queued for delayed garbage collection.
type delayedShardGCEntry struct {
	KBID      string                // knowledge base the shard belongs to
	Shard     SnapshotShardMetadata // metadata of the shard to delete
	NotBefore time.Time             // earliest time the shard can be deleted
	// Inferred from storage rather than seen being replaced, so deletion also
	// waits on the object's own age.
	Reconciled bool
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
		now = l.Clock.Now()
	}

	notBefore := now.Add(DefaultShardGCGraceWindow)

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, shard := range shards {
		l.enqueueShardGCLocked(kbID, shard, notBefore)
	}
}

func (l *KB) enqueueShardGCLocked(kbID string, shard SnapshotShardMetadata, notBefore time.Time) {
	if shard.Key == "" {
		return
	}
	if l.extendShardGCLocked(kbID, shard.Key, notBefore) {
		return
	}
	l.shardGC = append(l.shardGC, delayedShardGCEntry{KBID: kbID, Shard: shard, NotBefore: notBefore})
}

func (l *KB) extendShardGCLocked(kbID string, shardKey string, notBefore time.Time) bool {
	for i := range l.shardGC {
		entry := &l.shardGC[i]
		if entry.KBID == kbID && entry.Shard.Key == shardKey {
			if notBefore.After(entry.NotBefore) {
				entry.NotBefore = notBefore
			}
			return true
		}
	}
	return false
}

// EnqueueReplacedShardsForGC exposes delayed shard GC queueing for backend-owned
// compaction implementations.
func (l *KB) EnqueueReplacedShardsForGC(kbID string, shards []SnapshotShardMetadata, now time.Time) {
	l.enqueueReplacedShardsForGC(kbID, shards, now)
}

// deleteShardObject removes a shard file from blob storage.
//
// Returns nil if the file is already deleted (idempotent).
func (l *KB) deleteShardObject(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}

	err := l.BlobStore.Delete(ctx, key)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if errors.Is(err, ErrBlobNotFound) {
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
		now = l.Clock.Now()
	}

	l.mu.Lock()
	queue := append([]delayedShardGCEntry(nil), l.shardGC...)
	l.mu.Unlock()

	if len(queue) == 0 {
		return ShardGCSweepResult{}, nil
	}

	state := shardGCSweepState{
		activeKeysByKB: make(map[string]map[string]struct{}),
		freshKeysByKB:  make(map[string]map[string]struct{}),
		next:           make([]delayedShardGCEntry, 0, len(queue)),
	}
	for _, entry := range queue {
		if err := ctx.Err(); err != nil {
			return state.result, err
		}
		l.sweepShardGCEntry(ctx, now, entry, &state)
	}

	next := state.next
	result := state.result
	firstErr := state.firstErr

	result.Pending = len(next)
	l.mu.Lock()
	l.shardGC = next
	l.mu.Unlock()

	if result.Deleted > 0 || result.Retried > 0 || result.Pending > 0 {
		slog.Default().
			InfoContext(ctx, "completed deferred shard GC sweep", logKeyReason, "gc_sweep", "deleted", result.Deleted, "retried", result.Retried, "pending", result.Pending)
	}

	return result, firstErr
}

type shardGCSweepState struct {
	activeKeysByKB map[string]map[string]struct{}
	freshKeysByKB  map[string]map[string]struct{}
	next           []delayedShardGCEntry
	result         ShardGCSweepResult
	firstErr       error
}

func (l *KB) sweepShardGCEntry(
	ctx context.Context,
	now time.Time,
	entry delayedShardGCEntry,
	state *shardGCSweepState,
) {
	if now.Before(entry.NotBefore) {
		slog.Default().
			InfoContext(ctx, "deferred shard GC pending grace window", logKeyKBID, entry.KBID, logKeyReason, "grace_window", "shard_key", entry.Shard.Key, "not_before", entry.NotBefore)
		state.next = append(state.next, entry)
		return
	}
	activeKeys, err := l.activeShardKeysForGC(ctx, entry, state.activeKeysByKB)
	if err != nil {
		state.retry(entry, now, err)
		return
	}
	if _, stillReferenced := activeKeys[entry.Shard.Key]; stillReferenced {
		slog.Default().
			InfoContext(ctx, "deferred shard GC skipped referenced shard", logKeyKBID, entry.KBID, logKeyReason, "still_referenced", "shard_key", entry.Shard.Key)
		state.retry(entry, now, nil)
		return
	}
	reason, err := l.confirmShardDeletable(ctx, entry, now, state.freshKeysByKB)
	if err != nil {
		state.retry(entry, now, err)
		return
	}
	if reason != "" {
		slog.Default().
			InfoContext(ctx, "deferred shard GC skipped shard", logKeyKBID, entry.KBID, logKeyReason, reason, "shard_key", entry.Shard.Key)
		state.retry(entry, now, nil)
		return
	}
	if err := l.deleteShardObject(ctx, entry.Shard.Key); err != nil {
		slog.Default().
			WarnContext(ctx, "deferred shard GC delete failed", logKeyKBID, entry.KBID, logKeyReason, "delete_failed", "shard_key", entry.Shard.Key, logKeyError, err)
		state.retry(entry, now, fmt.Errorf("delete replaced shard %s: %w", entry.Shard.Key, err))
		return
	}
	state.result.Deleted++
	slog.Default().
		InfoContext(ctx, "deferred shard GC deleted shard", logKeyKBID, entry.KBID, logKeyReason, "deleted", "shard_key", entry.Shard.Key)
}

// confirmShardDeletable reports why an entry must not be deleted, or "" when it
// may be. Content-addressed keys make a republish byte-identical, so only a
// fresh manifest read and the object's write time can tell them apart.
func (l *KB) confirmShardDeletable(
	ctx context.Context,
	entry delayedShardGCEntry,
	now time.Time,
	cache map[string]map[string]struct{},
) (string, error) {
	freshKeys, ok := cache[entry.KBID]
	if !ok {
		doc, err := l.ManifestStore.Get(ctx, entry.KBID)
		if err != nil && !errors.Is(err, ErrManifestNotFound) {
			return "", fmt.Errorf("confirm manifest for shard gc: %w", err)
		}
		freshKeys = map[string]struct{}{}
		if err == nil && doc != nil {
			freshKeys = activeShardKeys(doc.Manifest.Shards)
		}
		cache[entry.KBID] = freshKeys
	}
	if _, referenced := freshKeys[entry.Shard.Key]; referenced {
		return "republished", nil
	}
	if !entry.Reconciled {
		return "", nil
	}
	info, err := l.BlobStore.Head(ctx, entry.Shard.Key)
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("confirm shard object for gc: %w", err)
	}
	if info == nil || info.UpdatedAt.IsZero() {
		return "unknown_write_time", nil
	}
	if !info.UpdatedAt.Before(now.Add(-DefaultOrphanedShardGracePeriod)) {
		return "restaged", nil
	}
	return "", nil
}

func (s *shardGCSweepState) retry(entry delayedShardGCEntry, now time.Time, err error) {
	if err != nil && s.firstErr == nil {
		s.firstErr = err
	}
	entry.NotBefore = now.Add(DefaultShardGCRetryDelay)
	s.next = append(s.next, entry)
	s.result.Retried++
}

func (l *KB) activeShardKeysForGC(
	ctx context.Context,
	entry delayedShardGCEntry,
	cache map[string]map[string]struct{},
) (map[string]struct{}, error) {
	if activeKeys, ok := cache[entry.KBID]; ok {
		return activeKeys, nil
	}
	doc, err := l.ManifestStore.Get(ctx, entry.KBID)
	if errors.Is(err, ErrManifestNotFound) {
		cache[entry.KBID] = map[string]struct{}{}
		return cache[entry.KBID], nil
	}
	if err != nil {
		slog.Default().
			WarnContext(ctx, "deferred shard GC manifest download failed", logKeyKBID, entry.KBID, logKeyReason, "manifest_download_failed", "shard_key", entry.Shard.Key, logKeyError, err)
		return nil, fmt.Errorf("download manifest for shard gc: %w", err)
	}
	activeKeys := activeShardKeys(doc.Manifest.Shards)
	cache[entry.KBID] = activeKeys
	return activeKeys, nil
}

func activeShardKeys(shards []SnapshotShardMetadata) map[string]struct{} {
	keys := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		if shard.Key != "" {
			keys[shard.Key] = struct{}{}
		}
	}
	return keys
}

// shardGCPendingCount returns the number of shards currently queued for GC.
// Used primarily for testing and metrics.
func (l *KB) shardGCPendingCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.shardGC)
}

// Shards upload before the manifest naming them, so this must exceed the
// slowest publish.
const DefaultOrphanedShardGracePeriod = time.Hour

// Listing storage is the expensive part, so an orphan can wait one extra
// interval to be collected.
const shardReconcileInterval = DefaultOrphanedShardGracePeriod

// Pruning holds the KB-wide lock, so it waits for the map to be worth it.
const shardReconcilePruneAt = 1024

// Whole snapshots, and the parts compaction merges them into.
var shardNamespaces = []string{".duckdb.shards/", ".duckdb.compacted/"}

func shardBlobPrefixes(kbID string) []string {
	prefixes := make([]string, 0, len(shardNamespaces))
	for _, namespace := range shardNamespaces {
		prefixes = append(prefixes, kbID+namespace)
	}
	return prefixes
}

func ShardBlobPrefix(kbID string) string { return kbID + shardNamespaces[0] }

var (
	snapshotShardSuffix = regexp.MustCompile(`^[0-9a-f]{16}/shard-\d{5,}\.duckdb$`)
	compactedPartSuffix = regexp.MustCompile(`^compact-\d+/part-\d{5,}$`)
)

// ownedShardKey guards against a kbID that itself contains a shard namespace,
// which would let one knowledge base delete another's blobs as its own.
func ownedShardKey(key, prefix string) bool {
	rest, found := strings.CutPrefix(key, prefix)
	if !found {
		return false
	}
	switch {
	case strings.HasSuffix(prefix, shardNamespaces[0]):
		return snapshotShardSuffix.MatchString(rest)
	case strings.HasSuffix(prefix, shardNamespaces[1]):
		return compactedPartSuffix.MatchString(rest)
	default:
		return false
	}
}

func orphanedShardBlobs(
	objects []BlobObjectInfo,
	active []SnapshotShardMetadata,
	cutoff time.Time,
	prefix string,
) []SnapshotShardMetadata {
	activeKeys := activeShardKeys(active)
	orphaned := make([]SnapshotShardMetadata, 0, len(objects))
	for _, object := range objects {
		if !ownedShardKey(object.Key, prefix) {
			continue
		}
		if _, referenced := activeKeys[object.Key]; referenced {
			continue
		}
		if object.UpdatedAt.IsZero() || !object.UpdatedAt.Before(cutoff) {
			continue
		}
		orphaned = append(orphaned, SnapshotShardMetadata{
			Key:       object.Key,
			Version:   object.Version,
			SizeBytes: object.Size,
		})
	}
	return orphaned
}

// EnqueueOrphanedShardBlobs queues whatever storage holds that the shards a
// publish just made active do not account for.
func (l *KB) EnqueueOrphanedShardBlobs(
	ctx context.Context,
	kbID string,
	active []SnapshotShardMetadata,
	now time.Time,
) error {
	if kbID == "" || l.BlobStore == nil {
		return nil
	}
	if now.IsZero() {
		now = l.Clock.Now()
	}
	previous, claimed := l.claimShardReconcile(kbID, now)
	if !claimed {
		return nil
	}
	if err := l.reconcileShardBlobs(ctx, kbID, active, now); err != nil {
		l.releaseShardReconcile(kbID, previous)
		return err
	}
	return nil
}

func (l *KB) reconcileShardBlobs(
	ctx context.Context,
	kbID string,
	active []SnapshotShardMetadata,
	now time.Time,
) error {
	cutoff := now.Add(-DefaultOrphanedShardGracePeriod)
	var errs []error
	for _, prefix := range shardBlobPrefixes(kbID) {
		objects, err := l.BlobStore.List(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("list %s for shard gc: %w", prefix, err))
			continue
		}
		orphaned := orphanedShardBlobs(objects, active, cutoff, prefix)
		if len(orphaned) > 0 {
			l.enqueueUnqueuedShardsForGC(kbID, orphaned, now.Add(DefaultShardGCGraceWindow))
		}
	}
	if len(errs) > 0 {
		l.recordShardReconcileFailure(kbID)
	}
	return errors.Join(errs...)
}

const shardManifestSuffix = ".duckdb.manifest.json"

// Attributes a failure that took out the whole scan, not one knowledge base.
const scanMetricsKey = "_scan"

// ReconcileShardBlobsForAllKBs re-derives orphaned shards for every knowledge
// base in storage, which publishing alone never manages: a generation ages past
// the grace period long after the publish that replaced it finished.
func (l *KB) ReconcileShardBlobsForAllKBs(ctx context.Context, now time.Time) error {
	if l.BlobStore == nil {
		return nil
	}
	if now.IsZero() {
		now = l.Clock.Now()
	}
	if !l.claimShardScan(now) {
		return nil
	}
	objects, err := l.BlobStore.List(ctx, "")
	if err != nil {
		l.recordShardReconcileFailure(scanMetricsKey)
		l.releaseShardScan()
		return fmt.Errorf("list knowledge bases for shard gc: %w", err)
	}
	kbIDs := rotateFrom(shardOwnersFromKeys(objects), l.shardScanCursor())
	var errs []error
	for _, kbID := range kbIDs {
		if err := ctx.Err(); err != nil {
			l.setShardScanCursor(kbID)
			errs = append(errs, err)
			break
		}
		if err := l.reconcileOneKB(ctx, kbID, now); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		l.releaseShardScan()
	} else {
		l.setShardScanCursor("")
	}
	return errors.Join(errs...)
}

// reconcileOneKB skips the per-KB throttle. The scan is already throttled, and
// checking twice would let one recent publish waste the whole listing.
func (l *KB) reconcileOneKB(ctx context.Context, kbID string, now time.Time) error {
	active, err := l.activeShardsForReconcile(ctx, kbID)
	if err != nil {
		l.recordShardReconcileFailure(kbID)
		return err
	}
	return l.reconcileShardBlobs(ctx, kbID, active, now)
}

// A deleted KB loses its manifest before its blobs, so no manifest means no
// live shards rather than nothing to do.
func (l *KB) activeShardsForReconcile(ctx context.Context, kbID string) ([]SnapshotShardMetadata, error) {
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if errors.Is(err, ErrManifestNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read manifest %s for shard gc: %w", kbID, err)
	}
	if doc == nil {
		return nil, nil
	}
	return doc.Manifest.Shards, nil
}

// Sorted so the resume cursor stays stable across scans.
func shardOwnersFromKeys(objects []BlobObjectInfo) []string {
	owners := make(map[string]struct{})
	for _, object := range objects {
		if kbID, ok := KBIDFromManifestKey(object.Key); ok {
			owners[kbID] = struct{}{}
			continue
		}
		if kbID, ok := kbIDFromShardKey(object.Key); ok {
			owners[kbID] = struct{}{}
		}
	}
	kbIDs := make([]string, 0, len(owners))
	for kbID := range owners {
		kbIDs = append(kbIDs, kbID)
	}
	sort.Strings(kbIDs)
	return kbIDs
}

func rotateFrom(kbIDs []string, cursor string) []string {
	if cursor == "" {
		return kbIDs
	}
	at := sort.SearchStrings(kbIDs, cursor)
	if at >= len(kbIDs) {
		return kbIDs
	}
	return append(append([]string(nil), kbIDs[at:]...), kbIDs[:at]...)
}

// KBIDFromManifestKey extracts the knowledge base a manifest key belongs to.
func KBIDFromManifestKey(key string) (string, bool) {
	kbID, found := strings.CutSuffix(key, shardManifestSuffix)
	if !found || kbID == "" || strings.Contains(kbID, "/") {
		return "", false
	}
	return kbID, true
}

func kbIDFromShardKey(key string) (string, bool) {
	for _, namespace := range shardNamespaces {
		kbID, _, found := strings.Cut(key, namespace)
		if !found || kbID == "" || strings.Contains(kbID, "/") {
			continue
		}
		if ownedShardKey(key, kbID+namespace) {
			return kbID, true
		}
	}
	return "", false
}

func (l *KB) claimShardScan(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.shardScannedAt.IsZero() && now.Sub(l.shardScannedAt) < shardReconcileInterval {
		return false
	}
	l.shardScannedAt = now
	return true
}

func (l *KB) shardScanCursor() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.shardScanAfter
}

func (l *KB) setShardScanCursor(kbID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.shardScanAfter = kbID
}

func (l *KB) releaseShardScan() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.shardScannedAt = time.Time{}
}

// The previous timestamp comes back so a failed attempt can return the slot
// instead of burning the interval.
func (l *KB) claimShardReconcile(kbID string, now time.Time) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	previous, seen := l.shardReconciled[kbID]
	if seen && now.Sub(previous) < shardReconcileInterval {
		return previous, false
	}
	if l.shardReconciled == nil {
		l.shardReconciled = make(map[string]time.Time)
	}
	if len(l.shardReconciled) >= shardReconcilePruneAt {
		for id, last := range l.shardReconciled {
			if now.Sub(last) > 2*shardReconcileInterval {
				delete(l.shardReconciled, id)
			}
		}
	}
	l.shardReconciled[kbID] = now
	return previous, true
}

func (l *KB) releaseShardReconcile(kbID string, previous time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if previous.IsZero() {
		delete(l.shardReconciled, kbID)
		return
	}
	l.shardReconciled[kbID] = previous
}

// Unlike enqueueReplacedShardsForGC this never pushes an existing deadline
// back, which on a busy KB would defer every entry forever.
func (l *KB) enqueueUnqueuedShardsForGC(
	kbID string,
	shards []SnapshotShardMetadata,
	notBefore time.Time,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	queued := make(map[string]struct{}, len(l.shardGC))
	for i := range l.shardGC {
		if l.shardGC[i].KBID == kbID {
			queued[l.shardGC[i].Shard.Key] = struct{}{}
		}
	}
	for _, shard := range shards {
		if _, pending := queued[shard.Key]; pending {
			continue
		}
		queued[shard.Key] = struct{}{}
		l.shardGC = append(l.shardGC, delayedShardGCEntry{
			KBID:       kbID,
			Shard:      shard,
			NotBefore:  notBefore,
			Reconciled: true,
		})
	}
}
