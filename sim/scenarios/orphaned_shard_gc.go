package scenarios

import (
	"fmt"
	"time"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

const orphanGCKBID = "orphaned-shard-gc"

// OrphanedShardGC publishes repeatedly so earlier generations fall out of the
// manifest, and lets the publish path reconcile them. Replaced generations used
// to linger forever because only compaction ever queued a shard for deletion.
func OrphanedShardGC(h *sim.Harness) {
	const rounds = 6
	for i := range rounds {
		ingestOrphanRound(h, orphanGCKBID, i)
		h.Clock().Advance(kb.DefaultOrphanedShardGracePeriod + time.Minute)
	}

	ctx := h.Ctx()
	active := activeShards(h, orphanGCKBID)
	before := shardBlobKeys(h, orphanGCKBID)
	if len(before) <= len(active) {
		h.Fatalf("expected replaced generations to still exist: %d blobs, %d active (seed=%d)",
			len(before), len(active), h.Seed())
	}

	h.Clock().Advance(kb.DefaultShardGCGraceWindow + time.Minute)
	result, err := h.KB().SweepDelayedShardGC(ctx, h.Clock().Now())
	if err != nil {
		h.Fatalf("sweep orphaned shards (seed=%d): %v", h.Seed(), err)
	}
	if result.Deleted == 0 {
		h.Fatalf("publishing queued no orphaned shards, %d blobs for %d active (seed=%d)",
			len(before), len(active), h.Seed())
	}
	assertOnlyActiveRemain(h, orphanGCKBID, active, before)
	assertQueryable(h, orphanGCKBID, rounds-1)
}

// ScheduledShardGC reclaims without publishing again. A generation ages past
// the grace period long after its publish finished, so a knowledge base written
// in a burst and then only read never cleans up on its own.
func ScheduledShardGC(h *sim.Harness) {
	const (
		kbID   = "scheduled-shard-gc"
		rounds = 4
	)
	for i := range rounds {
		ingestOrphanRound(h, kbID, i)
	}

	ctx := h.Ctx()
	active := activeShards(h, kbID)
	before := shardBlobKeys(h, kbID)
	if len(before) <= len(active) {
		h.Fatalf("expected replaced generations to still exist: %d blobs, %d active (seed=%d)",
			len(before), len(active), h.Seed())
	}

	// Nothing is old enough yet, which is the point.
	if err := h.KB().ReconcileShardBlobsForAllKBs(ctx, h.Clock().Now()); err != nil {
		h.Fatalf("early reconcile (seed=%d): %v", h.Seed(), err)
	}
	h.Clock().Advance(kb.DefaultShardGCGraceWindow + time.Minute)
	early, err := h.KB().SweepDelayedShardGC(ctx, h.Clock().Now())
	if err != nil {
		h.Fatalf("early sweep (seed=%d): %v", h.Seed(), err)
	}
	if early.Deleted != 0 {
		h.Errorf("deleted %d shards inside the grace period (seed=%d)", early.Deleted, h.Seed())
	}

	h.Clock().Advance(kb.DefaultOrphanedShardGracePeriod + time.Minute)
	if err := h.KB().ReconcileShardBlobsForAllKBs(ctx, h.Clock().Now()); err != nil {
		h.Fatalf("scheduled reconcile (seed=%d): %v", h.Seed(), err)
	}
	h.Clock().Advance(kb.DefaultShardGCGraceWindow + time.Minute)
	result, err := h.KB().SweepDelayedShardGC(ctx, h.Clock().Now())
	if err != nil {
		h.Fatalf("scheduled sweep (seed=%d): %v", h.Seed(), err)
	}
	if result.Deleted == 0 {
		h.Fatalf("scheduled reconcile queued nothing, %d blobs for %d active (seed=%d)",
			len(before), len(active), h.Seed())
	}
	assertOnlyActiveRemain(h, kbID, active, before)
	assertQueryable(h, kbID, rounds-1)
}

func ingestOrphanRound(h *sim.Harness, kbID string, round int) {
	doc := kb.Document{
		ID:   fmt.Sprintf("%s-%05d", kbID, round),
		Text: fmt.Sprintf("round %d content for %s", round, kbID),
	}
	if err := h.Ingest(kbID, []kb.Document{doc}); err != nil {
		h.Fatalf("round %d ingest (seed=%d): %v", round, h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)
}

func activeShards(h *sim.Harness, kbID string) []kb.SnapshotShardMetadata {
	active, err := h.ManifestShards(h.Ctx(), kbID)
	if err != nil {
		h.Fatalf("read manifest shards (seed=%d): %v", h.Seed(), err)
	}
	if len(active) == 0 {
		h.Fatalf("manifest has no shards for %s (seed=%d)", kbID, h.Seed())
	}
	return active
}

func assertOnlyActiveRemain(h *sim.Harness, kbID string, active []kb.SnapshotShardMetadata, before []string) {
	after := shardBlobKeys(h, kbID)
	if len(after) >= len(before) {
		h.Errorf("%d shard blobs remain of %d (seed=%d)", len(after), len(before), h.Seed())
	}
	for _, shard := range active {
		if !containsKey(after, shard.Key) {
			h.Errorf("sweep deleted referenced shard %s (seed=%d)", shard.Key, h.Seed())
		}
	}
}

func assertQueryable(h *sim.Harness, kbID string, round int) {
	probe, err := h.Embed(h.Ctx(), fmt.Sprintf("round %d content for %s", round, kbID))
	if err != nil {
		h.Fatalf("embed probe (seed=%d): %v", h.Seed(), err)
	}
	matches, err := h.Search(kbID, probe, 50)
	if err != nil {
		h.Fatalf("query after sweep (seed=%d): %v", h.Seed(), err)
	}
	if len(matches) == 0 {
		h.Errorf("no results after orphan sweep (seed=%d)", h.Seed())
	}
}

func shardBlobKeys(h *sim.Harness, kbID string) []string {
	keys := make([]string, 0)
	for _, prefix := range []string{kbID + ".duckdb.shards/", kbID + ".duckdb.compacted/"} {
		found, err := h.BlobKeys(prefix)
		if err != nil {
			h.Fatalf("list %s (seed=%d): %v", prefix, h.Seed(), err)
		}
		keys = append(keys, found...)
	}
	return keys
}

func containsKey(keys []string, want string) bool {
	for _, key := range keys {
		if key == want {
			return true
		}
	}
	return false
}
