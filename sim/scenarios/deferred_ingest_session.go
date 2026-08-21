package scenarios

import (
	"errors"
	"fmt"

	"github.com/mikills/minnow/sim"
)

// DeferredSessionCommit asserts the manifest does not move while a session is
// open and moves once when it commits.
func DeferredSessionCommit(h *sim.Harness) {
	const kbID = "deferred-session"
	seed := h.GenerateDocs(kbID, 5)
	if err := h.Ingest(kbID, seed); err != nil {
		h.Fatalf("seed ingest (seed=%d): %v", h.Seed(), err)
	}
	before := h.ManifestVersion(kbID)

	for batch := range 4 {
		docs := h.GenerateDocs(fmt.Sprintf("%s-b%d", kbID, batch), 5)
		for i := range docs {
			docs[i].ID = fmt.Sprintf("%s-%d-%d", kbID, batch, i)
		}
		if err := h.DeferIngest(kbID, docs); err != nil {
			h.Fatalf("deferred batch %d (seed=%d): %v", batch, h.Seed(), err)
		}
		if got := h.ManifestVersion(kbID); got != before {
			h.Fatalf("batch %d published on its own: manifest %q became %q (seed=%d)",
				batch, before, got, h.Seed())
		}
	}
	if !h.HasPendingSession(kbID) {
		h.Fatalf("an open session left nothing marked (seed=%d)", h.Seed())
	}

	if err := h.Commit(kbID); err != nil {
		h.Fatalf("commit (seed=%d): %v", h.Seed(), err)
	}
	if h.ManifestVersion(kbID) == before {
		h.Fatalf("commit did not publish: manifest still %q (seed=%d)", before, h.Seed())
	}
	if h.HasPendingSession(kbID) {
		h.Fatalf("commit left the session open (seed=%d)", h.Seed())
	}
}

// AbandonedSessionReaped models a client that dies mid-run. Its marker blocks
// eviction, compaction and every later write, and only the reaper clears it.
func AbandonedSessionReaped(h *sim.Harness) {
	const kbID = "abandoned-session"
	seed := h.GenerateDocs(kbID, 5)
	if err := h.Ingest(kbID, seed); err != nil {
		h.Fatalf("seed ingest (seed=%d): %v", h.Seed(), err)
	}
	before := h.ManifestVersion(kbID)

	orphans := h.GenerateDocs(kbID+"-orphan", 8)
	for i := range orphans {
		orphans[i].ID = fmt.Sprintf("%s-orphan-%d", kbID, i)
	}
	if err := h.DeferIngest(kbID, orphans); err != nil {
		h.Fatalf("deferred batch (seed=%d): %v", h.Seed(), err)
	}

	// A session a client still holds belongs to a run that may not be finished.
	handle, err := h.HoldSession(kbID)
	if err != nil {
		h.Fatalf("hold session (seed=%d): %v", h.Seed(), err)
	}
	reaped, err := h.ReapSessions()
	if err != nil {
		h.Fatalf("reap with a live session (seed=%d): %v", h.Seed(), err)
	}
	if reaped != 0 {
		h.Fatalf("the reaper published %d sessions a client still holds (seed=%d)", reaped, h.Seed())
	}
	if h.ManifestVersion(kbID) != before {
		h.Fatalf("a live session was published anyway (seed=%d)", h.Seed())
	}

	if err := h.KB().IngestSessionsFor().Release(h.Ctx(), kbID, handle); err != nil {
		h.Fatalf("release session (seed=%d): %v", h.Seed(), err)
	}
	reaped, err = h.ReapSessions()
	if err != nil {
		h.Fatalf("reap (seed=%d): %v", h.Seed(), err)
	}
	if reaped != 1 {
		h.Fatalf("the reaper recovered %d sessions, want 1 (seed=%d)", reaped, h.Seed())
	}
	if h.HasPendingSession(kbID) {
		h.Fatalf("the reaper left the session open (seed=%d)", h.Seed())
	}
	if h.ManifestVersion(kbID) == before {
		h.Fatalf("the reaper cleared the marker without publishing (seed=%d)", h.Seed())
	}
}

// DeferredCommitUnderBlobFaults asserts a failed commit is retryable. Clearing
// the marker first would leave the rows unpublished with nothing to say so.
func DeferredCommitUnderBlobFaults(h *sim.Harness) {
	const kbID = "deferred-commit-faults"
	seed := h.GenerateDocs(kbID, 5)
	if err := h.Ingest(kbID, seed); err != nil {
		h.Fatalf("seed ingest (seed=%d): %v", h.Seed(), err)
	}

	docs := h.GenerateDocs(kbID+"-deferred", 10)
	for i := range docs {
		docs[i].ID = fmt.Sprintf("%s-deferred-%d", kbID, i)
	}
	if err := h.DeferIngest(kbID, docs); err != nil {
		h.Fatalf("deferred batch (seed=%d): %v", h.Seed(), err)
	}
	before := h.ManifestVersion(kbID)

	h.SetBlobFaults(sim.BlobFaults{UploadFailRate: 1.0})
	err := h.Commit(kbID)
	h.SetBlobFaults(sim.BlobFaults{})
	if err == nil {
		h.Fatalf("a commit reported success while every upload failed (seed=%d)", h.Seed())
	}
	if !errors.Is(err, sim.ErrInjected) {
		h.Fatalf("unexpected non-injected commit error (seed=%d): %v", h.Seed(), err)
	}
	// Without the marker the retry below would find nothing to publish and the
	// rows would be lost with the run reported as finished.
	if !h.HasPendingSession(kbID) {
		h.Fatalf("a failed commit discarded the record of its unpublished rows (seed=%d)", h.Seed())
	}

	if err := h.Commit(kbID); err != nil {
		h.Fatalf("retried commit (seed=%d): %v", h.Seed(), err)
	}
	if h.HasPendingSession(kbID) {
		h.Fatalf("the retried commit left the session open (seed=%d)", h.Seed())
	}
	// no_doc_loss reads the local shard, so only the manifest proves a publish.
	if h.ManifestVersion(kbID) == before {
		h.Fatalf("the retried commit cleared the marker without publishing (seed=%d)", h.Seed())
	}
}
