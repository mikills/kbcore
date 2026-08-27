package scenarios

import (
	"testing"

	"github.com/mikills/minnow/sim"
)

func TestNewDocumentWorkflowScenarios(t *testing.T) {
	invariants := []sim.Invariant{
		sim.ManifestMonotonic(),
		sim.NoDocLoss(20),
		sim.ShardsInManifestExist(),
	}

	t.Run("concurrent_many_writers", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(11), sim.WithInvariants(invariants...))
		ConcurrentManyWriters(h)
		h.AssertInvariants()
	})

	t.Run("async_document_pipeline", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(11), sim.WithInvariants(invariants...))
		AsyncDocumentPipeline(h)
		h.AssertInvariants()
	})

	t.Run("filter_correctness", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(42), sim.WithInvariants(invariants...))
		FilterCorrectness(h)
		h.AssertInvariants()
	})

	t.Run("vector_primitive", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(7), sim.WithInvariants(invariants...))
		VectorPrimitive(h)
		h.AssertInvariants()
	})

	// Every deferred scenario ends with the rows published, so no_doc_loss
	// covers the whole session and not just the batches that were uploaded.
	t.Run("deferred_session_commit", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(23), sim.WithInvariants(invariants...))
		DeferredSessionCommit(h)
		h.AssertInvariants()
	})

	t.Run("abandoned_session_reaped", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(29), sim.WithInvariants(invariants...))
		AbandonedSessionReaped(h)
		h.AssertInvariants()
	})

	t.Run("orphaned_shard_gc", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(37), sim.WithInvariants(invariants...))
		OrphanedShardGC(h)
		h.AssertInvariants()
	})

	t.Run("scheduled_shard_gc", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(41), sim.WithInvariants(invariants...))
		ScheduledShardGC(h)
		h.AssertInvariants()
	})

	t.Run("deferred_commit_under_blob_faults", func(t *testing.T) {
		h := sim.New(t, sim.WithSeed(31), sim.WithInvariants(invariants...))
		DeferredCommitUnderBlobFaults(h)
		h.AssertInvariants()
	})
}
