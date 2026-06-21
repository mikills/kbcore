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
}
