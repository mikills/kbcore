package sim_test

import (
	"fmt"
	"testing"

	"github.com/mikills/minnow/sim"
	"github.com/mikills/minnow/sim/scenarios"
)

// TestSim exercises each scenario under a fixed set of seeds. Failures print
// the seed so a reviewer can rerun with -run to reproduce.
//
// To add a new scenario, drop a file under sim/scenarios/ that exports a
// func(h *sim.Harness) and add it to the map below.
func TestTieredStorageSim(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 7, 42, 100} {
		t.Run(fmt.Sprintf("recovery/seed_%d", seed), func(t *testing.T) {
			scenarios.TieredStorageRecovery(t, seed)
		})
	}
}

func TestSim(t *testing.T) {
	seeds := []int64{1, 2, 3, 7, 42, 100}

	cases := map[string]func(*sim.Harness){
		"warm_vs_cold":            scenarios.WarmVsCold,
		"flaky_blob_ingest":       scenarios.FlakyBlobIngest,
		"concurrent_writers":      scenarios.ConcurrentWriters,
		"concurrent_many_writers": scenarios.ConcurrentManyWriters,
		"async_document_pipeline": scenarios.AsyncDocumentPipeline,
		"cache_evicted_mid_query": scenarios.CacheEvictedMidQuery,
		"high_download_failure":   scenarios.HighDownloadFailure,
		"clock_jumps_backward":    scenarios.ClockJumpsBackward,
		"dim_mismatch_query":      scenarios.DimMismatchQuery,
		"repeated_small_ingests":  scenarios.RepeatedSmallIngests,
		"concurrent_ingest_query": scenarios.ConcurrentIngestAndQuery,
		"random_ops_fuzz":         scenarios.RandomOpsFuzz,
		"scope_upload_fault":      scenarios.ScopeUploadFault,
		"scope_read_fault":        scenarios.ScopeReadFault,
		"scope_isolation":         scenarios.ScopeIsolation,
	}

	invariants := []sim.Invariant{
		sim.ManifestMonotonic(),
		sim.NoDocLoss(50),
		sim.ShardsInManifestExist(),
	}

	for name, scenario := range cases {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed_%d", name, seed), func(t *testing.T) {
				h := sim.New(t, sim.WithSeed(seed), sim.WithInvariants(invariants...))
				scenario(h)
				h.AssertInvariants()
			})
		}
	}
}
