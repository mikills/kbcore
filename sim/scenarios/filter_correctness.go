package scenarios

import (
	"fmt"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
	"github.com/mikills/minnow/sim"
)

// FilterCorrectness ingests docs across two tenants, then verifies that
// filtered queries return only matching docs — across multiple shards,
// after cache eviction, and under blob faults.
//
// Invariant under test: a filtered top-k never returns a doc whose metadata
// does not satisfy the predicate.
func FilterCorrectness(h *sim.Harness) {
	const (
		kbID    = "filter-correctness"
		perTenant = 15
	)

	tenants := []string{"alpha", "beta"}
	allDocs := make([]kb.Document, 0, len(tenants)*perTenant)
	for ti, tenant := range tenants {
		for i := range perTenant {
			allDocs = append(allDocs, kb.Document{
				ID:   fmt.Sprintf("doc-%s-%03d", tenant, i),
				Text: fmt.Sprintf("%s document number %d sim content", tenant, i),
				Metadata: map[string]any{
					"tenant": tenant,
					"rank":   float64(ti*perTenant + i),
				},
			})
		}
	}

	if err := h.Ingest(kbID, allDocs); err != nil {
		h.Fatalf("ingest: %v", err)
	}

	queryVec := h.RandomVec(32)

	for _, tenant := range tenants {
		filter := &search.FilterExpr{
			Field: "tenant",
			Op:    search.FilterOpEq,
			Value: tenant,
		}
		results, err := h.SearchWithFilter(kbID, queryVec, len(allDocs), filter)
		if err != nil {
			h.Fatalf("filter query tenant=%s: %v", tenant, err)
		}
		for _, r := range results {
			got, _ := r.Metadata["tenant"].(string)
			if got != tenant {
				h.Errorf("filter tenant=%s: got doc %s with tenant=%q", tenant, r.ID, got)
			}
		}
		if len(results) != perTenant {
			h.Errorf("filter tenant=%s: expected %d results, got %d", tenant, perTenant, len(results))
		}
	}

	// Verify after cache eviction — forces a fresh shard download.
	if err := h.WipeCache(); err != nil {
		h.Fatalf("wipe cache: %v", err)
	}

	filter := &search.FilterExpr{
		Field: "tenant",
		Op:    search.FilterOpEq,
		Value: "alpha",
	}
	results, err := h.SearchWithFilter(kbID, queryVec, len(allDocs), filter)
	if err != nil {
		h.Fatalf("post-eviction filter query: %v", err)
	}
	for _, r := range results {
		got, _ := r.Metadata["tenant"].(string)
		if got != "alpha" {
			h.Errorf("post-eviction filter: got doc %s with tenant=%q", r.ID, got)
		}
	}

	h.RecordManifestVersion(kbID)
}
