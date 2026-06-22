package scenarios

import (
	"fmt"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
	"github.com/mikills/minnow/sim"
)

func FilterCorrectness(h *sim.Harness) {
	const (
		kbID      = "filter-correctness"
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
		assertTenantFilterResults(h, kbID, queryVec, tenant, perTenant)
	}

	if err := h.WipeCache(); err != nil {
		h.Fatalf("wipe cache: %v", err)
	}
	assertTenantFilterResults(h, kbID, queryVec, "alpha", perTenant)

	h.RecordManifestVersion(kbID)
}

func assertTenantFilterResults(h *sim.Harness, kbID string, queryVec []float32, tenant string, expected int) {
	filter := &search.FilterExpr{Field: "tenant", Op: search.FilterOpEq, Value: tenant}
	results, err := h.SearchWithFilter(kbID, queryVec, expected*2, filter)
	if err != nil {
		h.Fatalf("filter query tenant=%s: %v", tenant, err)
	}
	for _, r := range results {
		got, _ := r.Metadata["tenant"].(string)
		if got != tenant {
			h.Errorf("filter tenant=%s: got doc %s with tenant=%q", tenant, r.ID, got)
		}
	}
	if len(results) != expected {
		h.Errorf("filter tenant=%s: expected %d results, got %d", tenant, expected, len(results))
	}
}
