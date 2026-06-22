package scenarios

import (
	"fmt"
	"math"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

func VectorPrimitive(h *sim.Harness) {
	const (
		kbID = "vec-primitive"
		dim  = 32
		n    = 20
	)

	vecs := buildUnitVecs(n, dim)
	docs := make([]kb.Document, n)
	for i := range n {
		docs[i] = kb.Document{
			ID:        fmt.Sprintf("vec-%03d", i),
			Embedding: vecs[i],
			Metadata:  map[string]any{"idx": float64(i)},
		}
	}

	if err := h.Ingest(kbID, docs); err != nil {
		h.Fatalf("ingest pre-computed vectors: %v", err)
	}

	assertSelfTopResult(h, kbID, vecs)

	if err := h.WipeCache(); err != nil {
		h.Fatalf("wipe cache: %v", err)
	}

	results, err := h.Search(kbID, vecs[0], 1)
	if err != nil {
		h.Fatalf("post-eviction search: %v", err)
	}
	if len(results) == 0 || results[0].ID != "vec-000" {
		h.Errorf("post-eviction: expected vec-000 as top result")
	}

	h.RecordManifestVersion(kbID)
}

func buildUnitVecs(n, dim int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range n {
		v := make([]float32, dim)
		v[i%dim] = 1.0
		if i >= dim {
			v[(i+1)%dim] = 0.5
			v = normalizeVec32(v)
		}
		vecs[i] = v
	}
	return vecs
}

func assertSelfTopResult(h *sim.Harness, kbID string, vecs [][]float32) {
	for i, probe := range vecs {
		results, err := h.Search(kbID, probe, 3)
		if err != nil {
			h.Fatalf("search vec-%03d: %v", i, err)
		}
		if len(results) == 0 {
			h.Errorf("vec-%03d: got no results", i)
			continue
		}
		if results[0].ID != fmt.Sprintf("vec-%03d", i) {
			h.Errorf("vec-%03d: expected self as top result, got %s (distance=%.4f)", i, results[0].ID, results[0].Distance)
		}
	}
}

func normalizeVec32(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := float32(math.Sqrt(sum))
	if norm == 0 {
		return v
	}
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x / norm
	}
	return out
}
