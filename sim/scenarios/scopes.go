package scenarios

import (
	"errors"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

func ScopeUploadFault(h *sim.Harness) {
	loader := h.KB()
	seedScopeDocs(h, "scope-a", "scope-b", "scope-c")
	created, err := loader.ReplaceScope(h.Context(), "scope-kb", "main", []string{"scope-a", "scope-b"}, "")
	if err != nil {
		h.Fatalf("seed scope: %v", err)
	}
	h.SetBlobFaults(sim.BlobFaults{UploadFailRate: 1})
	if _, err := loader.ReplaceScope(h.Context(), "scope-kb", "main", []string{"scope-c"}, created.Revision); !errors.Is(err, sim.ErrInjected) {
		h.Fatalf("replace under fault: %v", err)
	}
	h.SetBlobFaults(sim.BlobFaults{})
	scope, err := loader.GetScope(h.Context(), "scope-kb", "main")
	if err != nil || len(scope.DocumentIDs) != 2 || scope.DocumentIDs[0] != "scope-a" || scope.DocumentIDs[1] != "scope-b" {
		h.Errorf("failed replace changed committed membership: scope=%+v err=%v", scope, err)
	}
}

func ScopeReadFault(h *sim.Harness) {
	loader := h.KB()
	seedScopeDocs(h, "scope-a")
	if _, err := loader.ReplaceScope(h.Context(), "scope-kb", "main", []string{"scope-a"}, ""); err != nil {
		h.Fatalf("seed scope: %v", err)
	}
	h.SetBlobFaults(sim.BlobFaults{DownloadFailRate: 1})
	if _, err := loader.ListScopes(h.Context(), "scope-kb"); !errors.Is(err, sim.ErrInjected) {
		h.Fatalf("list under fault: %v", err)
	}
	h.SetBlobFaults(sim.BlobFaults{})
	scopes, err := loader.ListScopes(h.Context(), "scope-kb")
	if err != nil || len(scopes) != 1 || scopes[0].ScopeID != "main" {
		h.Errorf("read fault damaged scopes: scopes=%+v err=%v", scopes, err)
	}
}

func ScopeIsolation(h *sim.Harness) {
	loader := h.KB()
	seedScopeDocs(h, "scope-shared", "scope-main", "scope-feature")
	main, err := loader.ReplaceScope(h.Context(), "scope-kb", "main", []string{"scope-shared", "scope-main"}, "")
	if err != nil {
		h.Fatalf("write main scope: %v", err)
	}
	if _, err := loader.ReplaceScope(h.Context(), "scope-kb", "feature", []string{"scope-shared", "scope-feature"}, ""); err != nil {
		h.Fatalf("write feature scope: %v", err)
	}
	if _, err := loader.ReplaceScope(h.Context(), "scope-kb", "main", nil, main.Revision); err != nil {
		h.Fatalf("clear main scope: %v", err)
	}
	feature, err := loader.GetScope(h.Context(), "scope-kb", "feature")
	if err != nil || len(feature.DocumentIDs) != 2 {
		h.Errorf("main update changed feature scope: scope=%+v err=%v", feature, err)
	}
}

func seedScopeDocs(h *sim.Harness, ids ...string) {
	docs := make([]kb.Document, 0, len(ids))
	for _, id := range ids {
		docs = append(docs, kb.Document{ID: id, Text: "scope document " + id})
	}
	if err := h.Ingest("scope-kb", docs); err != nil {
		h.Fatalf("seed scope docs: %v", err)
	}
}
