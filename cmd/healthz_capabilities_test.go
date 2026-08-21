package cmd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/labstack/echo/v4"
	kb "github.com/mikills/minnow/kb"
)

func capabilitiesOf(t *testing.T, deps Dependencies) []string {
	t.Helper()
	deps.Logger = slog.New(slog.DiscardHandler)
	deps.AppMetrics = kb.NoopAppMetrics{}
	e := echo.New()
	registerOpsRoutes(e, deps)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status %d", rec.Code)
	}
	var out struct {
		Capabilities []string `json:"capabilities"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Capabilities
}

func TestHealthzCapabilities(t *testing.T) {
	commit := func(context.Context, kb.SessionCommitPayload, string, string) (string, string, bool, error) {
		return "evt", "idem", true, nil
	}

	t.Run("deferred publish is advertised when commit is wired and one writer owns the data", func(t *testing.T) {
		got := capabilitiesOf(t, Dependencies{AppendSessionCommit: commit, DeferredPublish: true})
		// codeindex/client.go matches this literal across a module boundary, so
		// renaming it here silently stops every client deferring.
		if !slices.Contains(got, "ingest_sessions") {
			t.Fatalf("capabilities %v do not advertise ingest_sessions", got)
		}
	})

	t.Run("a deployment that may have several writers advertises nothing", func(t *testing.T) {
		got := capabilitiesOf(t, Dependencies{AppendSessionCommit: commit, DeferredPublish: false})
		if slices.Contains(got, "ingest_sessions") {
			t.Fatal("a deployment without a declared single writer offered deferred publishing")
		}
	})

	t.Run("a server that cannot commit advertises nothing", func(t *testing.T) {
		got := capabilitiesOf(t, Dependencies{DeferredPublish: true})
		if slices.Contains(got, "ingest_sessions") {
			t.Fatal("a server with no commit wired offered deferred publishing")
		}
	})
}

// mergeWithDefaultAppConfig copies field by field, so a new AppConfig field is
// silently dropped unless it is added there too.
func TestAppConfigMergeKeepsDeferredPublish(t *testing.T) {
	merged := mergeWithDefaultAppConfig(AppConfig{DeferredPublish: true})
	if !merged.DeferredPublish {
		t.Fatal("deferred publish was dropped merging the app config")
	}
}
