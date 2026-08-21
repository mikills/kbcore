package cmd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	kb "github.com/mikills/minnow/kb"
)

func newDeferredServer(t *testing.T, deps Dependencies) (*echo.Echo, *kb.IngestSessions) {
	t.Helper()
	deps.CacheDir = t.TempDir()
	deps.Logger = slog.New(slog.DiscardHandler)
	deps.AppMetrics = kb.NoopAppMetrics{}
	sessions := kb.NewIngestSessions(nil, deps.CacheDir)
	deps.Sessions = sessions
	deps.DeferredPublish = true
	if deps.AppendSessionCommit == nil {
		// A deployment that cannot commit must not hand out sessions, so the
		// write gate refuses without one wired.
		deps.AppendSessionCommit = func(
			_ context.Context, _ kb.SessionCommitPayload, idem, _ string,
		) (string, string, bool, error) {
			return "evt-commit", idem, true, nil
		}
	}
	e := echo.New()
	registerRagRoutes(e, deps, sessions)
	registerVectorRoutes(e, deps, sessions)
	return e, sessions
}

func post(t *testing.T, e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func sessionOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var out struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return out.SessionID
}

func TestDeferredIngest(t *testing.T) {
	upsert := func(recorded *[]kb.UpsertDocsOptions) func(context.Context, kb.DocumentUpsertPayload, string, string) (string, string, error) {
		return func(_ context.Context, p kb.DocumentUpsertPayload, idem, _ string) (string, string, error) {
			*recorded = append(*recorded, p.Options)
			return "evt-1", idem, nil
		}
	}
	batch := func(session string) string {
		return `{"kb_id":"kb","graph_enabled":false,"pre_chunked":true,"defer_publish":true,` +
			`"session_id":` + strconv.Quote(session) + `,` +
			`"documents":[{"id":"d1","text":"hello"}]}`
	}

	t.Run("the first batch is issued a handle the next one carries", func(t *testing.T) {
		var seen []kb.UpsertDocsOptions
		e, _ := newDeferredServer(t, Dependencies{AppendDocumentUpsert: upsert(&seen)})

		first := post(t, e, http.MethodPost, "/rag/ingest", batch(""))
		if first.Code != http.StatusAccepted {
			t.Fatalf("status %d body %s", first.Code, first.Body.String())
		}
		handle := sessionOf(t, first)
		if handle == "" {
			t.Fatal("a deferred batch was not issued a session handle")
		}
		second := post(t, e, http.MethodPost, "/rag/ingest", batch(handle))
		if second.Code != http.StatusAccepted {
			t.Fatalf("status %d body %s", second.Code, second.Body.String())
		}
		if got := sessionOf(t, second); got != handle {
			t.Fatalf("handle changed mid-session: %q then %q", handle, got)
		}
		if len(seen) != 2 || !seen[0].DeferPublish || !seen[1].DeferPublish {
			t.Fatalf("defer_publish did not reach the pipeline: %+v", seen)
		}
	})

	t.Run("a batch that cannot be queued leaves the session open", func(t *testing.T) {
		calls := 0
		deps := Dependencies{
			AppendDocumentUpsert: func(
				_ context.Context, _ kb.DocumentUpsertPayload, idem, _ string,
			) (string, string, error) {
				calls++
				if calls > 1 {
					return "", "", context.Canceled
				}
				return "evt-1", idem, nil
			},
		}
		e, sessions := newDeferredServer(t, deps)

		handle := sessionOf(t, post(t, e, http.MethodPost, "/rag/ingest", batch("")))
		if handle == "" {
			t.Fatal("a deferred batch was not issued a session handle")
		}
		// This batch renewed a session already holding earlier rows. Releasing
		// it would hand the knowledge base to another writer while those rows
		// are still unpublished.
		if rec := post(t, e, http.MethodPost, "/rag/ingest", batch(handle)); rec.Code < 500 {
			t.Fatalf("status %d, want a server error", rec.Code)
		}
		if _, err := sessions.Hold(context.Background(), "kb", ""); err == nil {
			t.Fatal("a failed batch dropped the session holding earlier rows")
		}
	})

	t.Run("another client cannot write into an open session", func(t *testing.T) {
		var seen []kb.UpsertDocsOptions
		e, _ := newDeferredServer(t, Dependencies{AppendDocumentUpsert: upsert(&seen)})

		if rec := post(t, e, http.MethodPost, "/rag/ingest", batch("")); rec.Code != http.StatusAccepted {
			t.Fatalf("status %d", rec.Code)
		}
		rec := post(t, e, http.MethodPost, "/rag/ingest", batch("stranger:token"))
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d, want 409", rec.Code)
		}
		if len(seen) != 1 {
			t.Fatal("a stranger's batch was written into the open session")
		}
	})

	t.Run("a batch that never enqueued does not hold the knowledge base", func(t *testing.T) {
		e, sessions := newDeferredServer(t, Dependencies{
			AppendDocumentUpsert: func(context.Context, kb.DocumentUpsertPayload, string, string) (string, string, error) {
				return "", "", context.Canceled
			},
		})
		if rec := post(t, e, http.MethodPost, "/rag/ingest", batch("")); rec.Code == http.StatusAccepted {
			t.Fatal("a failed append was reported as accepted")
		}
		// Otherwise the client is locked out of its own retry for the full TTL.
		if _, err := sessions.Hold(context.Background(), "kb", ""); err != nil {
			t.Fatalf("a failed batch kept the session: %v", err)
		}
	})

	t.Run("a deployment with deferred publishing off refuses to defer", func(t *testing.T) {
		var seen []kb.UpsertDocsOptions
		var deletes []kb.DeleteDocsOptions
		deps := Dependencies{
			AppendDocumentUpsert: upsert(&seen),
			DeleteDocuments: func(_ context.Context, _ string, _ []string, o kb.DeleteDocsOptions) error {
				deletes = append(deletes, o)
				return nil
			},
		}
		deps.CacheDir = t.TempDir()
		deps.Logger = slog.New(slog.DiscardHandler)
		deps.AppMetrics = kb.NoopAppMetrics{}
		deps.Sessions = kb.NewIngestSessions(nil, deps.CacheDir)
		e := echo.New()
		registerRagRoutes(e, deps, deps.Sessions)
		registerVectorRoutes(e, deps, deps.Sessions)

		// Withholding the capability from /healthz is not enough: a client that
		// skips the probe must not open a session the deployment cannot finish.
		if rec := post(t, e, http.MethodPost, "/rag/ingest", batch("")); rec.Code != http.StatusBadRequest {
			t.Fatalf("ingest status %d, want 400", rec.Code)
		}
		body := `{"kb_id":"kb","ids":["d1"],"defer_publish":true,"session_id":""}`
		if rec := post(t, e, http.MethodDelete, "/v1/vectors", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("delete status %d, want 400", rec.Code)
		}
		if len(seen) != 0 || len(deletes) != 0 {
			t.Fatal("a deferred request reached the pipeline anyway")
		}
	})

	t.Run("a deployment that cannot commit refuses to defer", func(t *testing.T) {
		var seen []kb.UpsertDocsOptions
		// A session opened here holds rows nothing on this deployment can
		// publish, until the reaper runs.
		deps := Dependencies{AppendDocumentUpsert: upsert(&seen)}
		deps.CacheDir = t.TempDir()
		deps.Logger = slog.New(slog.DiscardHandler)
		deps.AppMetrics = kb.NoopAppMetrics{}
		deps.DeferredPublish = true
		sessions := kb.NewIngestSessions(nil, deps.CacheDir)
		deps.Sessions = sessions
		e := echo.New()
		registerRagRoutes(e, deps, sessions)

		if rec := post(t, e, http.MethodPost, "/rag/ingest", batch("")); rec.Code != http.StatusBadRequest {
			t.Fatalf("ingest status %d, want 400", rec.Code)
		}
		if _, err := sessions.Hold(context.Background(), "kb", ""); err != nil {
			t.Fatalf("a refused batch took the session: %v", err)
		}
	})

	t.Run("a plain batch opens no session", func(t *testing.T) {
		var seen []kb.UpsertDocsOptions
		e, sessions := newDeferredServer(t, Dependencies{AppendDocumentUpsert: upsert(&seen)})
		body := `{"kb_id":"kb","graph_enabled":false,"pre_chunked":true,` +
			`"documents":[{"id":"d1","text":"hello"}]}`
		if rec := post(t, e, http.MethodPost, "/rag/ingest", body); rec.Code != http.StatusAccepted {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		if _, err := sessions.Hold(context.Background(), "kb", ""); err != nil {
			t.Fatalf("a plain ingest took a session: %v", err)
		}
		if len(seen) != 1 || seen[0].DeferPublish {
			t.Fatalf("a plain ingest deferred its publish: %+v", seen)
		}
	})
}

func TestDeferredVectorDelete(t *testing.T) {
	del := func(calls *[]kb.DeleteDocsOptions) func(context.Context, string, []string, kb.DeleteDocsOptions) error {
		return func(_ context.Context, _ string, _ []string, opts kb.DeleteDocsOptions) error {
			*calls = append(*calls, opts)
			return nil
		}
	}
	body := func(session string) string {
		return `{"kb_id":"kb","ids":["d1"],"defer_publish":true,"session_id":` + strconv.Quote(session) + `}`
	}

	t.Run("a deferred delete joins the session and echoes its handle", func(t *testing.T) {
		var calls []kb.DeleteDocsOptions
		e, _ := newDeferredServer(t, Dependencies{DeleteDocuments: del(&calls)})

		rec := post(t, e, http.MethodDelete, "/v1/vectors", body(""))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		handle := sessionOf(t, rec)
		if handle == "" {
			t.Fatal("a deferred delete was not issued a session handle")
		}
		if len(calls) != 1 || !calls[0].DeferPublish {
			t.Fatalf("defer_publish did not reach the delete: %+v", calls)
		}
	})

	t.Run("another client cannot delete inside an open session", func(t *testing.T) {
		var calls []kb.DeleteDocsOptions
		e, sessions := newDeferredServer(t, Dependencies{DeleteDocuments: del(&calls)})
		if _, err := sessions.Hold(context.Background(), "kb", ""); err != nil {
			t.Fatal(err)
		}
		rec := post(t, e, http.MethodDelete, "/v1/vectors", body("stranger:token"))
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d, want 409", rec.Code)
		}
		if len(calls) != 0 {
			t.Fatal("a stranger's delete ran inside the open session")
		}
	})
}

// An unrelated index in the same checkout has to be able to clean up its own
// leftovers, and a knowledge base holding an open session refuses that.
func TestUnpublishedWritesAreReportedAsAConflict(t *testing.T) {
	var seen []kb.DeleteDocsOptions
	deps := Dependencies{
		DeleteDocuments: func(_ context.Context, _ string, _ []string, o kb.DeleteDocsOptions) error {
			seen = append(seen, o)
			return kb.UnpublishedWritesError("kb")
		},
	}
	e, _ := newDeferredServer(t, deps)

	rec := post(t, e, http.MethodDelete, "/v1/vectors", `{"kb_id":"kb","ids":["a"]}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409", rec.Code)
	}
	if len(seen) != 1 {
		t.Fatalf("the delete did not reach the store: %+v", seen)
	}
}
