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

type commitRecorder struct {
	calls    []kb.SessionCommitPayload
	eventID  string
	failWith error
	dedupe   bool
}

func (r *commitRecorder) append(
	_ context.Context, payload kb.SessionCommitPayload, idem, _ string,
) (string, string, bool, error) {
	if r.failWith != nil {
		return "", "", false, r.failWith
	}
	r.calls = append(r.calls, payload)
	if r.eventID == "" {
		r.eventID = "evt-commit"
	}
	created := !r.dedupe
	return r.eventID, idem, created, nil
}

func newCommitServer(t *testing.T, deps Dependencies) (*echo.Echo, *kb.IngestSessions, string) {
	t.Helper()
	dir := t.TempDir()
	deps.CacheDir = dir
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.AppMetrics == nil {
		deps.AppMetrics = kb.NoopAppMetrics{}
	}
	sessions := kb.NewIngestSessions(nil, dir)
	deps.Sessions = sessions
	deps.DeferredPublish = true
	e := echo.New()
	registerRagRoutes(e, deps, sessions)
	return e, sessions, dir
}

func postCommit(t *testing.T, e *echo.Echo, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/rag/commit", strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestHandleRagCommit(t *testing.T) {
	t.Run("a server without commit wired refuses to open a session", func(t *testing.T) {
		e, sessions, _ := newCommitServer(t, Dependencies{})
		if rec := postCommit(t, e, `{"kb_id":"kb","session_id":"i:tok"}`); rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", rec.Code)
		}
		// A refusal must not be reported by holding the knowledge base.
		if _, err := sessions.Hold(context.Background(), "kb", ""); err != nil {
			t.Fatalf("a refused commit took the session: %v", err)
		}
	})

	t.Run("the holder queues the publish and gets an operation to follow", func(t *testing.T) {
		recorder := &commitRecorder{}
		e, sessions, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		handle, err := sessions.Hold(context.Background(), "kb", "")
		if err != nil {
			t.Fatal(err)
		}

		rec := postCommit(t, e, `{"kb_id":"kb","session_id":`+strconv.Quote(handle)+`}`)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		var out struct {
			EventID   string `json:"event_id"`
			SessionID string `json:"session_id"`
			StatusURL string `json:"status_url"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if out.EventID == "" || out.StatusURL != "/rag/operations/"+out.EventID {
			t.Fatalf("no operation to follow: %+v", out)
		}
		if out.SessionID != handle {
			t.Fatalf("handle changed to %q, want %q", out.SessionID, handle)
		}
		if len(recorder.calls) != 1 || recorder.calls[0].KBID != "kb" {
			t.Fatalf("commit was queued as %+v", recorder.calls)
		}
		// The session outlives the request: the worker releases it once the
		// rows are durable, so a late batch cannot land behind the publish.
		if _, err := sessions.Hold(context.Background(), "kb", ""); err == nil {
			t.Fatal("the session was freed before the publish ran")
		}
	})

	t.Run("another client's session is refused without queueing anything", func(t *testing.T) {
		recorder := &commitRecorder{}
		e, sessions, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		if _, err := sessions.Hold(context.Background(), "kb", ""); err != nil {
			t.Fatal(err)
		}
		rec := postCommit(t, e, `{"kb_id":"kb","session_id":"stranger:token"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d, want 409", rec.Code)
		}
		if len(recorder.calls) != 0 {
			t.Fatal("a stranger's commit queued the holder's writes")
		}
		// The rows are on another instance, so there is nothing to wait for.
		if got := rec.Header().Get("Retry-After"); got != "" {
			t.Fatalf("Retry-After %q sent for a session on another instance", got)
		}
	})

	t.Run("a commit sent to the wrong instance is refused", func(t *testing.T) {
		recorder := &commitRecorder{}
		e, sessions, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		handle, err := sessions.Hold(context.Background(), "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		_, token, _ := strings.Cut(handle, ":")

		rec := postCommit(t, e, `{"kb_id":"kb","session_id":`+strconv.Quote("other-instance:"+token)+`}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d, want 409", rec.Code)
		}
		if len(recorder.calls) != 0 {
			t.Fatal("an instance queued a publish for rows held elsewhere")
		}
	})

	t.Run("a commit that cannot be queued leaves the session with its holder", func(t *testing.T) {
		recorder := &commitRecorder{failWith: context.Canceled}
		e, sessions, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		handle, err := sessions.Hold(context.Background(), "kb", "")
		if err != nil {
			t.Fatal(err)
		}

		rec := postCommit(t, e, `{"kb_id":"kb","session_id":`+strconv.Quote(handle)+`}`)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status %d, want 500", rec.Code)
		}
		// Releasing here would hand the knowledge base to another writer while
		// this client's rows are still sitting unpublished.
		if _, err := sessions.Hold(context.Background(), "kb", ""); err == nil {
			t.Fatal("a failed commit dropped the caller's session")
		}
	})

	t.Run("a lapsed handle is refused rather than publishing what it finds", func(t *testing.T) {
		recorder := &commitRecorder{}
		e, sessions, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		handle, err := sessions.Hold(context.Background(), "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		if err := sessions.Release(context.Background(), "kb", handle); err != nil {
			t.Fatal(err)
		}

		// The handle names this instance but no longer holds anything.
		rec := postCommit(t, e, `{"kb_id":"kb","session_id":`+strconv.Quote(handle)+`}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		if len(recorder.calls) != 0 {
			t.Fatal("a lapsed handle queued a publish")
		}
	})

	t.Run("a conflict tells the caller when the holder lapses", func(t *testing.T) {
		recorder := &commitRecorder{}
		e, sessions, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		handle, err := sessions.Hold(context.Background(), "kb", "")
		if err != nil {
			t.Fatal(err)
		}
		_, token, _ := strings.Cut(handle, ":")
		stale := strings.Replace(handle, token, token+"-stale", 1)

		rec := postCommit(t, e, `{"kb_id":"kb","session_id":`+strconv.Quote(stale)+`}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status %d, want 409", rec.Code)
		}
		// Without a deadline the refused client has nothing to wait for and
		// gives up on writes it has already uploaded.
		after, err := strconv.Atoi(rec.Header().Get("Retry-After"))
		if err != nil || after <= 0 {
			t.Fatalf("Retry-After was %q", rec.Header().Get("Retry-After"))
		}
	})

	t.Run("a commit must name its session", func(t *testing.T) {
		recorder := &commitRecorder{}
		e, _, _ := newCommitServer(t, Dependencies{AppendSessionCommit: recorder.append})
		// Otherwise any caller can take a lease on any knowledge base and
		// publish whatever happens to be sitting there.
		if rec := postCommit(t, e, `{"kb_id":"kb"}`); rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", rec.Code)
		}
		if len(recorder.calls) != 0 {
			t.Fatal("a commit with no session queued a publish")
		}
	})

	t.Run("a server with deferred publishing off refuses to commit", func(t *testing.T) {
		recorder := &commitRecorder{}
		deps := Dependencies{AppendSessionCommit: recorder.append}
		dir := t.TempDir()
		deps.CacheDir = dir
		deps.Logger = slog.New(slog.DiscardHandler)
		deps.AppMetrics = kb.NoopAppMetrics{}
		deps.Sessions = kb.NewIngestSessions(nil, dir)
		e := echo.New()
		registerRagRoutes(e, deps, deps.Sessions)

		if rec := postCommit(t, e, `{"kb_id":"kb","session_id":"i:tok"}`); rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", rec.Code)
		}
	})
}
