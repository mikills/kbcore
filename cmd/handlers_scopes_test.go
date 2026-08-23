package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestScopeRoutes(t *testing.T) {
	scopes := make(map[string]kb.Scope)
	e := echo.New()
	registerScopeRoutes(e, Dependencies{
		ReplaceScope: func(_ context.Context, kbID, scopeID string, ids []string, _ string) (kb.Scope, error) {
			scope := kb.Scope{KBID: kbID, ScopeID: scopeID, DocumentIDs: []string{"a", "b"}, Revision: "rev"}
			scopes[scopeID] = scope
			return scope, nil
		},
		GetScope: func(_ context.Context, _, scopeID string) (kb.Scope, error) {
			if scope, ok := scopes[scopeID]; ok {
				return scope, nil
			}
			return kb.Scope{}, kb.ErrScopeNotFound
		},
		ListScopes: func(context.Context, string) ([]kb.Scope, error) {
			out := make([]kb.Scope, 0, len(scopes))
			for _, scope := range scopes {
				out = append(out, scope)
			}
			return out, nil
		},
		DeleteScope: func(_ context.Context, _, scopeID string) error {
			delete(scopes, scopeID)
			return nil
		},
		DeleteScopeIfRevision: func(_ context.Context, _, scopeID, revision string) error {
			require.Equal(t, "rev", revision)
			delete(scopes, scopeID)
			return nil
		},
		ScheduleScopeGC: func(_ context.Context, _ string, ids []string) ([]string, error) {
			return ids, nil
		},
	})

	replace := httptest.NewRequest(http.MethodPut, "/v1/scopes", strings.NewReader(
		`{"kb_id":"kb","scope_id":"main","document_ids":["b","a"]}`,
	))
	replace.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, replace)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"revision":`)

	list := httptest.NewRequest(http.MethodGet, "/v1/scopes?kb_id=kb", nil)
	recorder = httptest.NewRecorder()
	e.ServeHTTP(recorder, list)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"scope_id":"main"`)

	documents := httptest.NewRequest(http.MethodGet, "/v1/scopes/documents?kb_id=kb", nil)
	recorder = httptest.NewRecorder()
	e.ServeHTTP(recorder, documents)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"document_ids":["a","b"]`)

	gc := httptest.NewRequest(http.MethodPost, "/v1/scopes/gc", strings.NewReader(
		`{"kb_id":"kb","document_ids":["old"]}`,
	))
	gc.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder = httptest.NewRecorder()
	e.ServeHTTP(recorder, gc)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"scheduled_ids":["old"]`)

	remove := httptest.NewRequest(http.MethodDelete, "/v1/scopes/main?kb_id=kb&revision=rev", nil)
	recorder = httptest.NewRecorder()
	e.ServeHTTP(recorder, remove)
	require.Equal(t, http.StatusNoContent, recorder.Code)

	missing := httptest.NewRequest(http.MethodGet, "/v1/scopes/missing?kb_id=kb", nil)
	recorder = httptest.NewRecorder()
	e.ServeHTTP(recorder, missing)
	require.Equal(t, http.StatusNotFound, recorder.Code)
}
