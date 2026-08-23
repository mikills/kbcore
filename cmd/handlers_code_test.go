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

func TestCodeSearch(t *testing.T) {
	e := echo.New()
	var gotKB, gotQuery string
	var gotOpts kb.CodeSearchOptions
	registerCodeRoutes(e, Dependencies{
		SearchCode: func(_ context.Context, kbID, query string, opts kb.CodeSearchOptions) ([]kb.CodeSearchResult, error) {
			gotKB, gotQuery, gotOpts = kbID, query, opts
			return []kb.CodeSearchResult{{ID: "chunk", Path: "main.go", Content: "func main()"}}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/code/search", strings.NewReader(
		`{"kb_id":"repo","scope_id":"branch","query":"entry point","k":5,"path":"main","language":"go"}`,
	))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "repo", gotKB)
	require.Equal(t, "entry point", gotQuery)
	require.Equal(t, kb.CodeSearchOptions{TopK: 5, Path: "main", Language: "go", ScopeID: "branch"}, gotOpts)
	require.JSONEq(t, `{"kb_id":"repo","results":[{"id":"chunk","content":"func main()","distance":0,"path":"main.go"}]}`, rec.Body.String())
}

func TestCodeSearchRequiresScope(t *testing.T) {
	e := echo.New()
	registerCodeRoutes(e, Dependencies{})
	req := httptest.NewRequest(http.MethodPost, "/v1/code/search", strings.NewReader(
		`{"kb_id":"repo","query":"entry point"}`,
	))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCodeStatus(t *testing.T) {
	e := echo.New()
	registerCodeRoutes(e, Dependencies{CodeIndexPending: func(_ context.Context, kbID, sessionID string) (bool, error) {
		require.Equal(t, "repo", kbID)
		require.Equal(t, "instance:token", sessionID)
		return true, nil
	}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/code/status", strings.NewReader(
		`{"kb_id":"repo","session_id":"instance:token"}`,
	))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"pending":true}`, rec.Body.String())
}
