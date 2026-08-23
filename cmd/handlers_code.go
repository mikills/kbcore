package cmd

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/mikills/minnow/kb"
	codeindex "github.com/mikills/minnow/kb/codeindex"
)

func registerCodeRoutes(e *echo.Echo, deps Dependencies) {
	e.POST("/v1/code/search", func(c echo.Context) error { return handleCodeSearch(c, deps) })
	e.POST("/v1/code/status", func(c echo.Context) error { return handleCodeStatus(c, deps) })
}

func handleCodeStatus(c echo.Context, deps Dependencies) error {
	var req struct {
		KBID      string `json:"kb_id"`
		SessionID string `json:"session_id"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	req.SessionID = strings.TrimSpace(req.SessionID)
	if req.KBID == "" || req.SessionID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id and session_id are required"})
	}
	if deps.CodeIndexPending == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	pending, err := deps.CodeIndexPending(c.Request().Context(), req.KBID, req.SessionID)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, map[string]any{"pending": pending})
}

func handleCodeSearch(c echo.Context, deps Dependencies) error {
	var req codeindex.SearchRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	req.ScopeID = strings.TrimSpace(req.ScopeID)
	req.Query = strings.TrimSpace(req.Query)
	if req.KBID == "" || req.ScopeID == "" || req.Query == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			errorResponseKey: "kb_id, scope_id, and query are required",
		})
	}
	if req.K <= 0 {
		req.K = 10
	}
	if req.K > maxQueryK {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "k must be <= 200"})
	}
	if deps.SearchCode == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	results, err := deps.SearchCode(c.Request().Context(), req.KBID, req.Query, kb.CodeSearchOptions{
		TopK: req.K, Path: req.Path, Language: req.Language, ScopeID: req.ScopeID,
	})
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, codeindex.SearchResponse{KBID: req.KBID, Results: results})
}
