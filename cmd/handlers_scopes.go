package cmd

import (
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/mikills/minnow/kb"
)

type scopeRequest struct {
	KBID        string   `json:"kb_id"`
	ScopeID     string   `json:"scope_id"`
	DocumentIDs []string `json:"document_ids"`
	Revision    string   `json:"revision"`
}

type scopeGCRequest struct {
	KBID        string   `json:"kb_id"`
	DocumentIDs []string `json:"document_ids"`
}

func registerScopeRoutes(e *echo.Echo, deps Dependencies) {
	e.PUT("/v1/scopes", func(c echo.Context) error { return handleScopeReplace(c, deps) })
	e.GET("/v1/scopes", func(c echo.Context) error { return handleScopeList(c, deps) })
	e.GET("/v1/scopes/documents", func(c echo.Context) error { return handleScopeDocuments(c, deps) })
	e.POST("/v1/scopes/gc", func(c echo.Context) error { return handleScopeGC(c, deps) })
	e.GET("/v1/scopes/:scope_id", func(c echo.Context) error { return handleScopeGet(c, deps) })
	e.DELETE("/v1/scopes/:scope_id", func(c echo.Context) error { return handleScopeDelete(c, deps) })
}

func handleScopeDelete(c echo.Context, deps Dependencies) error {
	kbID := strings.TrimSpace(c.QueryParam("kb_id"))
	scopeID := strings.TrimSpace(c.Param("scope_id"))
	revision := strings.TrimSpace(c.QueryParam("revision"))
	if kbID == "" || scopeID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id and scope_id are required"})
	}
	if revision != "" && deps.DeleteScopeIfRevision == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	if revision == "" && deps.DeleteScope == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	var err error
	if revision != "" {
		err = deps.DeleteScopeIfRevision(c.Request().Context(), kbID, scopeID, revision)
	} else {
		err = deps.DeleteScope(c.Request().Context(), kbID, scopeID)
	}
	if err != nil {
		if errors.Is(err, kb.ErrScopeNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{errorResponseKey: err.Error()})
		}
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.NoContent(http.StatusNoContent)
}

func handleScopeGC(c echo.Context, deps Dependencies) error {
	var req scopeGCRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id is required"})
	}
	if deps.ScheduleScopeGC == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	scheduled, err := deps.ScheduleScopeGC(c.Request().Context(), req.KBID, req.DocumentIDs)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusAccepted, map[string]any{"scheduled_ids": scheduled})
}

func handleScopeDocuments(c echo.Context, deps Dependencies) error {
	kbID := strings.TrimSpace(c.QueryParam("kb_id"))
	if kbID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id is required"})
	}
	if deps.ListScopes == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	scopes, err := deps.ListScopes(c.Request().Context(), kbID)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	set := make(map[string]struct{})
	for _, scope := range scopes {
		for _, id := range scope.DocumentIDs {
			set[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return c.JSON(http.StatusOK, map[string]any{"document_ids": ids})
}

func handleScopeList(c echo.Context, deps Dependencies) error {
	kbID := strings.TrimSpace(c.QueryParam("kb_id"))
	if kbID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id is required"})
	}
	if deps.ListScopes == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	scopes, err := deps.ListScopes(c.Request().Context(), kbID)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, map[string]any{"scopes": scopes})
}

func handleScopeReplace(c echo.Context, deps Dependencies) error {
	var req scopeRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	req.ScopeID = strings.TrimSpace(req.ScopeID)
	if req.KBID == "" || req.ScopeID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id and scope_id are required"})
	}
	if deps.ReplaceScope == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	scope, err := deps.ReplaceScope(
		c.Request().Context(), req.KBID, req.ScopeID, req.DocumentIDs, req.Revision,
	)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, scope)
}

func handleScopeGet(c echo.Context, deps Dependencies) error {
	kbID := strings.TrimSpace(c.QueryParam("kb_id"))
	scopeID := strings.TrimSpace(c.Param("scope_id"))
	if kbID == "" || scopeID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id and scope_id are required"})
	}
	if deps.GetScope == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	scope, err := deps.GetScope(c.Request().Context(), kbID, scopeID)
	if errors.Is(err, kb.ErrScopeNotFound) {
		return c.JSON(http.StatusNotFound, map[string]any{errorResponseKey: err.Error()})
	}
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, scope)
}
