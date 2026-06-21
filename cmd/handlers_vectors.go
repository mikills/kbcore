package cmd

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
)

type vectorUpsertDoc struct {
	ID        string         `json:"id"`
	Vector    []float32      `json:"vector"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type vectorUpsertRequest struct {
	KBID string            `json:"kb_id"`
	Docs []vectorUpsertDoc `json:"vectors"`
}

type vectorQueryRequest struct {
	KBID   string             `json:"kb_id"`
	Vector []float32          `json:"vector"`
	K      int                `json:"k"`
	Filter *search.FilterExpr `json:"filter,omitempty"`
}

type vectorQueryResult struct {
	ID       string         `json:"id"`
	Distance float64        `json:"distance"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type vectorDeleteRequest struct {
	KBID string   `json:"kb_id"`
	IDs  []string `json:"ids"`
}

func registerVectorRoutes(e *echo.Echo, deps Dependencies) {
	e.POST("/v1/vectors/upsert", func(c echo.Context) error { return handleVectorUpsert(c, deps) })
	e.POST("/v1/vectors/query", func(c echo.Context) error { return handleVectorQuery(c, deps) })
	e.DELETE("/v1/vectors", func(c echo.Context) error { return handleVectorDelete(c, deps) })
}

func handleVectorUpsert(c echo.Context, deps Dependencies) error {
	var req vectorUpsertRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "invalid request body"})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = "default"
	}
	if len(req.Docs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "vectors must not be empty"})
	}
	if deps.UpsertVectors == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: "kb unavailable"})
	}

	docs := make([]kb.Document, 0, len(req.Docs))
	for _, d := range req.Docs {
		if strings.TrimSpace(d.ID) == "" {
			return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "each vector must have an id"})
		}
		if len(d.Vector) == 0 {
			return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "vector for id " + d.ID + " is empty"})
		}
		docs = append(docs, kb.Document{
			ID:        d.ID,
			Embedding: d.Vector,
			Metadata:  d.Metadata,
		})
	}

	if err := deps.UpsertVectors(c.Request().Context(), req.KBID, docs); err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, map[string]any{"upserted": len(docs)})
}

func handleVectorQuery(c echo.Context, deps Dependencies) error {
	var req vectorQueryRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "invalid request body"})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = "default"
	}
	if len(req.Vector) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "vector is required"})
	}
	if req.K <= 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "k must be > 0"})
	}
	if req.K > maxQueryK {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "k must be <= 200"})
	}
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "invalid filter: " + err.Error()})
		}
	}
	if deps.Search == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: "kb unavailable"})
	}

	results, err := deps.Search(c.Request().Context(), req.KBID, req.Vector, &kb.SearchOptions{
		Mode:   kb.SearchModeVector,
		TopK:   req.K,
		Filter: req.Filter,
	})
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}

	out := make([]vectorQueryResult, 0, len(results))
	for _, r := range results {
		out = append(out, vectorQueryResult{ID: r.ID, Distance: r.Distance, Metadata: r.Metadata})
	}
	return c.JSON(http.StatusOK, map[string]any{"results": out})
}

func handleVectorDelete(c echo.Context, deps Dependencies) error {
	var req vectorDeleteRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "invalid request body"})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = "default"
	}
	if len(req.IDs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "ids must not be empty"})
	}
	if deps.DeleteDocuments == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: "kb unavailable"})
	}

	if err := deps.DeleteDocuments(c.Request().Context(), req.KBID, req.IDs); err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, map[string]any{"deleted": len(req.IDs)})
}
