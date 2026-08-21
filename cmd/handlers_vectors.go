package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
)

type vectorUpsertDoc struct {
	ID       string         `json:"id"`
	Vector   []float32      `json:"vector"`
	Metadata map[string]any `json:"metadata,omitempty"`
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
	KBID         string   `json:"kb_id"`
	IDs          []string `json:"ids"`
	DeferPublish bool     `json:"defer_publish,omitempty"`
	SessionID    string   `json:"session_id,omitempty"`
}

type vectorHandler struct {
	deps     Dependencies
	sessions *kb.IngestSessions
}

func registerVectorRoutes(e *echo.Echo, deps Dependencies, sessions *kb.IngestSessions) {
	h := vectorHandler{deps: deps, sessions: sessions}
	e.POST("/v1/vectors/upsert", h.upsert)
	e.POST("/v1/vectors/query", h.query)
	e.POST("/v1/vectors/fetch", h.fetch)
	e.DELETE("/v1/vectors", h.delete)
}

func (h vectorHandler) upsert(c echo.Context) error { return handleVectorUpsert(c, h.deps) }
func (h vectorHandler) query(c echo.Context) error  { return handleVectorQuery(c, h.deps) }
func (h vectorHandler) fetch(c echo.Context) error  { return handleVectorFetch(c, h.deps) }
func (h vectorHandler) delete(c echo.Context) error { return handleVectorDelete(c, h.deps, h.sessions) }

func handleVectorUpsert(c echo.Context, deps Dependencies) error {
	var req vectorUpsertRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = defaultKBIDValue
	}
	if len(req.Docs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "vectors must not be empty"})
	}
	if deps.AppendDocumentUpsert == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
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

	idempotencyKey, correlationID := requestIDs(c)
	opID, eventID, err := deps.AppendDocumentUpsert(c.Request().Context(), kb.DocumentUpsertPayload{
		KBID:      req.KBID,
		Documents: docs,
	}, idempotencyKey, correlationID)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusAccepted, map[string]any{"operation_id": opID, "event_id": eventID})
}

func handleVectorQuery(c echo.Context, deps Dependencies) error {
	req, err := bindVectorQueryRequest(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	if deps.QueryVectors == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	rawResults, err := deps.QueryVectors(c.Request().Context(), req.KBID, req.Vector, req.K, req.Filter)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	out := make([]vectorQueryResult, 0, len(rawResults))
	for _, r := range rawResults {
		out = append(out, vectorQueryResult{ID: r.ID, Distance: r.Distance, Metadata: r.Metadata})
	}
	return c.JSON(http.StatusOK, map[string]any{"results": out})
}

func bindVectorQueryRequest(c echo.Context) (vectorQueryRequest, error) {
	var req vectorQueryRequest
	if err := c.Bind(&req); err != nil {
		return req, fmt.Errorf(errInvalidRequestBody)
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = defaultKBIDValue
	}
	if len(req.Vector) == 0 {
		return req, fmt.Errorf("vector is required")
	}
	if req.K <= 0 {
		return req, fmt.Errorf("k must be > 0")
	}
	if req.K > maxQueryK {
		return req, fmt.Errorf("k must be <= %d", maxQueryK)
	}
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return req, fmt.Errorf("invalid filter: %w", err)
		}
	}
	return req, nil
}

func handleVectorFetch(c echo.Context, deps Dependencies) error {
	var req struct {
		KBID string   `json:"kb_id"`
		IDs  []string `json:"ids"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = defaultKBIDValue
	}
	if len(req.IDs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "ids must not be empty"})
	}
	if len(req.IDs) > maxQueryK {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: fmt.Sprintf("ids must not exceed %d", maxQueryK)})
	}
	if deps.FetchVectors == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	records, err := deps.FetchVectors(c.Request().Context(), req.KBID, req.IDs)
	if err != nil {
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, map[string]any{"records": records})
}

func handleVectorDelete(c echo.Context, deps Dependencies, sessions *kb.IngestSessions) error {
	var req vectorDeleteRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: errInvalidRequestBody})
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = defaultKBIDValue
	}
	if len(req.IDs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "ids must not be empty"})
	}
	if deps.DeleteDocuments == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}

	// A deferred delete needs the same session as a deferred ingest.
	var sessionID string
	if req.DeferPublish {
		if !deferredPublishReady(deps) {
			return c.JSON(
				http.StatusBadRequest,
				map[string]any{errorResponseKey: errDeferredPublishDisabled},
			)
		}
		held, err := sessions.Hold(c.Request().Context(), req.KBID, req.SessionID)
		if err != nil {
			return writeIngestSessionError(c, deps, sessions, req.KBID, err)
		}
		sessionID = held
	}
	deleteOpts := kb.DeleteDocsOptions{DeferPublish: req.DeferPublish}
	if err := deps.DeleteDocuments(c.Request().Context(), req.KBID, req.IDs, deleteOpts); err != nil {
		releaseIngestSession(c.Request().Context(), sessions, req.KBID, req.SessionID, sessionID)
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	return c.JSON(http.StatusOK, map[string]any{"ids": req.IDs, "session_id": sessionID})
}
