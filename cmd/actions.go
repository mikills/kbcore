package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"

	"github.com/labstack/echo/v4"
)

type Dependencies struct {
	CacheMetricsHandler http.Handler
	AppMetrics          kb.AppMetrics
	SweepCache          func(context.Context) error
	ClearCache          func(context.Context) error
	IsBudgetExceeded    func(error) bool
	Embed               func(context.Context, string) ([]float32, error)
	Search              func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error)
	ForceCompaction     func(context.Context, string) (*kb.CompactionPublishResult, error)
	DeleteKnowledgeBase func(context.Context, string) error
	IndexCodebase       func(context.Context, kb.CodeIndexOptions) (kb.CodeIndexResult, error)
	CodeIndexStatus     func(context.Context, string) (kb.CodeIndexStatus, error)
	SearchCode          func(context.Context, string, string, kb.CodeSearchOptions) ([]kb.CodeSearchResult, error)
	CodeIndexPending    func(context.Context, string, string) (bool, error)
	InstallCodeHooks    func(context.Context, kb.CodeHookOptions) (kb.CodeHookStatus, error)
	UninstallCodeHooks  func(context.Context, string) (kb.CodeHookStatus, error)
	CodeHookStatus      func(context.Context, string) (kb.CodeHookStatus, error)

	// Media subsystem (optional. when nil the media endpoints 503).
	AppendMediaUpload func(context.Context, kb.MediaUploadInput, int64, string, string) (string, string, error)
	GetMedia          func(context.Context, string) (*kb.MediaObject, error)
	ListMedia         func(ctx context.Context, kbID, prefix, after string, limit int) (kb.MediaPage, error)
	DeleteMedia       func(context.Context, string) error
	MaxMediaBytes     int64

	CacheDir string
	Sessions *kb.IngestSessions
	// DeferredPublish gates the ingest_sessions capability. A session pins a
	// client to the instance holding its rows, so it is only offered where one
	// writer owns the data directory.
	DeferredPublish       bool
	AppendSessionCommit   func(context.Context, kb.SessionCommitPayload, string, string) (string, string, bool, error)
	DeleteDocuments       func(context.Context, string, []string, kb.DeleteDocsOptions) error
	FetchVectors          func(context.Context, string, []string) ([]kb.VectorRecord, error)
	QueryVectors          func(context.Context, string, []float32, int, *search.FilterExpr) ([]kb.QueryResult, error)
	ReplaceScope          func(context.Context, string, string, []string, string) (kb.Scope, error)
	GetScope              func(context.Context, string, string) (kb.Scope, error)
	ListScopes            func(context.Context, string) ([]kb.Scope, error)
	DeleteScope           func(context.Context, string, string) error
	DeleteScopeIfRevision func(context.Context, string, string, string) error
	ScheduleScopeGC       func(context.Context, string, []string) ([]string, error)

	// Event-driven ingest.
	AppendDocumentUpsert  func(context.Context, kb.DocumentUpsertPayload, string, string) (string, string, error)
	AppendFileIngest      func(context.Context, kb.FileIngestInput, int64, string, string) (string, string, error)
	GetEvent              func(context.Context, string) (*kb.KBEvent, error)
	FindOperationTerminal func(context.Context, string) (*kb.KBEvent, error)
	OperationStages       func(context.Context, string) ([]kb.OperationStageSnapshot, error)

	Logger *slog.Logger
}

func Register(e *echo.Echo, deps Dependencies) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.AppMetrics == nil {
		deps.AppMetrics = kb.NoopAppMetrics{}
	}
	sessions := deps.Sessions
	if sessions == nil {
		sessions = kb.NewIngestSessions(nil, deps.CacheDir)
	}
	registerOpsRoutes(e, deps)
	registerCacheRoutes(e, deps)
	registerRagRoutes(e, deps, sessions)
	registerMediaRoutes(e, deps)
	registerVectorRoutes(e, deps, sessions)
	registerScopeRoutes(e, deps)
	registerCodeRoutes(e, deps)
}

func requestIDs(c echo.Context) (string, string) {
	return strings.TrimSpace(
			c.Request().Header.Get("Idempotency-Key"),
		), strings.TrimSpace(
			c.Request().Header.Get("X-Correlation-Id"),
		)
}

const errorResponseKey = "error"
const eventIDResponseKey = "event_id"
const errKBUnavailable = "kb unavailable"
const errInvalidRequestBody = "invalid request body"
const defaultKBIDValue = "default"

func writeAcceptedOperation(c echo.Context, evtID, effectiveIdem string, body map[string]any) error {
	c.Response().Header().Set("X-Source-Event-Id", evtID)
	c.Response().Header().Set("Idempotency-Key", effectiveIdem)
	return c.JSON(http.StatusAccepted, body)
}

func WriteError(c echo.Context, err error, isBudgetExceeded func(error) bool) error {
	if isBudgetExceeded != nil && isBudgetExceeded(err) {
		c.Response().Header().Set("Retry-After", "1")
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: err.Error()})
	}
	// Another client holding the knowledge base, not this server failing.
	if errors.Is(err, kb.ErrUnpublishedWrites) || errors.Is(err, kb.ErrBlobVersionMismatch) ||
		errors.Is(err, kb.ErrScopedDocuments) || errors.Is(err, kb.ErrScopeDocumentsMissing) ||
		errors.Is(err, kb.ErrWriteLeaseConflict) {
		return c.JSON(http.StatusConflict, map[string]any{errorResponseKey: err.Error()})
	}
	return c.JSON(http.StatusInternalServerError, map[string]any{errorResponseKey: err.Error()})
}

func parsePositiveInt(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	var v int
	if _, err := fmt.Sscanf(raw, "%d", &v); err != nil || v < 0 {
		return fallback
	}
	return v
}
