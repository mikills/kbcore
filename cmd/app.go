package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
	"github.com/mikills/minnow/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

const cacheSweepTimeout = 150 * time.Millisecond

var appRootContext = context.Background()

// kbIDHeader is the header nginx uses to forward the consistent-hash routing key.
const kbIDHeader = "X-KB-ID"

// kbIDContextKey is the echo context key for the resolved kb id.
const kbIDContextKey = "kb_id"

type AppConfig struct {
	Address               string
	ReadHeaderTimeout     time.Duration
	ShutdownTimeout       time.Duration
	CacheEvictionInterval time.Duration
	MaxMediaBytes         int64
	MCP                   mcpserver.Config
	Logger                *slog.Logger
}

func DefaultAppConfig() AppConfig {
	return AppConfig{
		Address:               "127.0.0.1:8080",
		ReadHeaderTimeout:     5 * time.Second,
		ShutdownTimeout:       5 * time.Second,
		CacheEvictionInterval: 30 * time.Second,
		Logger:                slog.Default(),
	}
}

type App struct {
	kb      *kb.KB
	echo    *echo.Echo
	config  AppConfig
	logger  *slog.Logger
	metrics kb.AppMetrics

	mu       sync.Mutex
	listener net.Listener
	errCh    chan error
	started  bool

	cacheEvictCancel context.CancelFunc
	cacheEvictDone   chan struct{}
}

// Metrics returns the AppMetrics observer for this app.
func (a *App) Metrics() kb.AppMetrics {
	if a == nil {
		return nil
	}
	return a.metrics
}

func NewApp(loader *kb.KB, cfg AppConfig) *App {
	cfg = mergeWithDefaultAppConfig(cfg)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	metrics := kb.AppMetrics(kb.NoopAppMetrics{})
	if m := kb.NewInMemAppMetrics(); m != nil {
		metrics = m
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(requestLoggerMiddleware(logger, metrics))
	e.Use(kbIDMiddleware())

	app := &App{
		kb:      loader,
		echo:    e,
		config:  cfg,
		logger:  logger,
		metrics: metrics,
		errCh:   make(chan error, 1),
	}
	app.registerRoutes()
	return app
}

// kbIDMiddleware extracts the X-KB-ID header set by nginx consistent-hash
// routing and stores it in the echo context for observability. the value
// is also echoed back in the response header for debugging.
func kbIDMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			id := strings.TrimSpace(c.Request().Header.Get(kbIDHeader))
			if id != "" {
				c.Set(kbIDContextKey, id)
				c.Response().Header().Set(kbIDHeader, id)
			}
			return next(c)
		}
	}
}

func mergeWithDefaultAppConfig(cfg AppConfig) AppConfig {
	d := DefaultAppConfig()
	if cfg.Address != "" {
		d.Address = cfg.Address
	}
	if cfg.ReadHeaderTimeout > 0 {
		d.ReadHeaderTimeout = cfg.ReadHeaderTimeout
	}
	if cfg.ShutdownTimeout > 0 {
		d.ShutdownTimeout = cfg.ShutdownTimeout
	}
	if cfg.CacheEvictionInterval > 0 {
		d.CacheEvictionInterval = cfg.CacheEvictionInterval
	}
	if cfg.Logger != nil {
		d.Logger = cfg.Logger
	}
	if cfg.MaxMediaBytes > 0 {
		d.MaxMediaBytes = cfg.MaxMediaBytes
	}
	d.MCP = cfg.MCP
	return d
}

func requestLoggerMiddleware(logger *slog.Logger, metrics kb.AppMetrics) echo.MiddlewareFunc {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = kb.NoopAppMetrics{}
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			if err != nil {
				c.Error(err)
			}

			status := c.Response().Status
			if status == 0 {
				status = http.StatusOK
			}
			latencyMS := time.Since(start).Milliseconds()
			path := c.Path()
			if path == "" {
				path = c.Request().URL.Path
			}
			metrics.RecordRequest(c.Request().Method, path, status, latencyMS)
			attrs := []any{
				"method", c.Request().Method,
				"path", path,
				"status", status,
				"latency_ms", latencyMS,
				"remote_ip", c.RealIP(),
			}

			switch {
			case status >= http.StatusInternalServerError:
				logger.ErrorContext(c.Request().Context(), "http request", attrs...)
			case status >= http.StatusBadRequest:
				logger.WarnContext(c.Request().Context(), "http request", attrs...)
			default:
				logger.InfoContext(c.Request().Context(), "http request", attrs...)
			}
			return nil
		}
	}
}

func (a *App) registerRoutes() {
	deps := buildKBDeps(a.kb, a.logger)
	deps.CacheMetricsHandler = kb.NewCacheOpenMetricsHandler(a.kb)
	deps.IsBudgetExceeded = func(err error) bool {
		return errors.Is(err, kb.ErrCacheBudgetExceeded)
	}
	deps.MaxMediaBytes = a.config.MaxMediaBytes
	deps.AppMetrics = a.metrics

	// HTTP-only closures: media upload and file ingest are exposed on the
	// REST surface but not via MCP.
	deps.AppendMediaUpload = appMediaUploadFn(a.kb)
	deps.AppendFileIngest = appFileIngestFn(a.kb)

	Register(a.echo, deps)
	a.registerMCPRoutes(deps)
	RegisterUI(a.echo)
}

func appMediaUploadFn(
	loader *kb.KB,
) func(context.Context, kb.MediaUploadInput, int64, string, string) (string, string, error) {
	if loader == nil || loader.MediaStore == nil || loader.EventStore == nil {
		return nil
	}
	return loader.AppendMediaUploadDetailed
}

func appFileIngestFn(
	loader *kb.KB,
) func(context.Context, kb.FileIngestInput, int64, string, string) (string, string, error) {
	if loader == nil || loader.EventStore == nil {
		return nil
	}
	return loader.AppendFileIngestDetailed
}

func (a *App) registerMCPRoutes(deps Dependencies) {
	cfg := a.config.MCP
	if !cfg.Enabled || !cfg.HTTPEnabled {
		return
	}
	server := mcpserver.New(mcpServiceFromDeps(cfg, deps))
	a.echo.Any(cfg.HTTPPath, echo.WrapHandler(mcpserver.NewHTTPHandler(server, cfg)))
}

func NewMCPServerFromKB(loader *kb.KB, cfg mcpserver.Config, logger *slog.Logger) *mcp.Server {
	if logger == nil {
		logger = slog.Default()
	}
	return mcpserver.New(mcpServiceFromDeps(cfg, buildKBDeps(loader, logger)))
}

// buildKBDeps returns the Dependencies subset that closes over the KB and is
// needed by both the HTTP route registration and the MCP service. Keeping this
// in one place ensures that adding a KB-backed handler shows up in HTTP and
// MCP at the same time
func buildKBDeps(loader *kb.KB, logger *slog.Logger) Dependencies {
	if logger == nil {
		logger = slog.Default()
	}
	deps := Dependencies{
		Logger:     logger,
		Embed:      kbEmbedFn(loader),
		Search:     kbSearchFn(loader),
		SweepCache: kbSweepCacheFn(loader),
	}
	if loader == nil {
		return deps
	}
	deps.ClearCache = kbClearCacheFn(loader)
	deps.ForceCompaction = loader.CompactIfNeeded
	deps.DeleteKnowledgeBase = loader.DeleteKnowledgeBase
	deps.IndexCodebase = loader.IndexCodebase
	deps.CodeIndexStatus = loader.CodeIndexStatus
	deps.SearchCode = loader.SearchCode
	deps.InstallCodeHooks = kb.InstallCodeIndexHooks
	deps.UninstallCodeHooks = kb.UninstallCodeIndexHooks
	deps.CodeHookStatus = kb.CodeIndexHookStatus
	// Media route closures are only installed when a MediaStore is wired.
	// When media is disabled, MediaStore is nil and these closures stay nil,
	// which makes cmd/actions.go return 503.
	if loader.MediaStore != nil {
		deps.GetMedia = loader.MediaStore.Get
		deps.ListMedia = loader.MediaStore.List
		deps.DeleteMedia = loader.TombstoneMedia
	}
	if loader.EventStore != nil {
		deps.AppendDocumentUpsert = loader.AppendDocumentUpsertDetailed
		deps.GetEvent = loader.EventStore.Get
		deps.FindOperationTerminal = loader.FindOperationTerminal
		deps.OperationStages = loader.OperationStages
	}
	deps.DeleteDocuments = func(ctx context.Context, kbID string, ids []string) error {
		return loader.DeleteDocsAndUpload(ctx, kbID, ids, kb.DeleteDocsOptions{})
	}
	deps.UpsertVectors = func(ctx context.Context, kbID string, docs []kb.Document) error {
		return loader.UpsertDocsAndUpload(ctx, kbID, docs)
	}
	deps.QueryVectors = func(ctx context.Context, kbID string, vec []float32, k int, filter *search.FilterExpr) ([]kb.QueryResult, error) {
		return loader.SearchRaw(ctx, kbID, vec, k, filter)
	}
	return deps
}

func kbEmbedFn(loader *kb.KB) func(context.Context, string) ([]float32, error) {
	return func(ctx context.Context, input string) ([]float32, error) {
		if loader == nil {
			return nil, fmt.Errorf("kb unavailable")
		}
		return loader.Embed(ctx, input)
	}
}

func kbSearchFn(
	loader *kb.KB,
) func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error) {
	return func(ctx context.Context, kbID string, queryVec []float32, opts *kb.SearchOptions) ([]kb.ExpandedResult, error) {
		if loader == nil {
			return nil, fmt.Errorf("kb unavailable")
		}
		return loader.Search(ctx, kbID, queryVec, opts)
	}
}

func kbSweepCacheFn(loader *kb.KB) func(context.Context) error {
	return func(ctx context.Context) error {
		if loader == nil {
			return fmt.Errorf("kb unavailable")
		}
		return loader.SweepCache(ctx)
	}
}

func kbClearCacheFn(loader *kb.KB) func(context.Context) error {
	return func(context.Context) error { return loader.ClearCache() }
}

func mcpServiceFromDeps(cfg mcpserver.Config, deps Dependencies) mcpserver.Service {
	return mcpserver.Service{
		Config:                cfg,
		Logger:                deps.Logger,
		Embed:                 deps.Embed,
		Search:                deps.Search,
		AppendDocumentUpsert:  deps.AppendDocumentUpsert,
		GetEvent:              deps.GetEvent,
		FindOperationTerminal: deps.FindOperationTerminal,
		OperationStages:       deps.OperationStages,
		ListMedia:             deps.ListMedia,
		GetMedia:              deps.GetMedia,
		DeleteMedia:           deps.DeleteMedia,
		SweepCache:            deps.SweepCache,
		ClearCache:            deps.ClearCache,
		ForceCompaction:       deps.ForceCompaction,
		DeleteKnowledgeBase:   deps.DeleteKnowledgeBase,
		UpsertVectors:         deps.UpsertVectors,
		QueryVectors:          deps.QueryVectors,
		IndexCodebase:         deps.IndexCodebase,
		CodeIndexStatus:       deps.CodeIndexStatus,
		SearchCode:            deps.SearchCode,
		InstallCodeHooks:      deps.InstallCodeHooks,
		UninstallCodeHooks:    deps.UninstallCodeHooks,
		CodeHookStatus:        deps.CodeHookStatus,
	}
}

func (a *App) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return fmt.Errorf("app already started")
	}

	if a.kb != nil {
		a.startCacheEvictionLoopLocked()
	}

	ln, err := net.Listen("tcp", a.config.Address)
	if err != nil {
		if a.kb != nil {
			a.stopCacheEvictionLoopLocked()
		}
		return err
	}
	a.listener = ln
	a.started = true

	srv := &http.Server{Handler: a.echo, ReadHeaderTimeout: a.config.ReadHeaderTimeout}
	a.echo.Server = srv

	go func() {
		err := a.echo.Server.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		a.errCh <- err
	}()

	return nil
}

func (a *App) Address() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.listener == nil {
		return ""
	}
	addr := a.listener.Addr().String()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "::" || host == "0.0.0.0" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func (a *App) Wait() error {
	return <-a.errCh
}

func (a *App) Stop(ctx context.Context) error {
	a.mu.Lock()
	started := a.started
	a.started = false
	a.mu.Unlock()

	if !started {
		return nil
	}

	if a.kb != nil {
		a.mu.Lock()
		a.stopCacheEvictionLoopLocked()
		a.mu.Unlock()
	}

	if ctx == nil {
		c, cancel := context.WithTimeout(appRootContext, a.config.ShutdownTimeout)
		defer cancel()
		ctx = c
	}

	if err := a.echo.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func (a *App) startCacheEvictionLoopLocked() {
	if a.kb == nil || a.config.CacheEvictionInterval <= 0 {
		return
	}
	if a.cacheEvictCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(appRootContext)
	done := make(chan struct{})
	a.cacheEvictCancel = cancel
	a.cacheEvictDone = done
	interval := a.config.CacheEvictionInterval

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, sweepCancel := context.WithTimeout(ctx, cacheSweepTimeout)
				if err := a.kb.SweepCache(sweepCtx); err != nil {
					a.config.Logger.Warn("cache eviction sweep failed", errorResponseKey, err)
				}
				sweepCancel()
			}
		}
	}()
}

func (a *App) stopCacheEvictionLoopLocked() {
	if a.cacheEvictCancel == nil {
		return
	}
	cancel := a.cacheEvictCancel
	done := a.cacheEvictDone
	a.cacheEvictCancel = nil
	a.cacheEvictDone = nil
	cancel()
	if done != nil {
		<-done
	}
}
