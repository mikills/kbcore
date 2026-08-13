package mcpserver

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// HTTPHandler owns the streamable-HTTP transport lifecycle for one MCP server.
type HTTPHandler struct {
	server      *mcp.Server
	next        http.Handler
	stateless   bool
	maxSessions int
	newSession  sync.Mutex
	closing     atomic.Bool
}

func NewHTTPHandler(server *mcp.Server, cfg Config) *HTTPHandler {
	cfg = cfg.normalized()
	base := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{
		Stateless:      cfg.HTTPStateless,
		JSONResponse:   cfg.HTTPJSONResponse,
		SessionTimeout: cfg.HTTPSessionTimeout,
	})
	return &HTTPHandler{
		server:      server,
		next:        base,
		stateless:   cfg.HTTPStateless,
		maxSessions: cfg.HTTPMaxSessions,
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.closing.Load() {
		http.Error(w, "MCP transport is shutting down", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<20)
	if h.stateless || r.Header.Get("Mcp-Session-Id") != "" {
		h.next.ServeHTTP(w, r)
		return
	}

	// Serialize initialization candidates so the session count is a hard
	// admission bound while established-session traffic remains concurrent.
	h.newSession.Lock()
	defer h.newSession.Unlock()
	if h.closing.Load() {
		http.Error(w, "MCP transport is shutting down", http.StatusServiceUnavailable)
		return
	}
	if activeSessionCount(h.server) >= h.maxSessions {
		http.Error(w, "MCP session capacity reached", http.StatusServiceUnavailable)
		return
	}
	h.next.ServeHTTP(w, r)
}

// Shutdown rejects new requests, waits for initialization admission to drain,
// and closes retained sessions within ctx.
func (h *HTTPHandler) Shutdown(ctx context.Context) error {
	h.closing.Store(true)
	admissionDone := make(chan struct{})
	go func() {
		h.newSession.Lock()
		h.newSession.Unlock()
		close(admissionDone)
	}()
	select {
	case <-admissionDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, h.ActiveSessions())
	for session := range h.server.Sessions() {
		wg.Add(1)
		go func(session *mcp.ServerSession) {
			defer wg.Done()
			if err := session.Close(); err != nil {
				errCh <- err
			}
		}(session)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	close(errCh)
	var closeErr error
	for err := range errCh {
		closeErr = errors.Join(closeErr, err)
	}
	return closeErr
}

func (h *HTTPHandler) ActiveSessions() int { return activeSessionCount(h.server) }

func activeSessionCount(server *mcp.Server) int {
	count := 0
	for range server.Sessions() {
		count++
	}
	return count
}

func RunStdio(ctx context.Context, server *mcp.Server) error {
	return server.Run(ctx, &mcp.StdioTransport{})
}
