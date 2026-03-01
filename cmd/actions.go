package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mikills/kbcore/kb"

	"github.com/labstack/echo/v4"
)

type Dependencies struct {
	CacheMetricsHandler http.Handler
	AppMetrics          kb.AppMetrics
	SweepCache          func(context.Context) error
	IsBudgetExceeded    func(error) bool
	UpsertDocsAndUpload func(context.Context, string, []kb.Document, kb.UpsertDocsOptions) error
	Embed               func(context.Context, string) ([]float32, error)
	Search              func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error)
	Logger              *slog.Logger
}

type ragIngestRequest struct {
	KBID         string           `json:"kb_id"`
	ChunkSize    int              `json:"chunk_size"`
	GraphEnabled *bool            `json:"graph_enabled"`
	Documents    []ragIngestDocIn `json:"documents"`
}

type ragIngestDocIn struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

type ragQueryRequest struct {
	KBID       string `json:"kb_id"`
	Query      string `json:"query"`
	K          int    `json:"k"`
	SearchMode string `json:"search_mode,omitempty"`
}

type ragQueryResultOut struct {
	ID         string   `json:"id"`
	Content    string   `json:"content"`
	Distance   float64  `json:"distance"`
	Score      *float64 `json:"score,omitempty"`
	GraphScore *float64 `json:"graph_score,omitempty"`
}

func Register(e *echo.Echo, deps Dependencies) {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	metrics := deps.AppMetrics
	if metrics == nil {
		metrics = kb.NoopAppMetrics{}
	}

	e.GET("/healthz", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{"status": "ok"})
	})
	if deps.CacheMetricsHandler != nil {
		e.GET("/metrics/cache", echo.WrapHandler(deps.CacheMetricsHandler))
	}
	e.GET("/metrics/app", func(c echo.Context) error {
		return c.JSON(http.StatusOK, metrics.Snapshot())
	})
	e.POST("/cache/sweep", func(c echo.Context) error {
		if deps.SweepCache == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "kb unavailable"})
		}
		if err := deps.SweepCache(c.Request().Context()); err != nil {
			return WriteError(c, err, deps.IsBudgetExceeded)
		}
		return c.JSON(http.StatusOK, map[string]any{"status": "ok"})
	})

	e.POST("/rag/ingest", func(c echo.Context) error {
		start := time.Now()
		if deps.UpsertDocsAndUpload == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "kb unavailable"})
		}

		var req ragIngestRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid request body"})
		}
		req.KBID = strings.TrimSpace(req.KBID)
		if req.KBID == "" {
			req.KBID = "default"
		}
		if req.GraphEnabled == nil {
			err := fmt.Errorf("graph_enabled is required")
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
			return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		if len(req.Documents) == 0 {
			err := fmt.Errorf("documents are required")
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
			return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		opts := kb.UpsertDocsOptions{GraphEnabled: req.GraphEnabled}

		chunkSize := req.ChunkSize
		if chunkSize <= 0 {
			chunkSize = 500
		}
		chunker := kb.TextChunker{ChunkSize: chunkSize}

		chunks := make([]kb.Document, 0, len(req.Documents))
		docIDs := make([]string, 0, len(req.Documents))
		for _, doc := range req.Documents {
			baseID := strings.TrimSpace(doc.ID)
			text := strings.TrimSpace(doc.Text)
			if baseID == "" {
				baseID = generateDocID()
			}
			docIDs = append(docIDs, baseID)
			if text == "" {
				err := fmt.Errorf("each document requires non-empty text")
				metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), len(chunks), err)
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}

			docChunks, err := chunker.Chunk(c.Request().Context(), baseID, text)
			if err != nil {
				metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), len(chunks), err)
				logger.ErrorContext(c.Request().Context(), "rag ingest failed",
					"kb_id", req.KBID,
					"error", err,
				)
				return WriteError(c, err, deps.IsBudgetExceeded)
			}
			for _, chunk := range docChunks {
				chunks = append(chunks, kb.Document{
					ID:   chunk.ChunkID,
					Text: chunk.Text,
				})
			}
		}

		if err := deps.UpsertDocsAndUpload(c.Request().Context(), req.KBID, chunks, opts); err != nil {
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), len(chunks), err)
			logger.ErrorContext(c.Request().Context(), "rag ingest failed",
				"kb_id", req.KBID,
				"error", err,
			)
			if errors.Is(err, kb.ErrGraphUnavailable) {
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return WriteError(c, err, deps.IsBudgetExceeded)
		}

		logger.InfoContext(c.Request().Context(), "rag ingest completed",
			"kb_id", req.KBID,
			"ingested_docs", len(req.Documents),
			"ingested_chunks", len(chunks),
		)
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), len(chunks), nil)

		return c.JSON(http.StatusOK, map[string]any{
			"status":          "ok",
			"ingested_docs":   len(req.Documents),
			"ingested_chunks": len(chunks),
			"doc_ids":         docIDs,
		})
	})

	e.POST("/rag/query", func(c echo.Context) error {
		start := time.Now()
		if deps.Embed == nil || deps.Search == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "kb unavailable"})
		}

		var req ragQueryRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid request body"})
		}
		req.KBID = strings.TrimSpace(req.KBID)
		if req.KBID == "" {
			req.KBID = "default"
		}
		if strings.TrimSpace(req.Query) == "" {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "query is required"})
		}
		if req.K <= 0 {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "k must be > 0"})
		}
		mode, modeName, modeErr := parseSearchMode(req.SearchMode)
		if modeErr != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": modeErr.Error()})
		}

		embedStart := time.Now()
		vec, err := deps.Embed(c.Request().Context(), req.Query)
		embedLatencyMS := time.Since(embedStart).Milliseconds()
		metrics.RecordEmbed(req.KBID, embedLatencyMS, err)
		if err != nil {
			logger.ErrorContext(c.Request().Context(), "rag query failed",
				"kb_id", req.KBID,
				"error", err,
			)
			return WriteError(c, err, deps.IsBudgetExceeded)
		}

		searchOpts := &kb.SearchOptions{Mode: mode, TopK: req.K}
		results, err := deps.Search(c.Request().Context(), req.KBID, vec, searchOpts)
		if err != nil {
			metrics.RecordQuery(req.KBID, time.Since(start).Milliseconds(), 0, 0, err)
			logger.ErrorContext(c.Request().Context(), "rag query failed",
				"kb_id", req.KBID,
				"error", err,
			)
			if errors.Is(err, kb.ErrGraphQueryUnavailable) {
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			if errors.Is(err, kb.ErrKBUninitialized) {
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return WriteError(c, err, deps.IsBudgetExceeded)
		}

		includeScoring := mode != kb.SearchModeVector
		queryResults := make([]ragQueryResultOut, 0, len(results))
		for _, result := range results {
			out := ragQueryResultOut{
				ID:       result.ID,
				Content:  result.Content,
				Distance: result.Distance,
			}
			if includeScoring {
				score := result.Score
				graphScore := result.GraphScore
				out.Score = &score
				out.GraphScore = &graphScore
			}
			queryResults = append(queryResults, out)
		}

		queryForLog := strings.TrimSpace(req.Query)
		if runes := []rune(queryForLog); len(runes) > 100 {
			queryForLog = string(runes[:100])
		}
		logger.InfoContext(c.Request().Context(), "rag query completed",
			"kb_id", req.KBID,
			"search_mode", modeName,
			"query", queryForLog,
			"result_count", len(results),
			"latency_ms", time.Since(start).Milliseconds(),
		)
		topDistance := float32(0)
		if len(queryResults) > 0 {
			topDistance = float32(queryResults[0].Distance)
		}
		metrics.RecordQuery(req.KBID, time.Since(start).Milliseconds(), len(results), topDistance, nil)

		return c.JSON(http.StatusOK, map[string]any{"results": queryResults})
	})
}

func parseSearchMode(raw string) (kb.SearchMode, string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", "vector":
		return kb.SearchModeVector, "vector", nil
	case "graph":
		return kb.SearchModeGraph, "graph", nil
	case "adaptive":
		return kb.SearchModeAdaptive, "adaptive", nil
	default:
		return kb.SearchModeVector, "", fmt.Errorf("invalid search_mode: %q (allowed: vector, graph, adaptive)", raw)
	}
}

func generateDocID() string {
	return fmt.Sprintf("doc-%d-%s", time.Now().UnixMilli(), randomHex(3))
}

func randomHex(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func WriteError(c echo.Context, err error, isBudgetExceeded func(error) bool) error {
	if isBudgetExceeded != nil && isBudgetExceeded(err) {
		c.Response().Header().Set("Retry-After", "1")
		return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
	}
	return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
}
