package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime/multipart"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"

	"github.com/labstack/echo/v4"
)

const maxQueryK = 200

type ragIngestRequest struct {
	KBID         string           `json:"kb_id"`
	ChunkSize    int              `json:"chunk_size"`
	GraphEnabled *bool            `json:"graph_enabled"`
	Documents    []ragIngestDocIn `json:"documents"`
}

type ragIngestDocIn struct {
	ID        string             `json:"id"`
	Text      string             `json:"text"`
	MediaIDs  []string           `json:"media_ids,omitempty"`
	MediaRefs []kb.ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata  map[string]any     `json:"metadata,omitempty"`
}

type ragQueryRequest struct {
	KBID       string             `json:"kb_id"`
	Query      string             `json:"query"`
	K          int                `json:"k"`
	SearchMode string             `json:"search_mode,omitempty"`
	Filter     *search.FilterExpr `json:"filter,omitempty"`
}

type ragQueryResultOut struct {
	ID         string             `json:"id"`
	Content    string             `json:"content"`
	Distance   float64            `json:"distance"`
	Score      *float64           `json:"score,omitempty"`
	GraphScore *float64           `json:"graph_score,omitempty"`
	MediaRefs  []kb.ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata   map[string]any     `json:"metadata,omitempty"`
}

type multipartFileMetadata struct {
	Metadata map[string]any `json:"metadata,omitempty"`
}

type multipartIngestRequest struct {
	KBID         string
	ChunkSize    int
	GraphEnabled *bool
	Documents    []kb.Document
	FileIDs      []string
	FileMetadata map[string]multipartFileMetadata
}

func registerRagRoutes(e *echo.Echo, deps Dependencies) {
	e.POST("/rag/ingest", func(c echo.Context) error {
		return handleRagIngest(c, deps)
	})
	e.POST("/rag/query", func(c echo.Context) error {
		return handleRagQuery(c, deps)
	})
}

func handleRagIngest(c echo.Context, deps Dependencies) error {
	start := time.Now()
	logger := deps.Logger
	metrics := deps.AppMetrics
	if deps.AppendDocumentUpsert == nil && deps.AppendFileIngest == nil {
		return c.JSON(
			http.StatusServiceUnavailable,
			map[string]any{errorResponseKey: "ingest event pipeline not configured"},
		)
	}
	if strings.HasPrefix(strings.ToLower(c.Request().Header.Get("Content-Type")), "multipart/form-data") {
		return handleMultipartIngest(c, deps, logger, metrics, start)
	}
	req, err := bindRagIngestRequest(c)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	docs, docIDs, opts, err := buildIngestDocuments(req)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	evtID, effectiveIdem, err := appendRagIngestEvent(c, deps, req, docs, opts)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
		if errors.Is(err, kb.ErrGraphUnavailable) {
			return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
		}
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	logger.InfoContext(
		c.Request().Context(),
		"rag ingest accepted",
		kbIDContextKey,
		req.KBID,
		"document_count",
		len(req.Documents),
		eventIDResponseKey,
		evtID,
	)
	metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, nil)
	return writeAcceptedOperation(
		c,
		evtID,
		effectiveIdem,
		map[string]any{
			eventIDResponseKey: evtID,
			"status_url":       "/rag/operations/" + evtID,
			kbIDContextKey:     req.KBID,
			"document_count":   len(req.Documents),
			"doc_ids":          docIDs,
		},
	)
}

func bindRagIngestRequest(c echo.Context) (ragIngestRequest, error) {
	var req ragIngestRequest
	if err := c.Bind(&req); err != nil {
		return req, fmt.Errorf(errInvalidRequestBody)
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = "default"
	}
	if req.GraphEnabled == nil {
		return req, fmt.Errorf("graph_enabled is required")
	}
	if len(req.Documents) == 0 {
		return req, fmt.Errorf("documents are required")
	}
	return req, nil
}

func appendRagIngestEvent(
	c echo.Context,
	deps Dependencies,
	req ragIngestRequest,
	docs []kb.Document,
	opts kb.UpsertDocsOptions,
) (string, string, error) {
	idemKey, corr := requestIDs(c)
	return deps.AppendDocumentUpsert(
		c.Request().Context(),
		kb.DocumentUpsertPayload{KBID: req.KBID, Documents: docs, ChunkSize: req.ChunkSize, Options: opts},
		idemKey,
		corr,
	)
}

func handleRagQuery(c echo.Context, deps Dependencies) error {
	start := time.Now()
	logger := deps.Logger
	metrics := deps.AppMetrics
	if deps.Embed == nil || deps.Search == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
	}
	req, mode, modeName, err := bindRagQueryRequest(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	vec, err := embedRagQuery(c, deps, req, metrics)
	if err != nil {
		logger.ErrorContext(c.Request().Context(), "rag query failed", kbIDContextKey, req.KBID, errorResponseKey, err)
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	results, err := deps.Search(c.Request().Context(), req.KBID, vec, &kb.SearchOptions{Mode: mode, TopK: req.K, Filter: req.Filter, QueryText: req.Query})
	if err != nil {
		return handleRagSearchError(
			ragSearchErrorContext{c: c, deps: deps, logger: logger, metrics: metrics, start: start, req: req, err: err},
		)
	}
	includeScoring := mode != kb.SearchModeVector && mode != kb.SearchModeBM25
	queryResults := ragQueryResults(results, includeScoring)
	logRagQuery(
		ragQueryLog{c: c, logger: logger, req: req, modeName: modeName, resultCount: len(results), start: start},
	)
	recordRagQuerySuccess(metrics, req.KBID, start, results, queryResults)
	return c.JSON(http.StatusOK, map[string]any{"results": queryResults})
}

func bindRagQueryRequest(c echo.Context) (ragQueryRequest, kb.SearchMode, string, error) {
	var req ragQueryRequest
	if err := c.Bind(&req); err != nil {
		return req, kb.SearchModeVector, "", fmt.Errorf(errInvalidRequestBody)
	}
	req.KBID = strings.TrimSpace(req.KBID)
	if req.KBID == "" {
		req.KBID = "default"
	}
	if strings.TrimSpace(req.Query) == "" {
		return req, kb.SearchModeVector, "", fmt.Errorf("query is required")
	}
	if req.K <= 0 {
		return req, kb.SearchModeVector, "", fmt.Errorf("k must be > 0")
	}
	if req.K > maxQueryK {
		return req, kb.SearchModeVector, "", fmt.Errorf("k must be <= %d", maxQueryK)
	}
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return req, kb.SearchModeVector, "", fmt.Errorf("invalid filter: %w", err)
		}
	}
	mode, modeName, err := parseSearchMode(req.SearchMode)
	return req, mode, modeName, err
}

func embedRagQuery(c echo.Context, deps Dependencies, req ragQueryRequest, metrics kb.AppMetrics) ([]float32, error) {
	embedStart := time.Now()
	vec, err := deps.Embed(c.Request().Context(), req.Query)
	metrics.RecordEmbed(req.KBID, time.Since(embedStart).Milliseconds(), err)
	return vec, err
}

type ragSearchErrorContext struct {
	c       echo.Context
	deps    Dependencies
	logger  *slog.Logger
	metrics kb.AppMetrics
	start   time.Time
	req     ragQueryRequest
	err     error
}

func handleRagSearchError(ctx ragSearchErrorContext) error {
	ctx.metrics.RecordQuery(ctx.req.KBID, time.Since(ctx.start).Milliseconds(), 0, 0, ctx.err)
	ctx.logger.ErrorContext(
		ctx.c.Request().Context(),
		"rag query failed",
		kbIDContextKey,
		ctx.req.KBID,
		errorResponseKey,
		ctx.err,
	)
	if errors.Is(ctx.err, kb.ErrGraphQueryUnavailable) || errors.Is(ctx.err, kb.ErrKBUninitialized) {
		return ctx.c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: ctx.err.Error()})
	}
	return WriteError(ctx.c, ctx.err, ctx.deps.IsBudgetExceeded)
}

func ragQueryResults(results []kb.ExpandedResult, includeScoring bool) []ragQueryResultOut {
	queryResults := make([]ragQueryResultOut, 0, len(results))
	for _, result := range results {
		out := ragQueryResultOut{ID: result.ID, Content: result.Content, Distance: result.Distance}
		if includeScoring {
			score := result.Score
			graphScore := result.GraphScore
			out.Score = &score
			out.GraphScore = &graphScore
		}
		if len(result.MediaRefs) > 0 {
			out.MediaRefs = result.MediaRefs
		}
		if len(result.Metadata) > 0 {
			out.Metadata = result.Metadata
		}
		queryResults = append(queryResults, out)
	}
	return queryResults
}

type ragQueryLog struct {
	c           echo.Context
	logger      *slog.Logger
	req         ragQueryRequest
	modeName    string
	resultCount int
	start       time.Time
}

func logRagQuery(entry ragQueryLog) {
	queryForLog := strings.TrimSpace(entry.req.Query)
	if runes := []rune(queryForLog); len(runes) > 100 {
		queryForLog = string(runes[:100])
	}
	entry.logger.InfoContext(
		entry.c.Request().Context(),
		"rag query completed",
		kbIDContextKey,
		entry.req.KBID,
		"search_mode",
		entry.modeName,
		"query",
		queryForLog,
		"result_count",
		entry.resultCount,
		"latency_ms",
		time.Since(entry.start).Milliseconds(),
	)
}

func recordRagQuerySuccess(
	metrics kb.AppMetrics,
	kbID string,
	start time.Time,
	results []kb.ExpandedResult,
	queryResults []ragQueryResultOut,
) {
	topDistance := float32(0)
	if len(queryResults) > 0 {
		topDistance = float32(queryResults[0].Distance)
	}
	metrics.RecordQuery(kbID, time.Since(start).Milliseconds(), len(results), topDistance, nil)
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
	case "bm25":
		return kb.SearchModeBM25, "bm25", nil
	case "hybrid":
		return kb.SearchModeHybrid, "hybrid", nil
	default:
		return kb.SearchModeVector, "", fmt.Errorf("invalid search_mode: %q (allowed: vector, graph, adaptive, bm25, hybrid)", raw)
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

func buildIngestDocuments(req ragIngestRequest) ([]kb.Document, []string, kb.UpsertDocsOptions, error) {
	if req.GraphEnabled == nil {
		return nil, nil, kb.UpsertDocsOptions{}, fmt.Errorf("graph_enabled is required")
	}
	docs := make([]kb.Document, 0, len(req.Documents))
	docIDs := make([]string, 0, len(req.Documents))
	for _, doc := range req.Documents {
		id := strings.TrimSpace(doc.ID)
		text := strings.TrimSpace(doc.Text)
		if id == "" {
			id = generateDocID()
		}
		docIDs = append(docIDs, id)
		if text == "" {
			return nil, nil, kb.UpsertDocsOptions{}, fmt.Errorf("each document requires non-empty text")
		}
		docs = append(
			docs,
			kb.Document{ID: id, Text: text, MediaIDs: doc.MediaIDs, MediaRefs: doc.MediaRefs, Metadata: doc.Metadata},
		)
	}
	return docs, docIDs, kb.UpsertDocsOptions{GraphEnabled: req.GraphEnabled}, nil
}

func handleMultipartIngest(
	c echo.Context,
	deps Dependencies,
	logger *slog.Logger,
	metrics kb.AppMetrics,
	start time.Time,
) error {
	if err := prepareMultipartIngestRequest(c, deps); err != nil {
		return err
	}
	form := c.Request().MultipartForm
	req, files, docIDs, err := buildMultipartIngestInput(form)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), 0, 0, err)
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	defer closeMultipartFiles(files)
	fileUploads := multipartFileUploads(req, form, files, docIDs)
	input := kb.FileIngestInput{
		KBID:      req.KBID,
		Documents: req.Documents,
		Files:     fileUploads,
		ChunkSize: req.ChunkSize,
		Options:   kb.UpsertDocsOptions{GraphEnabled: req.GraphEnabled},
	}
	idemKey := strings.TrimSpace(c.Request().Header.Get("Idempotency-Key"))
	corr := strings.TrimSpace(c.Request().Header.Get("X-Correlation-Id"))
	evtID, effectiveIdem, err := deps.AppendFileIngest(c.Request().Context(), input, deps.MaxMediaBytes, idemKey, corr)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents)+len(fileUploads), 0, err)
		return writeMultipartIngestError(c, deps, err)
	}
	c.Response().Header().Set("X-Source-Event-Id", evtID)
	c.Response().Header().Set("Idempotency-Key", effectiveIdem)
	logger.InfoContext(
		c.Request().Context(),
		"multipart rag ingest accepted",
		kbIDContextKey,
		req.KBID,
		"document_count",
		len(req.Documents)+len(fileUploads),
		eventIDResponseKey,
		evtID,
	)
	metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents)+len(fileUploads), 0, nil)
	return c.JSON(
		http.StatusAccepted,
		map[string]any{
			eventIDResponseKey: evtID,
			"status_url":       "/rag/operations/" + evtID,
			kbIDContextKey:     req.KBID,
			"document_count":   len(req.Documents) + len(fileUploads),
			"doc_ids":          docIDs,
		},
	)
}

func prepareMultipartIngestRequest(c echo.Context, deps Dependencies) error {
	if deps.AppendFileIngest == nil {
		return c.JSON(
			http.StatusServiceUnavailable,
			map[string]any{errorResponseKey: "multipart file ingest not configured"},
		)
	}
	if deps.MaxMediaBytes > 0 && c.Request().ContentLength > deps.MaxMediaBytes {
		return c.JSON(
			http.StatusRequestEntityTooLarge,
			map[string]any{errorResponseKey: "upload exceeds maximum allowed size"},
		)
	}
	if deps.MaxMediaBytes > 0 {
		c.Request().Body = http.MaxBytesReader(c.Response().Writer, c.Request().Body, deps.MaxMediaBytes)
	}
	if err := c.Request().ParseMultipartForm(deps.MaxMediaBytes); err != nil {
		return multipartParseError(c, err)
	}
	return nil
}

func multipartParseError(c echo.Context, err error) error {
	if isRequestBodyTooLarge(err) {
		return c.JSON(
			http.StatusRequestEntityTooLarge,
			map[string]any{errorResponseKey: "upload exceeds maximum allowed size"},
		)
	}
	return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "invalid multipart form"})
}

func multipartFileUploads(
	req multipartIngestRequest,
	form *multipart.Form,
	files []multipart.File,
	docIDs []string,
) []kb.FileIngestUpload {
	fileUploads := make([]kb.FileIngestUpload, 0, len(files))
	for i, fh := range multipartFiles(form) {
		fileID := req.FileIDs[i]
		meta := req.FileMetadata[fileID]
		fileUploads = append(
			fileUploads,
			kb.FileIngestUpload{
				FileID:      fileID,
				DocumentID:  docIDs[len(req.Documents)+i],
				Filename:    fh.Filename,
				ContentType: fh.Header.Get("Content-Type"),
				Metadata:    meta.Metadata,
				Body:        files[i],
			},
		)
	}
	return fileUploads
}

func writeMultipartIngestError(c echo.Context, deps Dependencies, err error) error {
	if errors.Is(err, kb.ErrGraphUnavailable) {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	return WriteError(c, err, deps.IsBudgetExceeded)
}

func buildMultipartIngestInput(form *multipart.Form) (multipartIngestRequest, []multipart.File, []string, error) {
	req, err := parseMultipartIngestRequest(form)
	if err != nil {
		return req, nil, nil, err
	}
	fhs := multipartFiles(form)
	if err := validateMultipartFiles(req, fhs); err != nil {
		return req, nil, nil, err
	}
	if err := populateMultipartFileIDs(form, &req); err != nil {
		return req, nil, nil, err
	}
	if err := populateMultipartMetadata(form, &req); err != nil {
		return req, nil, nil, err
	}
	opened, docIDs, err := openMultipartIngestFiles(req, fhs)
	if err != nil {
		return req, nil, nil, err
	}
	return req, opened, docIDs, nil
}

func parseMultipartIngestRequest(form *multipart.Form) (multipartIngestRequest, error) {
	var req multipartIngestRequest
	req.KBID = strings.TrimSpace(firstFormValue(form, kbIDContextKey))
	if req.KBID == "" {
		req.KBID = "default"
	}
	gb, err := parseRequiredBool(firstFormValue(form, "graph_enabled"), "graph_enabled")
	if err != nil {
		return req, err
	}
	req.GraphEnabled = &gb
	req.ChunkSize = parsePositiveInt(firstFormValue(form, "chunk_size"), 0)
	return req, parseMultipartDocuments(form, &req)
}

func parseMultipartDocuments(form *multipart.Form, req *multipartIngestRequest) error {
	raw := strings.TrimSpace(firstFormValue(form, "documents"))
	if raw == "" {
		return nil
	}
	var docs []ragIngestDocIn
	if err := json.Unmarshal([]byte(raw), &docs); err != nil {
		return fmt.Errorf("documents must be valid JSON")
	}
	built, _, _, err := buildIngestDocuments(
		ragIngestRequest{KBID: req.KBID, ChunkSize: req.ChunkSize, GraphEnabled: req.GraphEnabled, Documents: docs},
	)
	if err != nil {
		return err
	}
	req.Documents = built
	return nil
}

func validateMultipartFiles(req multipartIngestRequest, fhs []*multipart.FileHeader) error {
	if len(req.Documents) == 0 && len(fhs) == 0 {
		return fmt.Errorf("documents or files are required")
	}
	return nil
}

func populateMultipartFileIDs(form *multipart.Form, req *multipartIngestRequest) error {
	for _, fileID := range form.Value["file_ids"] {
		trimmed := strings.TrimSpace(fileID)
		if trimmed == "" {
			continue
		}
		if containsString(req.FileIDs, trimmed) {
			return fmt.Errorf("file_ids must be unique")
		}
		req.FileIDs = append(req.FileIDs, trimmed)
	}
	if len(multipartFiles(form)) > 0 && len(req.FileIDs) != len(multipartFiles(form)) {
		return fmt.Errorf("file_ids count must match file count")
	}
	return nil
}

func populateMultipartMetadata(form *multipart.Form, req *multipartIngestRequest) error {
	req.FileMetadata = make(map[string]multipartFileMetadata, len(req.FileIDs))
	if raw := strings.TrimSpace(firstFormValue(form, "file_metadata")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &req.FileMetadata); err != nil {
			return fmt.Errorf("file_metadata must be valid JSON")
		}
		for fileID := range req.FileMetadata {
			if !containsString(req.FileIDs, fileID) {
				return fmt.Errorf("file_metadata contains unknown file_id %q", fileID)
			}
		}
	}
	for _, fileID := range req.FileIDs {
		if _, ok := req.FileMetadata[fileID]; !ok {
			req.FileMetadata[fileID] = multipartFileMetadata{}
		}
	}
	return nil
}

func openMultipartIngestFiles(
	req multipartIngestRequest,
	fhs []*multipart.FileHeader,
) ([]multipart.File, []string, error) {
	opened := make([]multipart.File, 0, len(fhs))
	docIDs := make([]string, 0, len(req.Documents)+len(fhs))
	for _, doc := range req.Documents {
		docIDs = append(docIDs, doc.ID)
	}
	for i, fh := range fhs {
		file, err := openMultipartFile(fh)
		if err != nil {
			closeMultipartFiles(opened)
			return nil, nil, fmt.Errorf("cannot open uploaded file %q", fh.Filename)
		}
		opened = append(opened, file)
		docIDs = append(docIDs, req.FileIDs[i])
	}
	return opened, docIDs, nil
}

func openMultipartFile(fh *multipart.FileHeader) (multipart.File, error) {
	return fh.Open()
}

func closeMultipartFiles(files []multipart.File) {
	for _, file := range files {
		if file != nil {
			if err := file.Close(); err != nil {
				slog.Default().Warn("multipart file close failed", errorResponseKey, err)
			}
		}
	}
}

func multipartFiles(form *multipart.Form) []*multipart.FileHeader {
	if form == nil {
		return nil
	}
	out := append([]*multipart.FileHeader(nil), form.File["files"]...)
	if len(out) == 0 {
		out = append(out, form.File["file"]...)
	}
	return out
}

func firstFormValue(form *multipart.Form, key string) string {
	if form == nil || len(form.Value[key]) == 0 {
		return ""
	}
	return form.Value[key][0]
}

func containsString(items []string, target string) bool {
	return slices.Contains(items, target)
}

func parseRequiredBool(raw, field string) (bool, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return false, fmt.Errorf("%s is required", field)
	}
	switch raw {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be true or false", field)
	}
}

func isRequestBodyTooLarge(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "request body too large") || strings.Contains(msg, "http: request body too large")
}
