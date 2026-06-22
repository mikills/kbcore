package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const errKBUnavailable = "kb unavailable"

type Service struct {
	Config Config
	Logger *slog.Logger

	Embed                 func(context.Context, string) ([]float32, error)
	Search                func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error)
	AppendDocumentUpsert  func(context.Context, kb.DocumentUpsertPayload, string, string) (string, string, error)
	GetEvent              func(context.Context, string) (*kb.KBEvent, error)
	FindOperationTerminal func(context.Context, string) (*kb.KBEvent, error)
	OperationStages       func(context.Context, string) ([]kb.OperationStageSnapshot, error)
	ListMedia             func(context.Context, string, string, string, int) (kb.MediaPage, error)
	GetMedia              func(context.Context, string) (*kb.MediaObject, error)
	DeleteMedia           func(context.Context, string) error
	SweepCache            func(context.Context) error
	ClearCache            func(context.Context) error
	ForceCompaction       func(context.Context, string) (*kb.CompactionPublishResult, error)
	DeleteKnowledgeBase   func(context.Context, string) error
	FetchVectors func(context.Context, string, []string) ([]kb.VectorRecord, error)
	QueryVectors func(context.Context, string, []float32, int, *search.FilterExpr) ([]kb.QueryResult, error)
	IndexCodebase         func(context.Context, kb.CodeIndexOptions) (kb.CodeIndexResult, error)
	CodeIndexStatus       func(context.Context, string) (kb.CodeIndexStatus, error)
	SearchCode            func(context.Context, string, string, kb.CodeSearchOptions) ([]kb.CodeSearchResult, error)
	InstallCodeHooks      func(context.Context, kb.CodeHookOptions) (kb.CodeHookStatus, error)
	UninstallCodeHooks    func(context.Context, string) (kb.CodeHookStatus, error)
	CodeHookStatus        func(context.Context, string) (kb.CodeHookStatus, error)
}

type queryInput struct {
	KBID       string          `json:"kb_id,omitempty"       jsonschema:"Knowledge base ID. Defaults to default."`
	Query      string          `json:"query"                 jsonschema:"Natural language query."`
	K          int             `json:"k"                     jsonschema:"Number of results to return."`
	SearchMode string          `json:"search_mode,omitempty" jsonschema:"vector, graph, adaptive, bm25, or hybrid."`
	Filter     json.RawMessage `json:"filter,omitempty"      jsonschema:"Optional metadata predicate filter as a JSON FilterExpr object."`
}

type queryOutput struct {
	KBID       string      `json:"kb_id"`
	SearchMode string      `json:"search_mode"`
	Results    []resultOut `json:"results"`
}

type resultOut struct {
	ID         string             `json:"id"`
	Content    string             `json:"content"`
	Distance   float64            `json:"distance"`
	Score      *float64           `json:"score,omitempty"`
	GraphScore *float64           `json:"graph_score,omitempty"`
	MediaRefs  []kb.ChunkMediaRef `json:"media_refs,omitempty"`
}

func (s *Service) query(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in queryInput,
) (*mcp.CallToolResult, queryOutput, error) {
	kbID := defaultKBID(in.KBID)
	if err := validateQueryInput(in); err != nil {
		return nil, queryOutput{}, err
	}
	mode, modeName, err := parseSearchMode(in.SearchMode)
	if err != nil {
		return nil, queryOutput{}, err
	}
	if s.Embed == nil || s.Search == nil {
		return nil, queryOutput{}, fmt.Errorf(errKBUnavailable)
	}
	vec, err := s.Embed(ctx, in.Query)
	if err != nil {
		return nil, queryOutput{}, err
	}
	filter, err := decodeFilterExpr(in.Filter)
	if err != nil {
		return nil, queryOutput{}, fmt.Errorf("invalid filter: %w", err)
	}
	results, err := s.Search(ctx, kbID, vec, &kb.SearchOptions{Mode: mode, TopK: in.K, Filter: filter, QueryText: in.Query})
	if err != nil {
		return nil, queryOutput{}, err
	}
	return nil, queryOutput{KBID: kbID, SearchMode: modeName, Results: resultOutputs(results, mode)}, nil
}

func validateQueryInput(in queryInput) error {
	if strings.TrimSpace(in.Query) == "" {
		return fmt.Errorf("query is required")
	}
	if in.K <= 0 {
		return fmt.Errorf("k must be > 0")
	}
	if in.K > queryToolMaxK {
		return fmt.Errorf("k must be <= %d", queryToolMaxK)
	}
	return nil
}

func resultOutputs(results []kb.ExpandedResult, mode kb.SearchMode) []resultOut {
	out := make([]resultOut, 0, len(results))
	for _, result := range results {
		out = append(out, resultOutput(result, mode))
	}
	return out
}

func resultOutput(result kb.ExpandedResult, mode kb.SearchMode) resultOut {
	item := resultOut{ID: result.ID, Content: result.Content, Distance: result.Distance}
	if mode != kb.SearchModeVector {
		score := result.Score
		graphScore := result.GraphScore
		item.Score = &score
		item.GraphScore = &graphScore
	}
	if len(result.MediaRefs) > 0 {
		item.MediaRefs = result.MediaRefs
	}
	return item
}

type ingestDocInput struct {
	ID        string             `json:"id"                   jsonschema:"Document ID."`
	Text      string             `json:"text"                 jsonschema:"Document text."`
	MediaIDs  []string           `json:"media_ids,omitempty"`
	MediaRefs []kb.ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata  map[string]any     `json:"metadata,omitempty"`
}

type ingestInput struct {
	KBID           string           `json:"kb_id,omitempty"`
	Documents      []ingestDocInput `json:"documents"`
	ChunkSize      int              `json:"chunk_size,omitempty"`
	GraphEnabled   bool             `json:"graph_enabled"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
	CorrelationID  string           `json:"correlation_id,omitempty"`
}

type ingestSyncInput struct {
	KBID           string           `json:"kb_id,omitempty"`
	Documents      []ingestDocInput `json:"documents"`
	ChunkSize      int              `json:"chunk_size,omitempty"`
	GraphEnabled   bool             `json:"graph_enabled"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
	CorrelationID  string           `json:"correlation_id,omitempty"`
	TimeoutMS      int64            `json:"timeout_ms,omitempty"`
}

type codeIndexInput struct {
	KBID             string `json:"kb_id,omitempty"`
	IndexKey         string `json:"index_key,omitempty"`
	Description      string `json:"description,omitempty"`
	Root             string `json:"root,omitempty"`
	IncludeUntracked bool   `json:"include_untracked,omitempty"`
}

func (s *Service) indexCodebase(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in codeIndexInput,
) (*mcp.CallToolResult, kb.CodeIndexResult, error) {
	if s.Config.ReadOnly || !s.Config.AllowIndexing {
		return nil, kb.CodeIndexResult{}, fmt.Errorf("indexing tools are disabled")
	}
	if s.IndexCodebase == nil {
		return nil, kb.CodeIndexResult{}, fmt.Errorf("code indexing is unavailable")
	}
	opts := kb.CodeIndexOptions{
		KBID:             strings.TrimSpace(in.KBID),
		IndexKey:         in.IndexKey,
		Description:      in.Description,
		Root:             strings.TrimSpace(in.Root),
		Include:          append([]string(nil), s.Config.CodeIndex.Include...),
		Exclude:          append([]string(nil), s.Config.CodeIndex.Exclude...),
		MaxFileBytes:     s.Config.CodeIndex.MaxFileBytes,
		ChunkSize:        s.Config.CodeIndex.ChunkSize,
		ChunkOverlap:     s.Config.CodeIndex.ChunkOverlap,
		IncludeUntracked: s.Config.CodeIndex.IncludeUntracked || in.IncludeUntracked,
		EmbedBatchSize:   s.Config.CodeIndex.ResourcePolicy.EmbedBatchSize,
		MaxBatchBytes:    s.Config.CodeIndex.ResourcePolicy.MaxBatchBytes,
		Throttle:         s.Config.CodeIndex.ResourcePolicy.Throttle,
		MaxHeapBytes:     s.Config.CodeIndex.ResourcePolicy.MaxHeapBytes,
		MaxRSSBytes:      s.Config.CodeIndex.ResourcePolicy.MaxRSSBytes,
		LargeRepoFiles:   s.Config.CodeIndex.ResourcePolicy.LargeRepoFiles,
		RequireConfirm:   s.Config.CodeIndex.RequireConfirm,
	}
	result, err := s.IndexCodebase(ctx, opts)
	return nil, result, err
}

type codeIndexStatusInput struct {
	KBID     string `json:"kb_id,omitempty"`
	IndexKey string `json:"index_key,omitempty"`
	Root     string `json:"root,omitempty"`
}

func (s *Service) codeIndexStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in codeIndexStatusInput,
) (*mcp.CallToolResult, kb.CodeIndexStatus, error) {
	if s.CodeIndexStatus == nil {
		return nil, kb.CodeIndexStatus{}, fmt.Errorf("code indexing is unavailable")
	}
	selection, err := kb.ResolveCodeIndexSelection(in.Root, in.IndexKey, strings.TrimSpace(in.KBID))
	if err != nil {
		return nil, kb.CodeIndexStatus{}, err
	}
	status, err := s.CodeIndexStatus(ctx, selection.KBID)
	status.IndexKey = selection.IndexKey
	status.Description = selection.Description
	return nil, status, err
}

type codeSearchInput struct {
	KBID     string `json:"kb_id,omitempty"`
	IndexKey string `json:"index_key,omitempty"`
	Root     string `json:"root,omitempty"`
	Query    string `json:"query"`
	K        int    `json:"k,omitempty"`
	Path     string `json:"path,omitempty"`
	Language string `json:"language,omitempty"`
}

type codeSearchOutput struct {
	KBID    string                `json:"kb_id"`
	Results []kb.CodeSearchResult `json:"results"`
}

func (s *Service) codeSearch(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in codeSearchInput,
) (*mcp.CallToolResult, codeSearchOutput, error) {
	if s.SearchCode == nil {
		return nil, codeSearchOutput{}, fmt.Errorf("code search is unavailable")
	}
	selection, err := kb.ResolveCodeIndexSelection(in.Root, in.IndexKey, strings.TrimSpace(in.KBID))
	if err != nil {
		return nil, codeSearchOutput{}, err
	}
	kbID := selection.KBID
	k := in.K
	if k <= 0 {
		k = 10
	}
	if k > queryToolMaxK {
		return nil, codeSearchOutput{}, fmt.Errorf("k must be <= %d", queryToolMaxK)
	}
	results, err := s.SearchCode(
		ctx,
		kbID,
		in.Query,
		kb.CodeSearchOptions{TopK: k, Path: in.Path, Language: in.Language},
	)
	return nil, codeSearchOutput{KBID: kbID, Results: results}, err
}

type codeHooksInput struct {
	Root     string `json:"root,omitempty"`
	KBID     string `json:"kb_id,omitempty"`
	IndexKey string `json:"index_key,omitempty"`
	Binary   string `json:"binary,omitempty"`
	Force    bool   `json:"force,omitempty"`
}

func (s *Service) installCodeHooks(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in codeHooksInput,
) (*mcp.CallToolResult, kb.CodeHookStatus, error) {
	if !s.Config.AllowAdmin || s.Config.ReadOnly {
		return nil, kb.CodeHookStatus{}, fmt.Errorf("admin tools are disabled")
	}
	if s.InstallCodeHooks == nil {
		return nil, kb.CodeHookStatus{}, fmt.Errorf("code hooks are unavailable")
	}
	selection, err := kb.ResolveCodeIndexSelection(in.Root, in.IndexKey, strings.TrimSpace(in.KBID))
	if err != nil {
		return nil, kb.CodeHookStatus{}, err
	}
	status, err := s.InstallCodeHooks(
		ctx,
		kb.CodeHookOptions{
			Root:     in.Root,
			KBID:     selection.KBID,
			IndexKey: selection.IndexKey,
			Binary:   in.Binary,
			Force:    in.Force,
		},
	)
	return nil, status, err
}

func (s *Service) uninstallCodeHooks(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in codeHooksInput,
) (*mcp.CallToolResult, kb.CodeHookStatus, error) {
	if !s.Config.AllowAdmin || s.Config.ReadOnly {
		return nil, kb.CodeHookStatus{}, fmt.Errorf("admin tools are disabled")
	}
	if s.UninstallCodeHooks == nil {
		return nil, kb.CodeHookStatus{}, fmt.Errorf("code hooks are unavailable")
	}
	status, err := s.UninstallCodeHooks(ctx, in.Root)
	return nil, status, err
}

func (s *Service) codeHooksStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in codeHooksInput,
) (*mcp.CallToolResult, kb.CodeHookStatus, error) {
	if !s.Config.AllowAdmin {
		return nil, kb.CodeHookStatus{}, fmt.Errorf("admin tools are disabled")
	}
	if s.CodeHookStatus == nil {
		return nil, kb.CodeHookStatus{}, fmt.Errorf("code hooks are unavailable")
	}
	status, err := s.CodeHookStatus(ctx, in.Root)
	return nil, status, err
}

type ingestOutput struct {
	EventID        string           `json:"event_id"`
	Status         string           `json:"status"`
	KBID           string           `json:"kb_id"`
	DocumentCount  int              `json:"document_count"`
	DocIDs         []string         `json:"doc_ids"`
	IdempotencyKey string           `json:"idempotency_key"`
	TimedOut       bool             `json:"timed_out,omitempty"`
	Terminal       map[string]any   `json:"terminal,omitempty"`
	Stages         []map[string]any `json:"stages,omitempty"`
}

func (s *Service) ingestAsync(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in ingestInput,
) (*mcp.CallToolResult, ingestOutput, error) {
	out, err := s.submitIngest(
		ctx,
		ingestSubmission{
			kbID:           in.KBID,
			documents:      in.Documents,
			chunkSize:      in.ChunkSize,
			graphEnabled:   in.GraphEnabled,
			idempotencyKey: in.IdempotencyKey,
			correlationID:  in.CorrelationID,
		},
	)
	return nil, out, err
}

func (s *Service) ingestSync(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in ingestSyncInput,
) (*mcp.CallToolResult, ingestOutput, error) {
	if !s.Config.AllowSyncIndexing {
		return nil, ingestOutput{}, fmt.Errorf("sync indexing tools are disabled")
	}
	out, err := s.submitIngest(
		ctx,
		ingestSubmission{
			kbID:           in.KBID,
			documents:      in.Documents,
			chunkSize:      in.ChunkSize,
			graphEnabled:   in.GraphEnabled,
			idempotencyKey: in.IdempotencyKey,
			correlationID:  in.CorrelationID,
		},
	)
	if err != nil {
		return nil, ingestOutput{}, err
	}
	timeout := s.syncIngestTimeout(in.TimeoutMS)
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(syncIngestPollInterval)
	defer ticker.Stop()
	for {
		completed, statusErr := s.refreshSyncIngestStatus(ctx, &out)
		if statusErr != nil {
			return nil, ingestOutput{}, statusErr
		}
		if completed {
			return nil, out, nil
		}
		if time.Now().After(deadline) {
			out.TimedOut = true
			out.Status = "timeout"
			return nil, out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ingestOutput{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

const syncIngestPollInterval = 100 * time.Millisecond

func (s *Service) syncIngestTimeout(timeoutMS int64) time.Duration {
	timeout := s.Config.DefaultSyncTimeout
	if timeoutMS > 0 {
		timeout = time.Duration(timeoutMS) * time.Millisecond
	}
	if timeout > s.Config.MaxSyncTimeout {
		return s.Config.MaxSyncTimeout
	}
	return timeout
}

func (s *Service) refreshSyncIngestStatus(ctx context.Context, out *ingestOutput) (bool, error) {
	status, err := s.operationStatusOutput(ctx, out.EventID)
	if err != nil {
		if errors.Is(err, kb.ErrEventNotFound) {
			return false, nil
		}
		return false, err
	}
	out.Stages = status.Stages
	out.Terminal = status.Terminal
	if status.Terminal == nil {
		return false, nil
	}
	out.Status = "completed"
	return true, nil
}

type ingestSubmission struct {
	kbID           string
	documents      []ingestDocInput
	chunkSize      int
	graphEnabled   bool
	idempotencyKey string
	correlationID  string
}

func (s *Service) submitIngest(ctx context.Context, submission ingestSubmission) (ingestOutput, error) {
	if s.Config.ReadOnly || !s.Config.AllowIndexing {
		return ingestOutput{}, fmt.Errorf("indexing tools are disabled")
	}
	if s.AppendDocumentUpsert == nil {
		return ingestOutput{}, fmt.Errorf("ingest event pipeline not configured")
	}
	kbID := defaultKBID(submission.kbID)
	docs, docIDs, err := ingestDocumentsFromInput(submission.documents)
	if err != nil {
		return ingestOutput{}, err
	}
	graph := submission.graphEnabled
	evtID, effectiveIdem, err := s.AppendDocumentUpsert(ctx, kb.DocumentUpsertPayload{
		KBID: kbID, Documents: docs, ChunkSize: submission.chunkSize, Options: kb.UpsertDocsOptions{GraphEnabled: &graph},
	}, strings.TrimSpace(submission.idempotencyKey), strings.TrimSpace(submission.correlationID))
	if err != nil {
		return ingestOutput{}, err
	}
	return ingestOutput{
		EventID:        evtID,
		Status:         "accepted",
		KBID:           kbID,
		DocumentCount:  len(docs),
		DocIDs:         docIDs,
		IdempotencyKey: effectiveIdem,
	}, nil
}

type operationStatusInput struct {
	EventID string `json:"event_id"`
}
type operationStatusOutput struct {
	EventID  string           `json:"event_id"`
	Root     map[string]any   `json:"root,omitempty"`
	Terminal map[string]any   `json:"terminal,omitempty"`
	Stages   []map[string]any `json:"stages,omitempty"`
}

func (s *Service) operationStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in operationStatusInput,
) (*mcp.CallToolResult, operationStatusOutput, error) {
	out, err := s.operationStatusOutput(ctx, strings.TrimSpace(in.EventID))
	return nil, out, err
}

func ingestDocumentsFromInput(docsIn []ingestDocInput) ([]kb.Document, []string, error) {
	if len(docsIn) == 0 {
		return nil, nil, fmt.Errorf("documents are required")
	}
	docs := make([]kb.Document, 0, len(docsIn))
	docIDs := make([]string, 0, len(docsIn))
	for _, input := range docsIn {
		doc, err := ingestDocumentFromInput(input)
		if err != nil {
			return nil, nil, err
		}
		docs = append(docs, doc)
		docIDs = append(docIDs, doc.ID)
	}
	return docs, docIDs, nil
}

func ingestDocumentFromInput(input ingestDocInput) (kb.Document, error) {
	id := strings.TrimSpace(input.ID)
	text := strings.TrimSpace(input.Text)
	if id == "" {
		return kb.Document{}, fmt.Errorf("document id is required")
	}
	if text == "" {
		return kb.Document{}, fmt.Errorf("document text is required")
	}
	return kb.Document{
		ID:        id,
		Text:      text,
		MediaIDs:  input.MediaIDs,
		MediaRefs: input.MediaRefs,
		Metadata:  input.Metadata,
	}, nil
}

func (s *Service) operationStatusOutput(ctx context.Context, eventID string) (operationStatusOutput, error) {
	if eventID == "" {
		return operationStatusOutput{}, fmt.Errorf("event_id required")
	}
	if s.GetEvent == nil {
		return operationStatusOutput{}, fmt.Errorf("event subsystem not configured")
	}
	ev, err := s.GetEvent(ctx, eventID)
	if err != nil {
		return operationStatusOutput{}, err
	}
	out := operationStatusOutput{EventID: eventID, Root: eventPayload(ev)}
	out.Terminal = s.operationTerminalPayload(ctx, eventID)
	out.Stages = s.operationStagesPayload(ctx, eventID)
	return out, nil
}

func (s *Service) operationTerminalPayload(ctx context.Context, eventID string) map[string]any {
	if s.FindOperationTerminal == nil {
		return nil
	}
	terminal, err := s.FindOperationTerminal(ctx, eventID)
	if err != nil && !errors.Is(err, kb.ErrEventNotFound) {
		s.logWarn(ctx, "operation_status: terminal lookup failed", "event_id", eventID, "error", err)
		return nil
	}
	return eventPayload(terminal)
}

func (s *Service) operationStagesPayload(ctx context.Context, eventID string) []map[string]any {
	if s.OperationStages == nil {
		return nil
	}
	stages, err := s.OperationStages(ctx, eventID)
	if err != nil {
		s.logWarn(ctx, "operation_status: stages lookup failed", "event_id", eventID, "error", err)
		return nil
	}
	out := make([]map[string]any, 0, len(stages))
	for _, stage := range stages {
		out = append(out, operationStagePayload(stage))
	}
	return out
}

func operationStagePayload(stage kb.OperationStageSnapshot) map[string]any {
	item := eventPayload(stage.Event)
	if item == nil {
		item = map[string]any{}
	}
	if stage.Failure != nil {
		item["failure"] = eventPayload(stage.Failure)
	}
	return item
}

type listMediaInput struct {
	KBID   string `json:"kb_id"`
	Prefix string `json:"prefix,omitempty"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
type listMediaOutput struct {
	Items []kb.MediaObject `json:"items"`
	Next  string           `json:"next,omitempty"`
	Limit int              `json:"limit"`
}

func (s *Service) listMedia(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in listMediaInput,
) (*mcp.CallToolResult, listMediaOutput, error) {
	if s.ListMedia == nil {
		return nil, listMediaOutput{}, fmt.Errorf("media subsystem not configured")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 500
	}
	page, err := s.ListMedia(ctx, defaultKBID(in.KBID), in.Prefix, in.After, limit)
	return nil, listMediaOutput{Items: page.Items, Next: page.NextToken, Limit: limit}, err
}

type mediaIDInput struct {
	MediaID string `json:"media_id"`
}
type mediaOutput struct {
	Media   *kb.MediaObject `json:"media,omitempty"`
	Deleted bool            `json:"deleted,omitempty"`
}

func (s *Service) getMedia(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in mediaIDInput,
) (*mcp.CallToolResult, mediaOutput, error) {
	if s.GetMedia == nil {
		return nil, mediaOutput{}, fmt.Errorf("media subsystem not configured")
	}
	media, err := s.GetMedia(ctx, strings.TrimSpace(in.MediaID))
	return nil, mediaOutput{Media: media}, err
}

func (s *Service) deleteMedia(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in mediaIDInput,
) (*mcp.CallToolResult, mediaOutput, error) {
	if !s.Config.AllowDestructive || s.Config.ReadOnly {
		return nil, mediaOutput{}, fmt.Errorf("destructive tools are disabled")
	}
	if s.DeleteMedia == nil {
		return nil, mediaOutput{}, fmt.Errorf("media subsystem not configured")
	}
	if err := s.DeleteMedia(ctx, strings.TrimSpace(in.MediaID)); err != nil {
		return nil, mediaOutput{}, err
	}
	return nil, mediaOutput{Deleted: true}, nil
}

type emptyInput struct{}
type statusOutput struct {
	Status string `json:"status"`
}

func (s *Service) sweepCache(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ emptyInput,
) (*mcp.CallToolResult, statusOutput, error) {
	if !s.Config.AllowAdmin {
		return nil, statusOutput{}, fmt.Errorf("admin tools are disabled")
	}
	if s.SweepCache == nil {
		return nil, statusOutput{}, fmt.Errorf(errKBUnavailable)
	}
	return nil, statusOutput{Status: "ok"}, s.SweepCache(ctx)
}

func (s *Service) clearCache(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ emptyInput,
) (*mcp.CallToolResult, statusOutput, error) {
	if !s.Config.AllowAdmin || !s.Config.AllowDestructive || s.Config.ReadOnly {
		return nil, statusOutput{}, fmt.Errorf("admin destructive tools are disabled")
	}
	if s.ClearCache == nil {
		return nil, statusOutput{}, fmt.Errorf(errKBUnavailable)
	}
	return nil, statusOutput{Status: "ok"}, s.ClearCache(ctx)
}

type kbIDInput struct {
	KBID string `json:"kb_id"`
}
type compactionOutput struct {
	Result *kb.CompactionPublishResult `json:"result,omitempty"`
}

func (s *Service) forceCompaction(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in kbIDInput,
) (*mcp.CallToolResult, compactionOutput, error) {
	if !s.Config.AllowAdmin {
		return nil, compactionOutput{}, fmt.Errorf("admin tools are disabled")
	}
	if s.ForceCompaction == nil {
		return nil, compactionOutput{}, fmt.Errorf("compaction unavailable")
	}
	kbID := strings.TrimSpace(in.KBID)
	if kbID == "" {
		return nil, compactionOutput{}, fmt.Errorf("kb_id required")
	}
	result, err := s.ForceCompaction(ctx, kbID)
	return nil, compactionOutput{Result: result}, err
}

type deleteKBOutput struct {
	Deleted bool   `json:"deleted"`
	KBID    string `json:"kb_id"`
}

func (s *Service) deleteKnowledgeBase(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in kbIDInput,
) (*mcp.CallToolResult, deleteKBOutput, error) {
	if !s.Config.AllowDestructive || s.Config.ReadOnly {
		return nil, deleteKBOutput{}, fmt.Errorf("destructive tools are disabled")
	}
	if s.DeleteKnowledgeBase == nil {
		return nil, deleteKBOutput{}, fmt.Errorf("delete knowledge base unavailable")
	}
	kbID := strings.TrimSpace(in.KBID)
	if kbID == "" {
		return nil, deleteKBOutput{}, fmt.Errorf("kb_id required")
	}
	if err := s.DeleteKnowledgeBase(ctx, kbID); err != nil {
		return nil, deleteKBOutput{}, err
	}
	return nil, deleteKBOutput{Deleted: true, KBID: kbID}, nil
}

func parseSearchMode(raw string) (kb.SearchMode, string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
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

func defaultKBID(raw string) string {
	if s := strings.TrimSpace(raw); s != "" {
		return s
	}
	return "default"
}

type upsertVectorsInput struct {
	KBID    string `json:"kb_id,omitempty" jsonschema:"Knowledge base ID. Defaults to default."`
	Vectors []struct {
		ID       string         `json:"id"`
		Vector   []float32      `json:"vector"`
		Metadata map[string]any `json:"metadata,omitempty"`
	} `json:"vectors" jsonschema:"List of vectors to upsert."`
}

type upsertVectorsOutput struct {
	KBID        string `json:"kb_id"`
	OperationID string `json:"operation_id"`
}

func (s *Service) upsertVectors(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in upsertVectorsInput,
) (*mcp.CallToolResult, upsertVectorsOutput, error) {
	kbID := defaultKBID(in.KBID)
	if len(in.Vectors) == 0 {
		return nil, upsertVectorsOutput{}, fmt.Errorf("vectors must not be empty")
	}
	if s.AppendDocumentUpsert == nil {
		return nil, upsertVectorsOutput{}, fmt.Errorf(errKBUnavailable)
	}
	docs := make([]kb.Document, 0, len(in.Vectors))
	for _, v := range in.Vectors {
		if strings.TrimSpace(v.ID) == "" {
			return nil, upsertVectorsOutput{}, fmt.Errorf("each vector must have an id")
		}
		if len(v.Vector) == 0 {
			return nil, upsertVectorsOutput{}, fmt.Errorf("vector for id %q is empty", v.ID)
		}
		docs = append(docs, kb.Document{ID: v.ID, Embedding: v.Vector, Metadata: v.Metadata})
	}
	opID, _, err := s.AppendDocumentUpsert(ctx, kb.DocumentUpsertPayload{KBID: kbID, Documents: docs}, "", "")
	if err != nil {
		return nil, upsertVectorsOutput{}, err
	}
	return nil, upsertVectorsOutput{KBID: kbID, OperationID: opID}, nil
}

type queryVectorsInput struct {
	KBID   string          `json:"kb_id,omitempty"  jsonschema:"Knowledge base ID. Defaults to default."`
	Vector []float32       `json:"vector"           jsonschema:"Query vector."`
	K      int             `json:"k"                jsonschema:"Number of results to return."`
	Filter json.RawMessage `json:"filter,omitempty" jsonschema:"Optional metadata predicate filter."`
}

type queryVectorsOutput struct {
	KBID    string              `json:"kb_id"`
	Results []vectorResultOut   `json:"results"`
}

type vectorResultOut struct {
	ID       string         `json:"id"`
	Distance float64        `json:"distance"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func (s *Service) queryVectors(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in queryVectorsInput,
) (*mcp.CallToolResult, queryVectorsOutput, error) {
	kbID := defaultKBID(in.KBID)
	if len(in.Vector) == 0 {
		return nil, queryVectorsOutput{}, fmt.Errorf("vector is required")
	}
	if in.K <= 0 {
		return nil, queryVectorsOutput{}, fmt.Errorf("k must be > 0")
	}
	if in.K > queryToolMaxK {
		return nil, queryVectorsOutput{}, fmt.Errorf("k must be <= %d", queryToolMaxK)
	}
	filter, err := decodeFilterExpr(in.Filter)
	if err != nil {
		return nil, queryVectorsOutput{}, fmt.Errorf("invalid filter: %w", err)
	}
	if s.QueryVectors == nil {
		return nil, queryVectorsOutput{}, fmt.Errorf(errKBUnavailable)
	}
	results, err := s.QueryVectors(ctx, kbID, in.Vector, in.K, filter)
	if err != nil {
		return nil, queryVectorsOutput{}, err
	}
	out := make([]vectorResultOut, 0, len(results))
	for _, r := range results {
		out = append(out, vectorResultOut{ID: r.ID, Distance: r.Distance, Metadata: r.Metadata})
	}
	return nil, queryVectorsOutput{KBID: kbID, Results: out}, nil
}

type fetchVectorsInput struct {
	KBID string   `json:"kb_id,omitempty" jsonschema:"Knowledge base ID. Defaults to default."`
	IDs  []string `json:"ids"             jsonschema:"List of vector IDs to fetch."`
}

type fetchVectorsOutput struct {
	KBID    string           `json:"kb_id"`
	Records []kb.VectorRecord `json:"records"`
}

func (s *Service) fetchVectors(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in fetchVectorsInput,
) (*mcp.CallToolResult, fetchVectorsOutput, error) {
	kbID := defaultKBID(in.KBID)
	if len(in.IDs) == 0 {
		return nil, fetchVectorsOutput{}, fmt.Errorf("ids must not be empty")
	}
	if s.FetchVectors == nil {
		return nil, fetchVectorsOutput{}, fmt.Errorf(errKBUnavailable)
	}
	records, err := s.FetchVectors(ctx, kbID, in.IDs)
	if err != nil {
		return nil, fetchVectorsOutput{}, err
	}
	return nil, fetchVectorsOutput{KBID: kbID, Records: records}, nil
}

func decodeFilterExpr(raw json.RawMessage) (*search.FilterExpr, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var f search.FilterExpr
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, err
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

// logWarn emits a warn-level log via the configured slog.Logger when one is
// set, falling back to slog.Default(). Centralized so MCP handlers always have
// somewhere to surface non-fatal lookup failures instead of swallowing them.
func (s *Service) logWarn(ctx context.Context, msg string, attrs ...any) {
	if s == nil {
		return
	}
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.WarnContext(ctx, msg, attrs...)
}

func eventPayload(ev *kb.KBEvent) map[string]any {
	if ev == nil {
		return nil
	}
	out := map[string]any{
		"event_id":       ev.EventID,
		"kb_id":          ev.KBID,
		"kind":           ev.Kind,
		"status":         ev.Status,
		"attempt":        ev.Attempt,
		"correlation_id": ev.CorrelationID,
		"causation_id":   ev.CausationID,
		"created_at":     ev.CreatedAt,
		"last_error":     ev.LastError,
	}
	switch ev.Kind {
	case kb.EventKBPublished:
		var payload kb.KBPublishedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["document_count"] = payload.DocumentCount
			out["chunk_count"] = payload.ChunkCount
			out["media_ids"] = payload.MediaIDs
			out["file_results"] = payload.FileResults
		}
	case kb.EventMediaUploaded:
		var payload kb.MediaUploadedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["media_id"] = payload.MediaID
			out["filename"] = payload.Filename
		}
	case kb.EventWorkerFailed:
		var payload kb.WorkerFailedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["stage"] = payload.Stage
			out["will_retry"] = payload.WillRetry
			out["file_results"] = payload.FileResults
		}
	}
	return out
}

func isNotFound(err error) bool {
	return errors.Is(err, kb.ErrEventNotFound) || errors.Is(err, kb.ErrMediaNotFound)
}
