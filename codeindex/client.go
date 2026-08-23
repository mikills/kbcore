package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	minnowcode "github.com/mikills/minnow/kb/codeindex"
)

type minnowClient struct {
	baseURL         *url.URL
	token           string
	http            *http.Client
	pollEvery       time.Duration
	operationTTL    time.Duration
	now             func() time.Time
	wait            func(context.Context, time.Duration) error
	sessionID       string
	sessionKB       string
	runID           string
	canDeferPublish bool
	canScope        bool
	onSession       func(string) error
	onWait          func(time.Duration)
	conflictBudget  time.Duration
	scopeRevision   string
	scopeIDs        []string
	scopeExists     bool
}

type jsonCall struct {
	client  *http.Client
	method  string
	path    string
	body    any
	idemKey string
	out     any
}

type retryDecision struct {
	retry bool
	after time.Duration
	// conflict clears on its own once the holding session lapses, so it is
	// waited out against a time budget rather than the attempt budget.
	conflict bool
}

type ingestRequest struct {
	KBID         string           `json:"kb_id"`
	GraphEnabled bool             `json:"graph_enabled"`
	PreChunked   bool             `json:"pre_chunked"`
	DeferPublish bool             `json:"defer_publish,omitempty"`
	GCUnscoped   bool             `json:"gc_unscoped,omitempty"`
	SessionID    string           `json:"session_id,omitempty"`
	Documents    []ingestDocument `json:"documents"`
}

type ingestDocument struct {
	ID       string         `json:"id"`
	Text     string         `json:"text"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type acceptedOperation struct {
	EventID   string `json:"event_id"`
	SessionID string `json:"session_id"`
}

type deferredAck struct {
	SessionID string `json:"session_id"`
}

type operationStatus struct {
	Terminal *operationTerminal `json:"terminal"`
}

type operationTerminal struct {
	Kind      string `json:"kind"`
	Status    string `json:"status"`
	LastError string `json:"last_error"`
	Stage     string `json:"stage"`
	WillRetry bool   `json:"will_retry"`
}

func newMinnowClient(cfg Config) (*minnowClient, error) {
	if err := validateMinnowURL(cfg.Minnow.URL); err != nil {
		return nil, err
	}
	base, err := url.Parse(strings.TrimRight(cfg.Minnow.URL, "/"))
	if err != nil {
		return nil, err
	}
	pollEvery, err := cfg.pollInterval()
	if err != nil {
		return nil, err
	}
	operationTTL, err := cfg.operationTimeout()
	if err != nil {
		return nil, err
	}
	return &minnowClient{
		baseURL: base, token: cfg.Minnow.Token, http: &http.Client{Timeout: 30 * time.Second},
		pollEvery: pollEvery, operationTTL: operationTTL, now: time.Now, wait: waitDuration,
		conflictBudget: sessionConflictBudget, runID: newRunID(),
	}, nil
}

func (c *minnowClient) check(ctx context.Context) error {
	var out struct {
		Capabilities []string `json:"capabilities"`
	}
	if err := c.doJSONWithRetry(ctx, http.MethodGet, "/healthz", nil, "", &out); err != nil {
		return err
	}
	c.canDeferPublish = slices.Contains(out.Capabilities, capabilityIngestSessions)
	c.canScope = slices.Contains(out.Capabilities, capabilityDocumentScopes)
	return nil
}

func (c *minnowClient) ingest(ctx context.Context, kbID string, docs []minnowcode.Document) error {
	wireDocs := make([]ingestDocument, 0, len(docs))
	for _, doc := range docs {
		wireDocs = append(wireDocs, ingestDocument{ID: doc.ID, Text: doc.Text, Metadata: flattenCodeMetadata(doc.Metadata)})
	}
	request := ingestRequest{
		KBID: kbID, GraphEnabled: false, PreChunked: true,
		DeferPublish: c.defers(kbID), GCUnscoped: c.canScope && c.defers(kbID),
		SessionID: c.sessionID, Documents: wireDocs,
	}
	var accepted acceptedOperation
	if err := c.doJSONWithRetry(ctx, http.MethodPost, "/rag/ingest", request, c.idempotencyKey(kbID, docs), &accepted); err != nil {
		return err
	}
	if accepted.EventID == "" {
		return fmt.Errorf("Minnow ingest response did not include event_id")
	}
	if err := c.adoptSession(kbID, accepted.SessionID); err != nil {
		return err
	}
	return c.waitForOperation(ctx, accepted.EventID)
}

// commit publishes everything the session deferred, as an operation to follow
// so no proxy read timeout sits between the run and its writes.
func (c *minnowClient) commit(ctx context.Context, kbID string) error {
	if c.sessionID == "" {
		return nil
	}
	body := map[string]any{"kb_id": kbID, "session_id": c.sessionID}
	// Keyed so a lost response retries onto the same operation.
	var accepted acceptedOperation
	if err := c.doJSON(ctx, jsonCall{
		client: c.http, method: http.MethodPost, path: "/rag/commit",
		body: body, idemKey: c.sessionID, out: &accepted,
	}); err != nil {
		return err
	}
	if accepted.EventID == "" {
		return fmt.Errorf("Minnow commit response did not include event_id")
	}
	// A publish rebuilds every shard, which outlasts the per-batch budget.
	return c.awaitOperation(ctx, accepted.EventID, commitTimeout)
}

func (c *minnowClient) delete(ctx context.Context, kbID string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	body := map[string]any{
		"kb_id": kbID, "ids": ids,
		"defer_publish": c.defers(kbID), "session_id": c.sessionID,
	}
	var ack deferredAck
	if err := c.doJSONWithRetry(ctx, http.MethodDelete, "/v1/vectors", body, "", &ack); err != nil {
		return err
	}
	return c.adoptSession(kbID, ack.SessionID)
}

func (c *minnowClient) replaceScope(ctx context.Context, kbID, scopeID string, ids []string) error {
	if !c.canScope {
		return fmt.Errorf("Minnow does not support document scopes")
	}
	body := map[string]any{
		"kb_id": kbID, "scope_id": scopeID, "document_ids": ids,
		"revision": c.scopeRevision,
	}
	var scope struct {
		Revision string `json:"revision"`
	}
	if err := c.doJSONWithRetry(ctx, http.MethodPut, "/v1/scopes", body, "", &scope); err != nil {
		if isHTTPConflict(err) {
			currentIDs, revision, exists, getErr := c.getScope(ctx, kbID, scopeID)
			if getErr == nil && exists && slices.Equal(currentIDs, ids) {
				c.scopeRevision = revision
				c.scopeIDs = currentIDs
				c.scopeExists = true
				return nil
			}
		}
		return err
	}
	c.scopeRevision = scope.Revision
	c.scopeIDs = append(c.scopeIDs[:0], ids...)
	c.scopeExists = true
	return nil
}

func (c *minnowClient) scheduleGC(ctx context.Context, kbID string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return []string{}, nil
	}
	body := map[string]any{"kb_id": kbID, "document_ids": ids}
	var out struct {
		ScheduledIDs []string `json:"scheduled_ids"`
	}
	key := "codeindex-scope-gc-" + shortHash(strings.Join(ids, "\x00"))
	if err := c.doJSONWithRetry(ctx, http.MethodPost, "/v1/scopes/gc", body, key, &out); err != nil {
		return nil, err
	}
	return out.ScheduledIDs, nil
}

func (c *minnowClient) deleteScope(ctx context.Context, kbID, scopeID, revision string) error {
	query := url.Values{"kb_id": []string{kbID}}
	if revision != "" {
		query.Set("revision", revision)
	}
	path := "/v1/scopes/" + url.PathEscape(scopeID) + "?" + query.Encode()
	return c.doJSONWithRetry(ctx, http.MethodDelete, path, nil, "", nil)
}

func (c *minnowClient) scopeMembers(ctx context.Context, kbID, scopeID string) (map[string]struct{}, error) {
	if !c.canScope {
		return map[string]struct{}{}, nil
	}
	path := "/v1/scopes/documents?kb_id=" + url.QueryEscape(kbID)
	var out struct {
		DocumentIDs []string `json:"document_ids"`
	}
	if err := c.doJSONWithRetry(ctx, http.MethodGet, path, nil, "", &out); err != nil {
		return nil, err
	}
	members := make(map[string]struct{}, len(out.DocumentIDs))
	for _, id := range out.DocumentIDs {
		members[id] = struct{}{}
	}
	currentIDs, revision, exists, err := c.getScope(ctx, kbID, scopeID)
	if err != nil {
		return nil, err
	}
	if exists {
		c.scopeRevision = revision
		c.scopeIDs = currentIDs
		c.scopeExists = true
	}
	return members, nil
}

func (c *minnowClient) getScope(ctx context.Context, kbID, scopeID string) ([]string, string, bool, error) {
	var current struct {
		DocumentIDs []string `json:"document_ids"`
		Revision    string   `json:"revision"`
	}
	path := "/v1/scopes/" + url.PathEscape(scopeID) + "?kb_id=" + url.QueryEscape(kbID)
	if err := c.doJSONWithRetry(ctx, http.MethodGet, path, nil, "", &current); err != nil {
		if isHTTPStatus(err, http.StatusNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	ids := append([]string(nil), current.DocumentIDs...)
	slices.Sort(ids)
	return ids, current.Revision, true, nil
}

func (c *minnowClient) published(ctx context.Context, kbID string, ids []string) (map[string]struct{}, error) {
	found := make(map[string]struct{})
	for len(ids) > 0 {
		end := min(len(ids), 200)
		body := map[string]any{"kb_id": kbID, "ids": ids[:end]}
		var out struct {
			Records []struct {
				ID string `json:"id"`
			} `json:"records"`
		}
		key := "codeindex-fetch-" + shortHash(strings.Join(ids[:end], "\x00"))
		if err := c.doJSONWithRetry(ctx, http.MethodPost, "/v1/vectors/fetch", body, key, &out); err != nil {
			return nil, err
		}
		for _, record := range out.Records {
			found[record.ID] = struct{}{}
		}
		ids = ids[end:]
	}
	return found, nil
}

// defers excludes knowledge bases from earlier runs, whose rows would strand
// under a session nothing commits.
func (c *minnowClient) defers(kbID string) bool {
	return c.canDeferPublish && kbID == c.sessionKB
}

// adoptSession records the handle the server issued, so a run that loses its
// own resumes under that one rather than opening a second.
func (c *minnowClient) adoptSession(kbID, id string) error {
	if id == "" || id == c.sessionID || kbID != c.sessionKB {
		return nil
	}
	if c.onSession != nil {
		if err := c.onSession(id); err != nil {
			return fmt.Errorf("record ingest session: %w", err)
		}
	}
	c.sessionID = id
	return nil
}

type minnowHTTPError struct {
	status int
	err    error
}

func (e *minnowHTTPError) Error() string { return e.err.Error() }
func (e *minnowHTTPError) Unwrap() error { return e.err }

func isHTTPConflict(err error) bool {
	return isHTTPStatus(err, http.StatusConflict)
}

func isHTTPStatus(err error, status int) bool {
	var response *minnowHTTPError
	return errors.As(err, &response) && response.status == status
}

func (c *minnowClient) waitForOperation(ctx context.Context, eventID string) error {
	return c.awaitOperation(ctx, eventID, c.operationTTL)
}

func (c *minnowClient) awaitOperation(ctx context.Context, eventID string, budget time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	ticker := time.NewTicker(c.pollEvery)
	defer ticker.Stop()
	for {
		var status operationStatus
		if err := c.pollOperation(waitCtx, eventID, &status); err != nil {
			return err
		}
		if status.Terminal != nil {
			if status.Terminal.Kind == "worker.failed" {
				if status.Terminal.WillRetry {
					if err := waitForPoll(waitCtx, ticker.C); err != nil {
						return fmt.Errorf("wait for Minnow operation %s: %w", eventID, err)
					}
					continue
				}
				message := firstNonEmpty(status.Terminal.LastError, status.Terminal.Stage, status.Terminal.Kind)
				return fmt.Errorf("Minnow operation failed: %s", message)
			}
			if status.Terminal.Kind == "kb.published" && status.Terminal.Status == "done" {
				return nil
			}
			if status.Terminal.Status == "pending" || status.Terminal.Status == "claimed" {
				if err := waitForPoll(waitCtx, ticker.C); err != nil {
					return fmt.Errorf("wait for Minnow operation %s: %w", eventID, err)
				}
				continue
			}
			message := firstNonEmpty(status.Terminal.LastError, status.Terminal.Stage, status.Terminal.Kind)
			return fmt.Errorf("Minnow operation failed: %s", message)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for Minnow operation %s: %w", eventID, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (c *minnowClient) doJSONWithRetry(ctx context.Context, method, path string, body any, idemKey string, out any) error {
	return c.doJSON(ctx, jsonCall{client: c.http, method: method, path: path, body: body, idemKey: idemKey, out: out})
}

func (c *minnowClient) doJSON(ctx context.Context, call jsonCall) error {
	attempt, waited := 0, time.Duration(0)
	for {
		decision, err := c.doJSONAttempt(ctx, call)
		if err == nil {
			return nil
		}
		switch {
		case decision.conflict:
			// A run refused by the orphan its own lost response left behind
			// has to outlast it. Failing costs the whole index.
			if waited+decision.after > c.conflictBudget {
				return err
			}
			waited += decision.after
			if c.onWait != nil {
				c.onWait(decision.after)
			}
		case decision.retry && attempt < 4:
			attempt++
		default:
			return err
		}
		if err := c.wait(ctx, decision.after); err != nil {
			return err
		}
	}
}

func (c *minnowClient) pollOperation(ctx context.Context, eventID string, status *operationStatus) error {
	path := "/rag/operations/" + url.PathEscape(eventID)
	return c.doJSONWithRetry(ctx, http.MethodGet, path, nil, "", status)
}

func parseRetryAfter(raw string, now time.Time) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	seconds, err := strconv.ParseUint(raw, 10, 31)
	if err == nil {
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(raw)
	if err != nil {
		return 0, false
	}
	if !when.After(now) {
		return 0, true
	}
	return when.Sub(now), true
}

func waitForPoll(ctx context.Context, tick <-chan time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tick:
		return nil
	}
}

func waitDuration(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *minnowClient) doJSONAttempt(ctx context.Context, call jsonCall) (retryDecision, error) {
	method, path, body, idemKey, out := call.method, call.path, call.body, call.idemKey, call.out
	canReplay := requestCanBeReplayed(method, idemKey)
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return retryDecision{}, err
		}
		reader = bytes.NewReader(data)
	}
	relative, err := url.Parse(path)
	if err != nil {
		return retryDecision{}, err
	}
	endpoint := c.baseURL.JoinPath(strings.TrimPrefix(relative.Path, "/"))
	endpoint.RawQuery = relative.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return retryDecision{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := call.client.Do(req)
	if err != nil {
		return retryDecision{retry: canReplay, after: c.pollEvery}, fmt.Errorf("%s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return retryDecision{retry: canReplay, after: c.pollEvery}, fmt.Errorf("read Minnow response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		decision := c.responseRetryDecision(resp.StatusCode, resp.Header.Get("Retry-After"), canReplay)
		failure := fmt.Errorf("Minnow %s %s: %s", method, path, responseError(data, resp.Status))
		return decision, &minnowHTTPError{status: resp.StatusCode, err: failure}
	}
	if out != nil {
		if err := decodeJSONResponse(data, out); err != nil {
			return retryDecision{retry: canReplay, after: c.pollEvery}, fmt.Errorf("decode Minnow response: %w", err)
		}
	}
	return retryDecision{}, nil
}

func (c *minnowClient) responseRetryDecision(status int, retryAfter string, canReplay bool) retryDecision {
	if !canReplay {
		return retryDecision{}
	}
	conflict := status == http.StatusConflict
	if !conflict && status != http.StatusTooManyRequests && (status < 500 || status > 599) {
		return retryDecision{}
	}
	after, ok := parseRetryAfter(retryAfter, c.now())
	if !ok {
		// Nothing to wait for, so the conflict is reported.
		if conflict {
			return retryDecision{}
		}
		after = c.pollEvery
	}
	if conflict {
		// Floored, or a lapsed deadline is polled flat out for the budget.
		return retryDecision{conflict: true, after: max(after, minConflictWait)}
	}
	// Capped, since Retry-After is whatever the far end says.
	return retryDecision{retry: true, after: min(after, maxRetryWait)}
}

func requestCanBeReplayed(method, idemKey string) bool {
	if idemKey != "" {
		return true
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func decodeJSONResponse(data []byte, out any) error {
	value := reflect.ValueOf(out)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("response target must be a non-nil pointer")
	}
	decoded := reflect.New(value.Elem().Type())
	if err := json.Unmarshal(data, decoded.Interface()); err != nil {
		return err
	}
	value.Elem().Set(decoded.Elem())
	return nil
}

func responseError(data []byte, fallback string) string {
	var failure struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(data, &failure)
	return firstNonEmpty(failure.Error, strings.TrimSpace(string(data)), fallback)
}

func flattenCodeMetadata(metadata map[string]any) map[string]any {
	out := make(map[string]any, len(metadata)+8)
	maps.Copy(out, metadata)
	code, ok := metadata["code"].(minnowcode.ChunkMetadata)
	if !ok {
		return out
	}
	out["code_path"] = code.Path
	out["code_language"] = code.Language
	out["code_symbol"] = code.Symbol
	out["code_kind"] = code.Kind
	out["code_start_line"] = code.StartLine
	out["code_end_line"] = code.EndLine
	out["code_file_hash"] = code.Hash
	return out
}

// newRunID scopes this run's idempotency keys. Keyed on content alone, a rerun
// replays keys the server already consumed and it queues nothing.
func newRunID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf[:])
}

func (c *minnowClient) idempotencyKey(kbID string, docs []minnowcode.Document) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(c.runID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(kbID))
	for _, doc := range docs {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(doc.ID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(doc.Text))
	}
	return "codeindex-" + hex.EncodeToString(hash.Sum(nil))
}

const (
	capabilityIngestSessions = "ingest_sessions"
	capabilityDocumentScopes = "document_scopes"
	commitTimeout            = 30 * time.Minute
	maxResponseBytes         = 64 << 20

	// Outlasts a server side session lease.
	sessionConflictBudget = 11 * time.Minute
	minConflictWait       = time.Second
	maxRetryWait          = time.Minute
)
