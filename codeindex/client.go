package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	minnowcode "github.com/mikills/minnow/codeindex/indexer"
)

type minnowClient struct {
	baseURL      *url.URL
	token        string
	http         *http.Client
	pollEvery    time.Duration
	operationTTL time.Duration
	now          func() time.Time
	wait         func(context.Context, time.Duration) error
}

type retryDecision struct {
	retry bool
	after time.Duration
}

type ingestRequest struct {
	KBID         string           `json:"kb_id"`
	GraphEnabled bool             `json:"graph_enabled"`
	PreChunked   bool             `json:"pre_chunked"`
	Documents    []ingestDocument `json:"documents"`
}

type ingestDocument struct {
	ID       string         `json:"id"`
	Text     string         `json:"text"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type acceptedOperation struct {
	EventID string `json:"event_id"`
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
	}, nil
}

func (c *minnowClient) check(ctx context.Context) error {
	var out map[string]any
	return c.doJSONWithRetry(ctx, http.MethodGet, "/healthz", nil, "", &out)
}

func (c *minnowClient) ingest(ctx context.Context, kbID string, docs []minnowcode.Document) error {
	wireDocs := make([]ingestDocument, 0, len(docs))
	for _, doc := range docs {
		wireDocs = append(wireDocs, ingestDocument{ID: doc.ID, Text: doc.Text, Metadata: flattenCodeMetadata(doc.Metadata)})
	}
	request := ingestRequest{KBID: kbID, GraphEnabled: false, PreChunked: true, Documents: wireDocs}
	var accepted acceptedOperation
	if err := c.doJSONWithRetry(ctx, http.MethodPost, "/rag/ingest", request, idempotencyKey(kbID, docs), &accepted); err != nil {
		return err
	}
	if accepted.EventID == "" {
		return fmt.Errorf("Minnow ingest response did not include event_id")
	}
	return c.waitForOperation(ctx, accepted.EventID)
}

func (c *minnowClient) delete(ctx context.Context, kbID string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return c.doJSONWithRetry(ctx, http.MethodDelete, "/v1/vectors", map[string]any{"kb_id": kbID, "ids": ids}, "", nil)
}

func (c *minnowClient) waitForOperation(ctx context.Context, eventID string) error {
	waitCtx, cancel := context.WithTimeout(ctx, c.operationTTL)
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
	for attempt := 0; ; attempt++ {
		decision, err := c.doJSONAttempt(ctx, method, path, body, idemKey, out)
		if err == nil {
			return nil
		}
		if attempt >= 4 || !decision.retry {
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

func (c *minnowClient) doJSONAttempt(
	ctx context.Context,
	method, path string,
	body any,
	idemKey string,
	out any,
) (retryDecision, error) {
	canReplay := requestCanBeReplayed(method, idemKey)
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return retryDecision{}, err
		}
		reader = bytes.NewReader(data)
	}
	endpoint := c.baseURL.JoinPath(strings.TrimPrefix(path, "/"))
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
	resp, err := c.http.Do(req)
	if err != nil {
		return retryDecision{retry: canReplay, after: c.pollEvery}, fmt.Errorf("%s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return retryDecision{retry: canReplay, after: c.pollEvery}, fmt.Errorf("read Minnow response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		decision := c.responseRetryDecision(resp.StatusCode, resp.Header.Get("Retry-After"), canReplay)
		return decision, fmt.Errorf("Minnow %s %s: %s", method, path, responseError(data, resp.Status))
	}
	if out != nil {
		if err := decodeJSONResponse(data, out); err != nil {
			return retryDecision{retry: canReplay, after: c.pollEvery}, fmt.Errorf("decode Minnow response: %w", err)
		}
	}
	return retryDecision{}, nil
}

func (c *minnowClient) responseRetryDecision(status int, retryAfter string, canReplay bool) retryDecision {
	if !canReplay || (status != http.StatusTooManyRequests && (status < 500 || status > 599)) {
		return retryDecision{}
	}
	after, ok := parseRetryAfter(retryAfter, c.now())
	if !ok {
		after = c.pollEvery
	}
	return retryDecision{retry: true, after: after}
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

func idempotencyKey(kbID string, docs []minnowcode.Document) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(kbID))
	for _, doc := range docs {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(doc.ID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(doc.Text))
	}
	return "codeindex-" + hex.EncodeToString(hash.Sum(nil))
}
