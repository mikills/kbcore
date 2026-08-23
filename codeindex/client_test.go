package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	indexer "github.com/mikills/minnow/kb/codeindex"
)

func TestMinnowClientRetriesHealthAndDeleteUsingRetryAfter(t *testing.T) {
	fixedNow := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	var mu sync.Mutex
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.Method+" "+r.URL.Path]++
		attempt := requests[r.Method+" "+r.URL.Path]
		mu.Unlock()
		if attempt == 1 {
			switch r.URL.Path {
			case "/healthz":
				w.Header().Set("Retry-After", fixedNow.Add(2*time.Second).Format(http.TimeFormat))
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			case "/v1/vectors":
				w.Header().Set("Retry-After", "3")
				http.Error(w, "slow down", http.StatusTooManyRequests)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()

	client := newTestMinnowClient(t, server.URL)
	client.now = func() time.Time { return fixedNow }
	var waits []time.Duration
	client.wait = func(_ context.Context, duration time.Duration) error {
		waits = append(waits, duration)
		return nil
	}
	if err := client.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.delete(context.Background(), "kb", []string{"stale"}); err != nil {
		t.Fatal(err)
	}
	if len(waits) != 2 || waits[0] != 2*time.Second || waits[1] != 3*time.Second {
		t.Fatalf("Retry-After was not respected: %v", waits)
	}
	if requests["GET /healthz"] != 2 || requests["DELETE /v1/vectors"] != 2 {
		t.Fatalf("expected one retry per endpoint, got %+v", requests)
	}
}

func TestIngestRetriesLostAcceptedResponseWithStableIdempotencyKey(t *testing.T) {
	client := newTestMinnowClient(t, "http://minnow.test")
	client.wait = func(context.Context, time.Duration) error { return nil }
	var postKeys, postBodies []string
	postAttempts := 0
	client.http = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/rag/ingest":
			postAttempts++
			postKeys = append(postKeys, req.Header.Get("Idempotency-Key"))
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			postBodies = append(postBodies, string(body))
			if postAttempts == 1 {
				return response(req, http.StatusAccepted, errorReader{}), nil
			}
			return response(req, http.StatusAccepted, strings.NewReader(`{"event_id":"evt"}`)), nil
		case req.Method == http.MethodGet && req.URL.Path == "/rag/operations/evt":
			return response(req, http.StatusOK, strings.NewReader(`{"terminal":{"kind":"kb.published","status":"done"}}`)), nil
		default:
			return nil, errors.New("unexpected request: " + req.Method + " " + req.URL.Path)
		}
	})}

	docs := []indexer.Document{{ID: "chunk-id", Text: "package example"}}
	if err := client.ingest(context.Background(), "kb", docs); err != nil {
		t.Fatal(err)
	}
	if len(postKeys) != 2 || postKeys[0] == "" || postKeys[0] != postKeys[1] {
		t.Fatalf("ingest retries did not use one stable idempotency key: %q", postKeys)
	}
	if len(postBodies) != 2 || postBodies[0] != postBodies[1] {
		t.Fatal("ingest retry changed the request body")
	}
}

func TestFinalWorkerFailureDoesNotPollBecauseEventIsPending(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = io.WriteString(w, `{"terminal":{"kind":"worker.failed","status":"pending","stage":"embed","will_retry":false}}`)
	}))
	defer server.Close()
	client := newTestMinnowClient(t, server.URL)
	if err := client.waitForOperation(context.Background(), "evt"); err == nil {
		t.Fatal("expected final worker failure")
	}
	if requests != 1 {
		t.Fatalf("final worker failure was polled %d times", requests)
	}
}

func TestParseRetryAfterAcceptsZeroAndRejectsInvalidValues(t *testing.T) {
	now := time.Now()
	if delay, ok := parseRetryAfter("0", now); !ok || delay != 0 {
		t.Fatalf("delta-seconds zero parsed as (%s, %v)", delay, ok)
	}
	if _, ok := parseRetryAfter("not-a-date", now); ok {
		t.Fatal("invalid Retry-After was accepted")
	}
}

func newTestMinnowClient(t *testing.T, baseURL string) *minnowClient {
	t.Helper()
	cfg := defaultConfig()
	cfg.Minnow.URL = baseURL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	client, err := newMinnowClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func response(req *http.Request, status int, body io.Reader) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(body),
		Request:    req,
	}
}

func TestIdempotencyKeysAreScopedToTheRun(t *testing.T) {
	docs := []indexer.Document{{ID: "code-a-1", Text: "package main"}}
	first := newTestMinnowClient(t, "http://example.invalid")
	second := newTestMinnowClient(t, "http://example.invalid")

	if first.idempotencyKey("kb", docs) != first.idempotencyKey("kb", docs) {
		t.Fatal("a retry inside one run changed its key, so the server would queue the batch twice")
	}
	// A rerun after a failure replays the same batches. Sharing keys with the
	// failed run makes the server answer with its finished operation and queue
	// nothing, while the client records every file as indexed.
	if first.idempotencyKey("kb", docs) == second.idempotencyKey("kb", docs) {
		t.Fatal("a rerun replayed the previous run's idempotency key")
	}
}
