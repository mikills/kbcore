package openaiembedder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/internal/openaiembed"
)

var ErrInvalidEmbeddingDimension = errors.New("invalid embedding dimension")

type closeableHTTPResponse struct{ *http.Response }

func closeableHTTPDo(client *http.Client, req *http.Request) (*closeableHTTPResponse, error) {
	return newCloseableHTTPResponse(client.Do(req))
}

func newCloseableHTTPResponse(resp *http.Response, err error) (*closeableHTTPResponse, error) {
	if err != nil {
		return nil, err
	}
	return &closeableHTTPResponse{Response: resp}, nil
}

func (r *closeableHTTPResponse) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}

type closeableResponse struct{ *http.Response }

func (r *closeableResponse) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}

const (
	defaultOpenAICompatibleBaseURL = "https://api.openai.com/v1"
	// defaultOpenAICompatibleTimeout caps a single attempt, not a whole
	// EmbedBatch: retries and their backoff multiply it. Callers that plumb a
	// context deadline bound the chain; without one it is attempts x timeout.
	defaultOpenAICompatibleTimeout = 30 * time.Second
)

// OpenAICompatibleEmbedder requests embeddings from OpenAI-compatible
// /v1/embeddings APIs, including hosted OpenAI-compatible providers and
// Ollama's compatibility endpoint.
type OpenAICompatibleEmbedder struct {
	BaseURL    string
	Model      string
	Token      string
	Dimensions int
	HTTPClient *http.Client
	// TotalTimeout bounds one EmbedBatch across all its retries. Zero uses
	// embedTotalTimeout.
	TotalTimeout time.Duration
}

type OpenAICompatibleEmbedderConfig struct {
	BaseURL      string
	Model        string
	Token        string
	Dimensions   int
	TotalTimeout time.Duration
}

// NewOpenAICompatibleEmbedder creates an embedder for OpenAI-compatible APIs.
func NewOpenAICompatibleEmbedder(cfg OpenAICompatibleEmbedderConfig) (*OpenAICompatibleEmbedder, error) {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultOpenAICompatibleBaseURL
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, fmt.Errorf("openai compatible embedder model cannot be empty")
	}
	if cfg.Dimensions < 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidEmbeddingDimension, cfg.Dimensions)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("openai compatible embedder base_url must be a valid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf(
			"openai compatible embedder base_url scheme must be http or https (got %q)",
			parsed.Scheme,
		)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("openai compatible embedder base_url must include a host")
	}

	return &OpenAICompatibleEmbedder{
		BaseURL:      strings.TrimRight(baseURL, "/"),
		Model:        model,
		Token:        strings.TrimSpace(cfg.Token),
		Dimensions:   cfg.Dimensions,
		TotalTimeout: cfg.TotalTimeout,
		HTTPClient:   &http.Client{Timeout: defaultOpenAICompatibleTimeout, Transport: embedTransport()},
	}, nil
}

// Embed requests a single embedding for input.
func (e *OpenAICompatibleEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	vectors, err := e.EmbedBatch(ctx, []string{input})
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}

// EmbedBatch requests embeddings for multiple inputs in one provider call.
func (e *OpenAICompatibleEmbedder) EmbedBatch(ctx context.Context, inputs []string) (out [][]float32, err error) {
	cleanInputs, err := e.validateBatchInputs(inputs)
	if err != nil {
		return nil, err
	}
	requestBody, err := json.Marshal(e.embeddingRequestBody(cleanInputs))
	if err != nil {
		return nil, fmt.Errorf("marshal openai compatible embed request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(e.BaseURL, "/")+"/embeddings",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return nil, fmt.Errorf("create openai compatible embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(e.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	retryCtx, cancel := context.WithTimeout(ctx, e.totalTimeout())
	defer cancel()
	req = req.WithContext(retryCtx)
	parsed, err := e.sendWithRetry(retryCtx, req, requestBody, len(cleanInputs))
	if err != nil {
		return nil, err
	}
	return openaiembed.Vectors(parsed, len(cleanInputs))
}

// A provider answers a burst with 429 rather than queueing it, so concurrent
// batches need backoff to become throughput instead of failures.
const (
	embedMaxAttempts = 4
	embedRetryBase   = 500 * time.Millisecond
	embedRetryMax    = 30 * time.Second
	// embedTotalTimeout bounds a whole EmbedBatch and is deliberately shorter
	// than embedMaxAttempts x the client timeout: four stalled attempts plus
	// backoff run past 120s, and a batch holding its slot in the process
	// embedding budget that long queues every other ingest behind it. Attempts
	// are the ceiling, this is the cap that usually binds.
	embedTotalTimeout = 90 * time.Second
	// A 429 for a spent quota and one for pacing differ only in the body.
	errorBodyLimit  = 4 << 10
	errorDrainLimit = 64 << 10
)

func (e *OpenAICompatibleEmbedder) sendWithRetry(
	ctx context.Context,
	req *http.Request,
	body []byte,
	want int,
) (openaiembed.Response, error) {
	var (
		lastErr error
		wait    time.Duration
	)
	for attempt := range embedMaxAttempts {
		if attempt > 0 {
			if err := sleepBeforeRetry(ctx, wait); err != nil {
				if lastErr != nil {
					return openaiembed.Response{}, fmt.Errorf("%w; last provider response: %w", err, lastErr)
				}
				return openaiembed.Response{}, err
			}
			req = req.Clone(ctx)
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		parsed, retryAfter, err := e.sendOnce(ctx, req, want)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
		if retryAfter < 0 {
			return openaiembed.Response{}, err
		}
		wait = retryWait(retryAfter, attempt)
	}
	return openaiembed.Response{}, fmt.Errorf("after %d attempts: %w", embedMaxAttempts, lastErr)
}

// sendOnce returns a non-negative delay when a retry is worth it, -1 when not.
func (e *OpenAICompatibleEmbedder) sendOnce(
	ctx context.Context,
	req *http.Request,
	want int,
) (openaiembed.Response, time.Duration, error) {
	reply, err := closeableHTTPDo(e.httpClient(), req)
	if err != nil {
		if ctx.Err() != nil {
			return openaiembed.Response{}, -1, err
		}
		return openaiembed.Response{}, 0, fmt.Errorf("request embeddings from openai compatible API: %w", err)
	}
	// Not folded into the error: a close failing after a good response would
	// otherwise resend a billable batch.
	defer func() { _ = reply.Close() }()

	status := reply.StatusCode
	if status == http.StatusTooManyRequests || status >= 500 {
		// Draining returns the connection to the idle pool.
		detail, _ := io.ReadAll(io.LimitReader(reply.Body, errorBodyLimit))
		_, _ = io.Copy(io.Discard, io.LimitReader(reply.Body, errorDrainLimit))
		return openaiembed.Response{}, headerRetryAfter(reply.Header), fmt.Errorf(
			"openai compatible embed returned %d: %s", status, strings.TrimSpace(string(detail)),
		)
	}
	decoded, decodeErr := openaiembed.Decode(status, reply.Body, want)
	if decodeErr != nil {
		return openaiembed.Response{}, -1, decodeErr
	}
	return decoded, 0, nil
}

// headerRetryAfter honours the provider's own pacing when it sends one.
func headerRetryAfter(h http.Header) time.Duration {
	raw := strings.TrimSpace(h.Get("Retry-After"))
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return boundedRetryAfter(time.Duration(seconds) * time.Second)
	}
	if when, err := http.ParseTime(raw); err == nil {
		if delay := time.Until(when); delay > 0 {
			return boundedRetryAfter(delay)
		}
	}
	return 0
}

// A window longer than we will hold an ingest open. Truncating it would mean
// coming back before the server said, so give up instead.
func boundedRetryAfter(d time.Duration) time.Duration {
	if d > embedRetryMax {
		return -1
	}
	return d
}

// A server delay is a floor, so jitter only ever adds to it. Backoff carries no
// such promise and spreads both ways to break up a synchronised burst.
func retryWait(retryAfter time.Duration, attempt int) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter+rand.N(retryAfter/4+1), embedRetryMax)
	}
	return jittered(backoffFor(attempt))
}

// backoffFor doubles per attempt, counting the zero-based try that just failed.
func backoffFor(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 16 {
		return embedRetryMax
	}
	return min(embedRetryBase<<attempt, embedRetryMax)
}

// jittered stops concurrent batches sharing one Retry-After from waking together.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	spread := d / 2
	if spread <= 0 {
		return d
	}
	return min(d-spread/2+rand.N(spread), embedRetryMax)
}

func sleepBeforeRetry(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (e *OpenAICompatibleEmbedder) validateBatchInputs(inputs []string) ([]string, error) {
	if e == nil {
		return nil, fmt.Errorf("openai compatible embedder is nil")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("inputs cannot be empty")
	}
	if strings.TrimSpace(e.Model) == "" {
		return nil, fmt.Errorf("openai compatible embedder model cannot be empty")
	}
	if e.Dimensions < 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidEmbeddingDimension, e.Dimensions)
	}
	cleanInputs := make([]string, len(inputs))
	for i, input := range inputs {
		if strings.TrimSpace(input) == "" {
			return nil, fmt.Errorf("input cannot be empty")
		}
		cleanInputs[i] = input
	}
	return cleanInputs, nil
}

func (e *OpenAICompatibleEmbedder) embeddingRequestBody(inputs []string) map[string]any {
	body := map[string]any{"model": e.Model, "input": inputs}
	if e.Dimensions > 0 {
		body["dimensions"] = e.Dimensions
	}
	return body
}

func (e *OpenAICompatibleEmbedder) totalTimeout() time.Duration {
	if e.TotalTimeout > 0 {
		return e.TotalTimeout
	}
	return embedTotalTimeout
}

func (e *OpenAICompatibleEmbedder) httpClient() *http.Client {
	if e.HTTPClient != nil {
		return e.HTTPClient
	}
	return &http.Client{Timeout: defaultOpenAICompatibleTimeout, Transport: embedTransport()}
}

// The default transport keeps 2 idle connections per host, so concurrent
// batches would renegotiate TLS for most requests. Built once: a per-client
// clone carries its own pool, which is the problem it exists to solve.
var embedTransport = sync.OnceValue(func() http.RoundTripper {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	clone := t.Clone()
	clone.MaxIdleConnsPerHost = maxEmbedConnsPerHost
	return clone
})

const maxEmbedConnsPerHost = 64
