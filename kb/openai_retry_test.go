package kb_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func retryServer(t *testing.T, handler func(attempt int64, w http.ResponseWriter)) (*kb.OpenAICompatibleEmbedder, *atomic.Int64) {
	t.Helper()
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handler(attempts.Add(1), w)
	}))
	t.Cleanup(server.Close)
	embedder, err := kb.NewOpenAICompatibleEmbedder(
		kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
	)
	require.NoError(t, err)
	return embedder, &attempts
}

func okBody(w http.ResponseWriter) {
	_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,2]}]}`))
}

func TestOpenAIEmbedRetry(t *testing.T) {
	t.Run("retries a 429 and succeeds", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(attempt int64, w http.ResponseWriter) {
			if attempt == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			okBody(w)
		})
		vectors, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.NoError(t, err)
		require.Equal(t, [][]float32{{1, 2}}, vectors)
		require.Equal(t, int64(2), attempts.Load())
	})

	t.Run("retries a 500", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(attempt int64, w http.ResponseWriter) {
			if attempt < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			okBody(w)
		})
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.NoError(t, err)
		require.Equal(t, int64(3), attempts.Load())
	})

	t.Run("does not retry a 400", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(_ int64, w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadRequest)
		})
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.Error(t, err)
		require.Equal(t, int64(1), attempts.Load(), "a bad request answers the same way every time")
	})

	t.Run("does not retry a 401", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(_ int64, w http.ResponseWriter) {
			w.WriteHeader(http.StatusUnauthorized)
		})
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.Error(t, err)
		require.Equal(t, int64(1), attempts.Load())
	})

	t.Run("gives up and reports the attempts", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(_ int64, w http.ResponseWriter) {
			w.WriteHeader(http.StatusTooManyRequests)
		})
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.ErrorContains(t, err, "attempts")
		require.ErrorContains(t, err, "429")
		require.Equal(t, int64(4), attempts.Load())
	})

	t.Run("honours Retry-After in seconds", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(attempt int64, w http.ResponseWriter) {
			if attempt == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			okBody(w)
		})
		start := time.Now()
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.NoError(t, err)
		require.GreaterOrEqual(t, time.Since(start), time.Second, "came back before the window reopened")
		require.Equal(t, int64(2), attempts.Load())
	})

	t.Run("ignores a Retry-After in the past", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(attempt int64, w http.ResponseWriter) {
			if attempt == 1 {
				w.Header().Set("Retry-After", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			okBody(w)
		})
		start := time.Now()
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.NoError(t, err)
		// Backoff alone, not the hour the header names.
		require.Less(t, time.Since(start), time.Second)
		require.Equal(t, int64(2), attempts.Load())
	})

	t.Run("gives up when Retry-After exceeds the ceiling", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(_ int64, w http.ResponseWriter) {
			w.Header().Set("Retry-After", "600")
			w.WriteHeader(http.StatusTooManyRequests)
		})
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.Error(t, err)
		require.Equal(t, int64(1), attempts.Load(), "waited out a window longer than an ingest holds")
	})

	t.Run("a cancelled context stops retrying", func(t *testing.T) {
		embedder, attempts := retryServer(t, func(_ int64, w http.ResponseWriter) {
			w.WriteHeader(http.StatusTooManyRequests)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := embedder.EmbedBatch(ctx, []string{"one"})
		require.Error(t, err)
		require.Less(t, attempts.Load(), int64(4), "backoff should be cut short")
	})

	t.Run("a whole batch is bounded, not just one attempt", func(t *testing.T) {
		// Every attempt is told to come back in 30s, which without a total
		// deadline holds a slot in the process embedding budget for minutes.
		embedder, attempts := retryServer(t, func(_ int64, w http.ResponseWriter) {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		})
		embedder.TotalTimeout = 200 * time.Millisecond

		start := time.Now()
		_, err := embedder.EmbedBatch(context.Background(), []string{"one"})
		require.Error(t, err)
		require.Less(t, time.Since(start), 5*time.Second, "the retry chain outlived its deadline")
		require.Less(t, attempts.Load(), int64(4))
	})

	t.Run("resends the body on every attempt", func(t *testing.T) {
		var bodies atomic.Int64
		var attempts atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sent, _ := io.ReadAll(r.Body)
			if strings.Contains(string(sent), `"one"`) {
				bodies.Add(1)
			}
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			okBody(w)
		}))
		defer server.Close()
		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
		)
		require.NoError(t, err)
		_, err = embedder.EmbedBatch(context.Background(), []string{"one"})
		require.NoError(t, err)
		require.Equal(t, int64(2), bodies.Load(), "the retry sent a different body")
	})

}
