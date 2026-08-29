package kb_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/require"
)

func TestOpenAICompatibleEmbedder(t *testing.T) {
	t.Run("sends_openai_compatible_request", func(t *testing.T) {
		var gotPath string
		var gotAuth string
		var gotBody struct {
			Model      string   `json:"model"`
			Input      []string `json:"input"`
			Dimensions int      `json:"dimensions"`
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotAuth = r.Header.Get("Authorization")
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"embedding":[0.25,0.5,0.75]}]}`))
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(kb.OpenAICompatibleEmbedderConfig{
			BaseURL:    server.URL + "/v1/",
			Model:      "text-embedding-3-small",
			Token:      "secret-token",
			Dimensions: 3,
		})
		require.NoError(t, err)

		vec, err := embedder.Embed(context.Background(), "hello")
		require.NoError(t, err)
		require.Equal(t, []float32{0.25, 0.5, 0.75}, vec)
		require.Equal(t, "/v1/embeddings", gotPath)
		require.Equal(t, "Bearer secret-token", gotAuth)
		require.Equal(t, "text-embedding-3-small", gotBody.Model)
		require.Equal(t, []string{"hello"}, gotBody.Input)
		require.Equal(t, 3, gotBody.Dimensions)
	})

	t.Run("embeds_batch", func(t *testing.T) {
		var gotBody struct {
			Input []string `json:"input"`
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
			_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,2]},{"index":1,"embedding":[3,4]}]}`))
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
		)
		require.NoError(t, err)
		vectors, err := embedder.EmbedBatch(context.Background(), []string{"one", "two"})
		require.NoError(t, err)
		require.Equal(t, []string{"one", "two"}, gotBody.Input)
		require.Equal(t, [][]float32{{1, 2}, {3, 4}}, vectors)
	})

	t.Run("reorders_batch_by_index", func(t *testing.T) {
		// The API does not promise response order. Positional assembly would
		// give every document another document's vector, silently.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(
				`{"data":[{"index":2,"embedding":[5,6]},{"index":0,"embedding":[1,2]},{"index":1,"embedding":[3,4]}]}`,
			))
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
		)
		require.NoError(t, err)
		vectors, err := embedder.EmbedBatch(context.Background(), []string{"one", "two", "three"})
		require.NoError(t, err)
		require.Equal(t, [][]float32{{1, 2}, {3, 4}, {5, 6}}, vectors)
	})

	t.Run("embeds_batch_without_indices", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"embedding":[1,2]},{"embedding":[3,4]}]}`))
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
		)
		require.NoError(t, err)
		vectors, err := embedder.EmbedBatch(context.Background(), []string{"one", "two"})
		require.NoError(t, err)
		require.Equal(t, [][]float32{{1, 2}, {3, 4}}, vectors)
	})

	t.Run("omits_optional_token_and_dimensions", func(t *testing.T) {
		var gotAuth string
		var gotBody map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
			_, _ = w.Write([]byte(`{"data":[{"embedding":[1,2]}]}`))
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "nomic-embed-text"},
		)
		require.NoError(t, err)
		_, err = embedder.Embed(context.Background(), "hello")
		require.NoError(t, err)
		require.Empty(t, gotAuth)
		require.NotContains(t, gotBody, "dimensions")
	})

	t.Run("rejects_empty_embeddings", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"embedding":[]}]}`))
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
		)
		require.NoError(t, err)
		_, err = embedder.Embed(context.Background(), "hello")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty embedding")
	})

	t.Run("surfaces_non_200_body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad token", http.StatusUnauthorized)
		}))
		defer server.Close()

		embedder, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"},
		)
		require.NoError(t, err)
		_, err = embedder.Embed(context.Background(), "hello")
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")
		require.Contains(t, err.Error(), "bad token")
	})

	t.Run("validates_constructor_inputs", func(t *testing.T) {
		_, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: "ftp://example.com", Model: "model"},
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "scheme")

		_, err = kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: "http://example.com", Model: ""},
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "model")

		_, err = kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: "http://example.com", Model: "model", Dimensions: -1},
		)
		require.ErrorIs(t, err, kb.ErrInvalidEmbeddingDimension)
	})

	t.Run("constructor sets default timeout", func(t *testing.T) {
		emb, err := kb.NewOpenAICompatibleEmbedder(
			kb.OpenAICompatibleEmbedderConfig{BaseURL: "http://example.com", Model: "model"},
		)
		require.NoError(t, err)
		require.NotNil(t, emb.HTTPClient)
		require.Equal(t, 30*time.Second, emb.HTTPClient.Timeout)
	})
}
