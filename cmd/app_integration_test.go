package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mikills/kbcore/kb"
	"github.com/mikills/kbcore/kb/testutil"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queryResultPayload struct {
	Results []kb.QueryResult `json:"results"`
}

func setupAppTest(t *testing.T, embedder kb.Embedder, kbID string) string {
	return setupAppTestWithOptions(t, embedder, kbID)
}

func setupAppTestWithOptions(t *testing.T, embedder kb.Embedder, kbID string, opts ...kb.KBOption) string {
	t.Helper()

	ctx := context.Background()
	bucket := "kbcore-" + strings.ReplaceAll(strings.ToLower(kbID), "_", "-")

	s3Mock, err := testutil.StartMockS3(ctx, bucket)
	require.NoError(t, err)
	t.Cleanup(s3Mock.Close)

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	leaseMgr, err := kb.NewRedisWriteLeaseManager(redisClient, "test:lease:")
	require.NoError(t, err)

	cacheDir := filepath.Join(t.TempDir(), "cache")
	blobStore := kb.NewS3BlobStore(s3Mock.Client, s3Mock.Bucket, "")

	kbOpts := []kb.KBOption{
		kb.WithEmbedder(embedder),
		kb.WithWriteLeaseManager(leaseMgr),
		kb.WithWriteLeaseTTL(3 * time.Second),
		kb.WithDuckDBExtensionDir(kb.TestExtensionDir()),
	}
	kbOpts = append(kbOpts, opts...)

	loader := kb.NewKB(blobStore, cacheDir, kbOpts...)

	app := NewApp(loader, AppConfig{Address: "127.0.0.1:0"})
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		_ = app.Stop(context.Background())
		_ = app.Wait()
	})

	require.NotEmpty(t, app.Address())
	return "http://" + app.Address()
}

func readFinanceDocument(t *testing.T) string {
	t.Helper()

	financePath := filepath.Join("..", "kb", "testdata", "documents", "finance-aapl-10k.txt")
	financeDoc, err := os.ReadFile(financePath)
	require.NoError(t, err)
	return string(financeDoc)
}

func TestAppRAG(t *testing.T) {
	t.Run("s3_redis", testAppRAGS3Redis)
	t.Run("ollama_smoke", testAppRAGOllama)
	t.Run("validation", testAppRAGValidation)
	t.Run("modes_sharded", testAppRAGModesSharded)
	t.Run("query_fields_graph", testAppQueryFieldsGraph)
}

func testAppRAGS3Redis(t *testing.T) {
	embedder := appKeywordEmbedder{}
	kbID := "kb-app-integration"
	baseURL := setupAppTest(t, embedder, kbID)
	financeDoc := readFinanceDocument(t)

	ingestResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		"kb_id":         kbID,
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "finance-aapl-10k", "text": financeDoc},
			{"id": "pasta-guide", "text": "Pasta carbonara recipe with parmesan, eggs, pancetta, and black pepper. Boil pasta, whisk egg and cheese, then combine off heat to avoid scrambling."},
		},
		"chunk_size": 450,
	})
	require.NoError(t, err)
	defer ingestResp.Body.Close()
	require.Equal(t, http.StatusOK, ingestResp.StatusCode)

	var ingestPayload map[string]any
	require.NoError(t, json.NewDecoder(ingestResp.Body).Decode(&ingestPayload))
	assert.Greater(t, int(ingestPayload["ingested_chunks"].(float64)), 1)

	type queryCase struct {
		name               string
		query              string
		expectFinanceChunk bool
	}

	queryCases := []queryCase{
		{name: "expected_hit", query: "iphone revenue and services growth", expectFinanceChunk: true},
		{name: "expected_miss", query: "pasta carbonara parmesan recipe", expectFinanceChunk: false},
	}

	for _, tc := range queryCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp, err := postJSON(baseURL+"/rag/query", map[string]any{
				"kb_id": kbID,
				"query": tc.query,
				"k":     5,
			})
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var payload queryResultPayload
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			require.NotEmpty(t, payload.Results)

			hasFinanceChunk := false
			for _, item := range payload.Results {
				assert.NotEmpty(t, item.Content)
				if strings.HasPrefix(item.ID, "finance-aapl-10k-chunk-") {
					hasFinanceChunk = true
					break
				}
			}

			if tc.expectFinanceChunk {
				assert.True(t, hasFinanceChunk, "%s expected finance chunk hit", tc.name)
			} else {
				assert.False(t, strings.HasPrefix(payload.Results[0].ID, "finance-aapl-10k-chunk-"), "%s expected top result to miss finance chunks", tc.name)
			}
		})
	}
}

func testAppRAGOllama(t *testing.T) {
	ollamaBaseURL := strings.TrimSpace(os.Getenv("OLLAMA_BASE_URL"))
	if ollamaBaseURL == "" {
		t.Skip("set OLLAMA_BASE_URL to run Ollama-backed app integration test")
	}

	model := strings.TrimSpace(os.Getenv("OLLAMA_MODEL"))
	if model == "" {
		model = "all-minilm"
	}

	embedder := &appOllamaEmbedder{BaseURL: ollamaBaseURL, Model: model}
	vec, err := embedder.Embed(context.Background(), "ping")
	require.NoError(t, err, "Ollama not reachable or model not pulled")
	require.NotEmpty(t, vec, "ollama embedding vector must be non-empty")

	kbID := "kb-app-ollama"
	appBaseURL := setupAppTest(t, embedder, kbID)
	financeDoc := readFinanceDocument(t)

	ingestResp, err := postJSON(appBaseURL+"/rag/ingest", map[string]any{
		"kb_id":         kbID,
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "finance-aapl-10k", "text": financeDoc},
			{"id": "pasta-guide", "text": "Pasta carbonara recipe with parmesan, eggs, pancetta, and black pepper. Boil pasta, whisk egg and cheese, then combine off heat to avoid scrambling."},
		},
		"chunk_size": 450,
	})
	require.NoError(t, err)
	defer ingestResp.Body.Close()
	require.Equal(t, http.StatusOK, ingestResp.StatusCode)

	var ingestPayload map[string]any
	require.NoError(t, json.NewDecoder(ingestResp.Body).Decode(&ingestPayload))
	assert.Greater(t, int(ingestPayload["ingested_chunks"].(float64)), 1)

	resp, err := postJSON(appBaseURL+"/rag/query", map[string]any{
		"kb_id": kbID,
		"query": "iphone sales and services revenue trends",
		"k":     5,
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload queryResultPayload
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.NotEmpty(t, payload.Results)
	assert.NotEmpty(t, payload.Results[0].ID)
	assert.NotEmpty(t, payload.Results[0].Content)
}

func testAppRAGValidation(t *testing.T) {
	baseURL := setupAppTest(t, appKeywordEmbedder{}, "kb-app-validation")

	seedResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		"kb_id":         "kb-app-validation",
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "query-seed", "text": "seed queryable document"},
		},
	})
	require.NoError(t, err)
	defer seedResp.Body.Close()
	require.Equal(t, http.StatusOK, seedResp.StatusCode)

	testCases := []struct {
		name           string
		path           string
		body           map[string]any
		expectedStatus int
		expectedError  string
	}{
		{
			name:           "ingest_missing_kb_id",
			path:           "/rag/ingest",
			body:           map[string]any{"graph_enabled": false, "documents": []map[string]string{{"id": "doc-1", "text": "hello"}}},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "ingest_missing_graph_enabled",
			path:           "/rag/ingest",
			body:           map[string]any{"kb_id": "kb-app-validation", "documents": []map[string]string{{"id": "doc-1", "text": "hello"}}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph_enabled is required",
		},
		{
			name:           "ingest_empty_documents",
			path:           "/rag/ingest",
			body:           map[string]any{"kb_id": "kb-app-validation", "graph_enabled": false, "documents": []map[string]string{}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "documents are required",
		},
		{
			name:           "ingest_graph_enabled_true_unavailable",
			path:           "/rag/ingest",
			body:           map[string]any{"kb_id": "kb-app-validation", "graph_enabled": true, "documents": []map[string]string{{"id": "doc-2", "text": "hello graph"}}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph extraction is not configured",
		},
		{
			name:           "query_empty_query",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "", "k": 5},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "query is required",
		},
		{
			name:           "query_mode_omitted_defaults_vector",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "seed queryable document", "k": 5},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "query_mode_vector_success",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "seed queryable document", "k": 5, "search_mode": "vector"},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "query_mode_adaptive_success",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "seed queryable document", "k": 5, "search_mode": "adaptive"},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "query_mode_graph_unavailable",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "seed queryable document", "k": 5, "search_mode": "graph"},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph query requested but graph data is unavailable",
		},
		{
			name:           "query_mode_graph_case_insensitive_unavailable",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "seed queryable document", "k": 5, "search_mode": "GRAPH"},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph query requested but graph data is unavailable",
		},
		{
			name:           "query_mode_invalid",
			path:           "/rag/query",
			body:           map[string]any{"kb_id": "kb-app-validation", "query": "seed queryable document", "k": 5, "search_mode": "bad-mode"},
			expectedStatus: http.StatusBadRequest,
			expectedError:  `invalid search_mode: "bad-mode" (allowed: vector, graph, adaptive)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, postErr := postJSON(baseURL+tc.path, tc.body)
			require.NoError(t, postErr)
			defer resp.Body.Close()

			require.Equal(t, tc.expectedStatus, resp.StatusCode)

			if tc.expectedError != "" {
				var payload map[string]string
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, tc.expectedError, payload["error"])
			}
		})
	}
}

func testAppRAGModesSharded(t *testing.T) {
	kbID := "kb-app-query-modes-sharded"
	baseURL := setupAppTestWithOptions(t, appKeywordEmbedder{}, kbID,
		kb.WithShardingPolicy(kb.ShardingPolicy{ShardTriggerVectorRows: 1}),
	)

	seedResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		"kb_id":         kbID,
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "query-seed", "text": "seed queryable document"},
		},
	})
	require.NoError(t, err)
	defer seedResp.Body.Close()
	require.Equal(t, http.StatusOK, seedResp.StatusCode)

	testCases := []struct {
		name           string
		searchMode     string
		expectedStatus int
		expectedError  string
	}{
		{name: "mode_vector", searchMode: "vector", expectedStatus: http.StatusOK},
		{name: "mode_graph_unavailable", searchMode: "graph", expectedStatus: http.StatusBadRequest, expectedError: kb.ErrGraphQueryUnavailable.Error()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				"kb_id": kbID,
				"query": "seed queryable document",
				"k":     5,
			}
			if tc.searchMode != "" {
				body["search_mode"] = tc.searchMode
			}

			resp, err := postJSON(baseURL+"/rag/query", body)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.expectedStatus, resp.StatusCode)

			if tc.expectedError != "" {
				var payload map[string]string
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, tc.expectedError, payload["error"])
			}
		})
	}
}

type queryResultWithScoresPayload struct {
	Results []struct {
		ID         string   `json:"id"`
		Content    string   `json:"content"`
		Distance   float64  `json:"distance"`
		Score      *float64 `json:"score"`
		GraphScore *float64 `json:"graph_score"`
	} `json:"results"`
}

func testAppQueryFieldsGraph(t *testing.T) {
	kbID := "kb-app-query-graph-fields"
	baseURL := setupAppTestWithOptions(
		t,
		appKeywordEmbedder{},
		kbID,
		kb.WithGraphBuilder(&kb.GraphBuilder{
			Chunker:       appTestChunker{},
			Grapher:       appTestGrapher{},
			Canonicalizer: appIdentityCanonicalizer{},
		}),
	)

	ingestResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		"kb_id":         kbID,
		"graph_enabled": true,
		"documents": []map[string]string{
			{"id": "doc-finance", "text": "finance revenue services growth"},
			{"id": "doc-food", "text": "pasta carbonara parmesan recipe"},
		},
		"chunk_size": 300,
	})
	require.NoError(t, err)
	defer ingestResp.Body.Close()
	require.Equal(t, http.StatusOK, ingestResp.StatusCode)

	testCases := []struct {
		name       string
		searchMode string
		wantScores bool
	}{
		{name: "vector", searchMode: "vector", wantScores: false},
		{name: "graph", searchMode: "graph", wantScores: true},
		{name: "graph_upper", searchMode: "GRAPH", wantScores: true},
		{name: "adaptive_mixed_case", searchMode: "AdApTiVe", wantScores: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resp, err := postJSON(baseURL+"/rag/query", map[string]any{
				"kb_id":       kbID,
				"query":       "finance services growth",
				"k":           5,
				"search_mode": tc.searchMode,
			})
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var payload queryResultWithScoresPayload
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			require.NotEmpty(t, payload.Results)
			assert.NotEmpty(t, payload.Results[0].ID)
			assert.NotEmpty(t, payload.Results[0].Content)

			if tc.wantScores {
				assert.NotNil(t, payload.Results[0].Score)
				assert.NotNil(t, payload.Results[0].GraphScore)
			} else {
				assert.Nil(t, payload.Results[0].Score)
				assert.Nil(t, payload.Results[0].GraphScore)
			}
		})
	}
}

type appTestChunker struct{}

func (appTestChunker) Chunk(_ context.Context, docID string, text string) ([]kb.Chunk, error) {
	return []kb.Chunk{{
		DocID:   docID,
		ChunkID: docID + "-chunk-000",
		Text:    text,
		Start:   0,
		End:     len(text),
	}}, nil
}

type appTestGrapher struct{}

func (appTestGrapher) Extract(_ context.Context, chunks []kb.Chunk) (*kb.GraphExtraction, error) {
	entities := make([]kb.EntityCandidate, 0, len(chunks)*2)
	edges := make([]kb.EdgeCandidate, 0, len(chunks))
	for _, chunk := range chunks {
		entityName := "ent:" + chunk.DocID
		entities = append(entities,
			kb.EntityCandidate{Name: entityName, ChunkID: chunk.ChunkID},
			kb.EntityCandidate{Name: "shared", ChunkID: chunk.ChunkID},
		)
		edges = append(edges, kb.EdgeCandidate{
			Src:     entityName,
			Dst:     "shared",
			RelType: "mentions",
			Weight:  1.0,
			ChunkID: chunk.ChunkID,
		})
	}
	return &kb.GraphExtraction{Entities: entities, Edges: edges}, nil
}

type appIdentityCanonicalizer struct{}

func (appIdentityCanonicalizer) Canonicalize(_ context.Context, name string) (string, error) {
	return name, nil
}

type appKeywordEmbedder struct{}

func (appKeywordEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	lower := strings.ToLower(input)
	finance := keywordScore(lower, []string{"finance", "revenue", "iphone", "services", "apple", "sales", "margin"})
	food := keywordScore(lower, []string{"pasta", "carbonara", "egg", "parmesan", "recipe", "cook"})
	bias := float32(len(strings.Fields(lower))%5) * 0.01
	return []float32{finance + bias, food + bias, 0.1 + bias}, nil
}

func keywordScore(input string, words []string) float32 {
	score := float32(0)
	for _, w := range words {
		if strings.Contains(input, w) {
			score += 1
		}
	}
	return score
}

type appOllamaEmbedder struct {
	BaseURL string
	Model   string
}

func (o *appOllamaEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	reqBody, err := json.Marshal(map[string]string{
		"model": o.Model,
		"input": input,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(o.BaseURL, "/")+"/api/embed", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Embeddings) == 0 || len(result.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("empty embeddings in response")
	}

	vec := make([]float32, len(result.Embeddings[0]))
	for i, v := range result.Embeddings[0] {
		vec[i] = float32(v)
	}

	return vec, nil
}

func postJSON(url string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return resp, nil
}
