package kb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// OllamaEmbedder tests
// -----------------------------------------------------------------------------

func TestOllamaEmbedder(t *testing.T) {
	testCases := []struct {
		name        string
		baseURL     string
		model       string
		input       string
		wantErr     bool
		errContains string
	}{
		{
			name:        "empty_input",
			baseURL:     "http://localhost:11434",
			model:       "all-minilm",
			input:       "",
			wantErr:     true,
			errContains: "empty",
		},
		{
			name:        "whitespace_only_input",
			baseURL:     "http://localhost:11434",
			model:       "all-minilm",
			input:       "   ",
			wantErr:     true,
			errContains: "empty",
		},
		{
			name:    "unreachable_url",
			baseURL: "http://127.0.0.1:1",
			model:   "all-minilm",
			input:   "hello",
			wantErr: true,
		},
		{
			name:    "invalid_url_scheme",
			baseURL: "not-a-url://bad",
			model:   "all-minilm",
			input:   "hello",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			embedder := NewOllamaEmbedder(tc.baseURL, tc.model)

			var err error
			if tc.name == "empty_input" || tc.name == "whitespace_only_input" {
				testKB := NewKB(
					&LocalBlobStore{Root: t.TempDir()},
					t.TempDir(),
					WithEmbedder(embedder),
				)
				_, err = testKB.Embed(context.Background(), tc.input)
			} else {
				_, err = embedder.Embed(context.Background(), tc.input)
			}

			if tc.wantErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}

			require.NoError(t, err)
		})
	}
}

// -----------------------------------------------------------------------------
// OllamaGrapher tests
// -----------------------------------------------------------------------------

func ollamaGrapherResponse(innerJSON string) []byte {
	body, err := json.Marshal(ollamaGenerateResponse{Response: innerJSON})
	if err != nil {
		panic(err)
	}
	return body
}

func TestOllamaGrapher(t *testing.T) {
	type wantEdge struct {
		src    string
		dst    string
		rel    string
		weight float64
	}

	testCases := []struct {
		name            string
		chunks          []Chunk
		mockStatus      int
		mockBody        []byte
		mockBodies      [][]byte
		baseURL         string
		wantErr         bool
		errContains     string
		wantEntityCount int
		wantEdgeCount   int
		wantEntityNames []string
		wantEdge        *wantEdge
		checkRequest    func(*testing.T, ollamaGenerateRequest)
		checkResult     func(*testing.T, *GraphExtraction)
	}{
		{
			name:            "nil_chunks",
			chunks:          nil,
			mockStatus:      http.StatusOK,
			mockBody:        ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:            "empty_chunks",
			chunks:          []Chunk{},
			mockStatus:      http.StatusOK,
			mockBody:        ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:            "valid_entities_and_edges",
			chunks:          []Chunk{{DocID: "d1", ChunkID: "d1-chunk-000", Text: "Alice works at ACME Corp."}},
			mockStatus:      http.StatusOK,
			mockBody:        ollamaGrapherResponse(`{"entities":["Alice","ACME Corp"],"edges":[{"src":"Alice","dst":"ACME Corp","rel":"works_at","weight":0.9}]}`),
			wantEntityCount: 2,
			wantEdgeCount:   1,
			wantEntityNames: []string{"Alice", "ACME Corp"},
			wantEdge: &wantEdge{
				src:    "Alice",
				dst:    "ACME Corp",
				rel:    "works_at",
				weight: 0.9,
			},
		},
		{
			name:            "empty_entities_empty_edges",
			chunks:          []Chunk{{ChunkID: "c0", Text: "no entities here"}},
			mockStatus:      http.StatusOK,
			mockBody:        ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:        "non_200_status",
			chunks:      []Chunk{{ChunkID: "c0", Text: "x"}},
			mockStatus:  http.StatusInternalServerError,
			mockBody:    []byte("internal error"),
			wantErr:     true,
			errContains: "500",
		},
		{
			name:       "malformed_outer_json",
			chunks:     []Chunk{{ChunkID: "c0", Text: "x"}},
			mockStatus: http.StatusOK,
			mockBody:   []byte("not-json"),
			wantErr:    true,
		},
		{
			name:        "malformed_inner_json",
			chunks:      []Chunk{{ChunkID: "c0", Text: "x"}},
			mockStatus:  http.StatusOK,
			mockBody:    ollamaGrapherResponse(`{broken`),
			wantErr:     true,
			errContains: "c0",
		},
		{
			name:    "unreachable_url",
			baseURL: "http://127.0.0.1:1",
			chunks:  []Chunk{{ChunkID: "c0", Text: "x"}},
			wantErr: true,
		},
		{
			name:       "prompt_contains_chunk_text",
			chunks:     []Chunk{{ChunkID: "c0", Text: "uniqueMarker"}},
			mockStatus: http.StatusOK,
			mockBody:   ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			checkRequest: func(t *testing.T, req ollamaGenerateRequest) {
				assert.Contains(t, req.Prompt, "uniqueMarker")
			},
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:       "request_model_field",
			chunks:     []Chunk{{ChunkID: "c0", Text: "x"}},
			mockStatus: http.StatusOK,
			mockBody:   ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			checkRequest: func(t *testing.T, req ollamaGenerateRequest) {
				assert.Equal(t, defaultGrapherModel, req.Model)
			},
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:       "request_format_field",
			chunks:     []Chunk{{ChunkID: "c0", Text: "x"}},
			mockStatus: http.StatusOK,
			mockBody:   ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			checkRequest: func(t *testing.T, req ollamaGenerateRequest) {
				assert.Equal(t, "json", req.Format)
			},
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:       "request_stream_false",
			chunks:     []Chunk{{ChunkID: "c0", Text: "x"}},
			mockStatus: http.StatusOK,
			mockBody:   ollamaGrapherResponse(`{"entities":[],"edges":[]}`),
			checkRequest: func(t *testing.T, req ollamaGenerateRequest) {
				assert.False(t, req.Stream)
			},
			wantEntityCount: 0,
			wantEdgeCount:   0,
		},
		{
			name:            "multi_chunk_aggregates",
			chunks:          []Chunk{{DocID: "d", ChunkID: "d-chunk-000", Text: "a"}, {DocID: "d", ChunkID: "d-chunk-001", Text: "b"}},
			mockStatus:      http.StatusOK,
			mockBodies:      [][]byte{ollamaGrapherResponse(`{"entities":["Entity1"],"edges":[]}`), ollamaGrapherResponse(`{"entities":["Entity2"],"edges":[]}`)},
			wantEntityCount: 2,
			wantEdgeCount:   0,
			checkResult: func(t *testing.T, result *GraphExtraction) {
				require.Len(t, result.Entities, 2)
				names := []string{result.Entities[0].Name, result.Entities[1].Name}
				chunkIDs := []string{result.Entities[0].ChunkID, result.Entities[1].ChunkID}
				assert.ElementsMatch(t, []string{"Entity1", "Entity2"}, names)
				assert.ElementsMatch(t, []string{"d-chunk-000", "d-chunk-001"}, chunkIDs)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			grapher := NewOllamaGrapher(tc.baseURL, "")

			if tc.baseURL == "" {
				var mu sync.Mutex
				callIndex := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, http.MethodPost, r.Method)

					var reqBody ollamaGenerateRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&reqBody))
					if tc.checkRequest != nil {
						tc.checkRequest(t, reqBody)
					}

					status := tc.mockStatus
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)

					body := tc.mockBody
					if len(tc.mockBodies) > 0 {
						mu.Lock()
						idx := callIndex
						callIndex++
						mu.Unlock()
						if idx < len(tc.mockBodies) {
							body = tc.mockBodies[idx]
						} else {
							body = tc.mockBodies[len(tc.mockBodies)-1]
						}
					}

					if body != nil {
						_, err := w.Write(body)
						require.NoError(t, err)
					}
				}))
				t.Cleanup(server.Close)
				grapher = NewOllamaGrapher(server.URL, "")
			}

			result, err := grapher.Extract(ctx, tc.chunks)
			if tc.wantErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Len(t, result.Entities, tc.wantEntityCount)
			assert.Len(t, result.Edges, tc.wantEdgeCount)

			if len(tc.wantEntityNames) > 0 {
				names := make([]string, 0, len(result.Entities))
				for _, entity := range result.Entities {
					names = append(names, entity.Name)
				}
				assert.ElementsMatch(t, tc.wantEntityNames, names)
			}

			if tc.wantEdge != nil {
				require.NotEmpty(t, result.Edges)
				edge := result.Edges[0]
				assert.Equal(t, tc.wantEdge.src, edge.Src)
				assert.Equal(t, tc.wantEdge.dst, edge.Dst)
				assert.Equal(t, tc.wantEdge.rel, edge.RelType)
				assert.Equal(t, tc.wantEdge.weight, edge.Weight)
			}

			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}
