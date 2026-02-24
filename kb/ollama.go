package kb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

const (
	defaultOllamaBaseURL = "http://localhost:11434"
	defaultOllamaModel   = "all-minilm"

	defaultGrapherModel   = "gemma3:4b"
	defaultGrapherWorkers = 4
)

const ollamaGrapherPromptTemplate = `You are a knowledge-graph extraction system. Extract named entities and directed relationships from the text below.

Output schema (return exactly this JSON structure, one line, no markdown fences):
{"entities":["string"],"edges":[{"src":"string","dst":"string","rel":"string","weight":1.0}]}

Entity rules:
- Extract people, organizations, locations, products, events, concepts, and technical terms.
- Use the fullest canonical name found in the text (e.g. "John Smith" not "John", "ACME Corporation" not "ACME").
- Resolve pronouns and abbreviations to their full entity name when clearly identifiable.
- No duplicate entity names in the list.

Edge rules:
- src is the subject (actor), dst is the object (target) of the relationship.
- rel is a lowercase snake_case verb phrase (e.g. works_at, founded_by, located_in, acquired, authored).
- Both src and dst must appear in the entities list.
- weight is a confidence/relevance float from 0.1 to 1.0; use 1.0 when clearly stated, lower when implied.
- No duplicate edges.

If no entities can be extracted, return {"entities":[],"edges":[]}.

Example:
Input: "In 2015, Sundar Pichai became CEO of Google. Google is headquartered in Mountain View, California."
Output: {"entities":["Sundar Pichai","Google","Mountain View","California"],"edges":[{"src":"Sundar Pichai","dst":"Google","rel":"ceo_of","weight":1.0},{"src":"Google","dst":"Mountain View","rel":"headquartered_in","weight":1.0},{"src":"Mountain View","dst":"California","rel":"located_in","weight":1.0}]}

Text:
%s`

// OllamaEmbedder implements Embedder using Ollama /api/embed.
type OllamaEmbedder struct {
	BaseURL string
	Model   string
}

// NewOllamaEmbedder creates an Ollama embedder with optional overrides.
func NewOllamaEmbedder(baseURL, model string) *OllamaEmbedder {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		trimmedBaseURL = defaultOllamaBaseURL
	}

	trimmedModel := strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = defaultOllamaModel
	}

	return &OllamaEmbedder{
		BaseURL: trimmedBaseURL,
		Model:   trimmedModel,
	}
}

// Embed requests a vector embedding from Ollama.
func (o *OllamaEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	requestBody, err := json.Marshal(map[string]string{
		"model": o.Model,
		"input": input,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal ollama embed request: %w", err)
	}

	endpoint := strings.TrimRight(o.BaseURL, "/") + "/api/embed"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("create ollama embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request embeddings from ollama: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		if len(body) == 0 {
			return nil, fmt.Errorf("ollama embed request failed with status %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("ollama embed request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode ollama embed response: %w", err)
	}

	if len(parsed.Embeddings) == 0 || len(parsed.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("ollama embed response contained empty embeddings")
	}

	vector := make([]float32, len(parsed.Embeddings[0]))
	for i, v := range parsed.Embeddings[0] {
		vector[i] = float32(v)
	}

	return vector, nil
}

// Ping validates Ollama connectivity by requesting a short embedding.
func (o *OllamaEmbedder) Ping(ctx context.Context) error {
	_, err := o.Embed(ctx, "ping")
	return err
}

// OllamaGrapher extracts graph candidates using Ollama /api/generate.
type OllamaGrapher struct {
	BaseURL     string
	Model       string
	MaxParallel int
}

// NewOllamaGrapher creates an Ollama grapher with optional overrides.
func NewOllamaGrapher(baseURL, model string) *OllamaGrapher {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		trimmedBaseURL = defaultOllamaBaseURL
	}

	trimmedModel := strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = defaultGrapherModel
	}

	return &OllamaGrapher{
		BaseURL:     trimmedBaseURL,
		Model:       trimmedModel,
		MaxParallel: defaultGrapherWorkers,
	}
}

type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Format string `json:"format"`
	Stream bool   `json:"stream"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}

type ollamaGrapherResult struct {
	Entities []string            `json:"entities"`
	Edges    []ollamaGrapherEdge `json:"edges"`
}

type ollamaGrapherEdge struct {
	Src    string  `json:"src"`
	Dst    string  `json:"dst"`
	Rel    string  `json:"rel"`
	Weight float64 `json:"weight"`
}

var _ Grapher = (*OllamaGrapher)(nil)

func (o *OllamaGrapher) extractChunk(ctx context.Context, chunk Chunk) ([]EntityCandidate, []EdgeCandidate, error) {
	prompt := fmt.Sprintf(ollamaGrapherPromptTemplate, chunk.Text)
	payload, err := json.Marshal(ollamaGenerateRequest{
		Model:  o.Model,
		Prompt: prompt,
		Format: "json",
		Stream: false,
	})
	if err != nil {
		return nil, nil, err
	}

	url := strings.TrimRight(o.BaseURL, "/") + "/api/generate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr != nil {
			return nil, nil, fmt.Errorf("ollama grapher request failed with status %d", resp.StatusCode)
		}
		return nil, nil, fmt.Errorf("ollama grapher request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response ollamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, nil, err
	}

	var result ollamaGrapherResult
	if err := json.Unmarshal([]byte(response.Response), &result); err != nil {
		return nil, nil, fmt.Errorf("ollama grapher parse failed for chunk %s: %w", chunk.ChunkID, err)
	}

	seenEntities := make(map[string]struct{}, len(result.Entities))
	entities := make([]EntityCandidate, 0, len(result.Entities))
	for _, entityName := range result.Entities {
		trimmed := strings.TrimSpace(entityName)
		if trimmed == "" {
			continue
		}
		if _, exists := seenEntities[trimmed]; exists {
			continue
		}
		seenEntities[trimmed] = struct{}{}
		entities = append(entities, EntityCandidate{Name: trimmed, ChunkID: chunk.ChunkID})
	}

	edges := make([]EdgeCandidate, 0, len(result.Edges))
	for _, edge := range result.Edges {
		weight := edge.Weight
		if weight <= 0 {
			weight = 1.0
		}
		edges = append(edges, EdgeCandidate{
			Src:     edge.Src,
			Dst:     edge.Dst,
			RelType: edge.Rel,
			Weight:  weight,
			ChunkID: chunk.ChunkID,
		})
	}

	return entities, edges, nil
}

// Extract implements Grapher.
func (o *OllamaGrapher) Extract(ctx context.Context, chunks []Chunk) (*GraphExtraction, error) {
	if len(chunks) == 0 {
		return &GraphExtraction{}, nil
	}

	workers := o.MaxParallel
	if workers <= 0 {
		workers = defaultGrapherWorkers
	}
	if workers > len(chunks) {
		workers = len(chunks)
	}
	if workers <= 1 {
		allEntities := make([]EntityCandidate, 0)
		allEdges := make([]EdgeCandidate, 0)
		for _, chunk := range chunks {
			entities, edges, err := o.extractChunk(ctx, chunk)
			if err != nil {
				return nil, err
			}
			allEntities = append(allEntities, entities...)
			allEdges = append(allEdges, edges...)
		}
		return &GraphExtraction{Entities: allEntities, Edges: allEdges}, nil
	}

	type chunkResult struct {
		entities []EntityCandidate
		edges    []EdgeCandidate
		err      error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]chunkResult, len(chunks))
	jobs := make(chan int, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				entities, edges, err := o.extractChunk(ctx, chunks[idx])
				if err != nil {
					results[idx].err = err
					cancel()
					continue
				}
				results[idx].entities = entities
				results[idx].edges = edges
			}
		}()
	}

	go func() {
		for i := range chunks {
			select {
			case <-ctx.Done():
				break
			case jobs <- i:
			}
		}
		close(jobs)
	}()

	wg.Wait()

	allEntities := make([]EntityCandidate, 0)
	allEdges := make([]EdgeCandidate, 0)
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		allEntities = append(allEntities, results[i].entities...)
		allEdges = append(allEdges, results[i].edges...)
	}

	return &GraphExtraction{Entities: allEntities, Edges: allEdges}, nil
}
