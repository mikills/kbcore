package kb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// ExpansionOptions configures graph-based search expansion.
// Zero values are replaced with defaults.
type ExpansionOptions struct {
	SeedK               int
	Hops                int
	MaxNeighborsPerNode int
	Alpha               float64
	Decay               float64
	EdgeTypes           []string
	UseDuckPGQ          bool
	MaxEntityResults    int
	OfflineExt          bool
}

// ExpandedResult represents a blended result combining vector and graph scores.
type ExpandedResult struct {
	ID         string
	Content    string
	Distance   float64
	GraphScore float64
	Score      float64
}

// SearchMode controls which retrieval strategy is used.
//
//   - SearchModeVector: pure vector similarity search.
//   - SearchModeGraph: vector seed + BFS/PGQ graph expansion (true hybrid).
//   - SearchModeAdaptive: vector-first with graph fallback when top-1
//     similarity is below AdaptiveMinSim.
type SearchMode int

const (
	SearchModeVector SearchMode = iota
	SearchModeGraph
	SearchModeAdaptive
)

// SearchOptions configures search strategy selection.
type SearchOptions struct {
	Mode           SearchMode
	TopK           int
	MaxDistance    *float64
	Expansion      *ExpansionOptions
	AdaptiveMinSim float64
}

type edgeRow struct {
	Src    string
	Dst    string
	Weight float64
}

// Search performs vector, graph, or adaptive retrieval based on options.
func (k *KB) Search(ctx context.Context, kbID string, queryVec []float32, opts *SearchOptions) ([]ExpandedResult, error) {
	if k.ArtifactFormat == nil {
		return nil, ErrArtifactFormatNotConfigured
	}

	options := normalizeSearchOptions(opts)
	if options.TopK <= 0 {
		return nil, fmt.Errorf("%w: top_k must be > 0", ErrInvalidQueryRequest)
	}

	ragReq := RagQueryRequest{
		KBID:     kbID,
		QueryVec: queryVec,
		Options: RagQueryOptions{
			TopK:        options.TopK,
			MaxDistance: options.MaxDistance,
		},
	}

	graphReq := GraphQueryRequest{
		KBID:     kbID,
		QueryVec: queryVec,
		Options: GraphQueryOptions{
			TopK:        options.TopK,
			MaxDistance: options.MaxDistance,
			Expansion:   options.Expansion,
		},
	}

	switch options.Mode {
	case SearchModeGraph:
		return k.ArtifactFormat.QueryGraph(ctx, graphReq)
	case SearchModeAdaptive:
		vectorResults, err := k.ArtifactFormat.QueryRag(ctx, ragReq)
		if err != nil {
			return nil, err
		}
		if len(vectorResults) == 0 {
			return []ExpandedResult{}, nil
		}
		sim := 1.0 / (1.0 + vectorResults[0].Distance)
		if sim >= options.AdaptiveMinSim {
			return vectorResults, nil
		}
		return k.ArtifactFormat.QueryGraph(ctx, graphReq)
	default:
		return k.ArtifactFormat.QueryRag(ctx, ragReq)
	}
}

func searchExpandedWithDB(ctx context.Context, db *sql.DB, queryVec []float32, topK int, options ExpansionOptions) ([]ExpandedResult, error) {
	if err := ensureGraphQueryReady(ctx, db); err != nil {
		return nil, err
	}

	seeds, err := queryTopKWithDB(ctx, db, queryVec, options.SeedK)
	if err != nil {
		return nil, err
	}
	if len(seeds) == 0 {
		return []ExpandedResult{}, nil
	}

	entityScores, err := seedEntityScores(ctx, db, seeds)
	if err != nil {
		return nil, err
	}

	entityScores, err = expandEntityScores(ctx, db, entityScores, options)
	if err != nil {
		return nil, err
	}

	if options.MaxEntityResults > 0 && len(entityScores) > options.MaxEntityResults {
		entityScores = topNEntityScores(entityScores, options.MaxEntityResults)
	}

	return buildExpandedResults(ctx, db, queryVec, topK, seeds, entityScores, options.Alpha)
}

func mergeExpandedShardResults(shardResults [][]ExpandedResult, topK int) []ExpandedResult {
	if topK <= 0 {
		return []ExpandedResult{}
	}
	flattened := make([]ExpandedResult, 0)
	for _, shard := range shardResults {
		flattened = append(flattened, shard...)
	}
	if len(flattened) == 0 {
		return []ExpandedResult{}
	}

	sort.SliceStable(flattened, func(i, j int) bool {
		if flattened[i].Score == flattened[j].Score {
			if flattened[i].Distance == flattened[j].Distance {
				return flattened[i].ID < flattened[j].ID
			}
			return flattened[i].Distance < flattened[j].Distance
		}
		return flattened[i].Score > flattened[j].Score
	})

	if topK > len(flattened) {
		topK = len(flattened)
	}
	return flattened[:topK]
}

func seedEntityScores(ctx context.Context, db *sql.DB, seeds []QueryResult) (map[string]float64, error) {
	seedDocIDs := extractSeedDocIDs(seeds)
	seedEntities, err := queryEntitiesForDocs(ctx, db, seedDocIDs)
	if err != nil {
		return nil, err
	}
	entityScores := make(map[string]float64, len(seedEntities))
	for id, weight := range seedEntities {
		entityScores[id] = weight
	}
	return entityScores, nil
}

func expandEntityScores(ctx context.Context, db *sql.DB, scores map[string]float64, options ExpansionOptions) (map[string]float64, error) {
	if options.Hops <= 0 || len(scores) == 0 {
		return scores, nil
	}
	if options.UseDuckPGQ {
		return expandEntityScoresDuckPGQ(ctx, db, scores, options)
	}
	return expandEntityScoresBFS(ctx, db, scores, options)
}

func expandEntityScoresBFS(ctx context.Context, db *sql.DB, scores map[string]float64, options ExpansionOptions) (map[string]float64, error) {
	frontier := copyFloatMap(scores)
	for hop := 0; hop < options.Hops; hop++ {
		sources := mapKeys(frontier)
		if len(sources) == 0 {
			break
		}

		edges, err := queryEdgesBySources(ctx, db, sources, options.EdgeTypes, options.MaxNeighborsPerNode)
		if err != nil {
			return nil, err
		}
		if len(edges) == 0 {
			break
		}

		nextFrontier := make(map[string]float64)
		for _, edge := range edges {
			if edge.Weight <= 0 {
				continue
			}
			base := frontier[edge.Src]
			if base <= 0 {
				continue
			}
			nextWeight := base * edge.Weight * options.Decay
			if nextWeight <= 0 {
				continue
			}
			scores[edge.Dst] += nextWeight
			nextFrontier[edge.Dst] += nextWeight
		}

		if len(nextFrontier) == 0 {
			break
		}
		frontier = nextFrontier
	}

	return scores, nil
}

func buildExpandedResults(ctx context.Context, db *sql.DB, queryVec []float32, topK int, seeds []QueryResult, entityScores map[string]float64, alpha float64) ([]ExpandedResult, error) {
	docGraphScore, err := queryDocsForEntities(ctx, db, entityScores)
	if err != nil {
		return nil, err
	}

	candidateIDs := candidateIDsFromSeeds(seeds, docGraphScore)
	docMatches, err := queryDocMatchesForIDs(ctx, db, queryVec, candidateIDs)
	if err != nil {
		return nil, err
	}
	if len(docMatches) == 0 {
		return []ExpandedResult{}, nil
	}

	var maxGraphScore float64
	for _, score := range docGraphScore {
		if score > maxGraphScore {
			maxGraphScore = score
		}
	}

	results := make([]ExpandedResult, 0, len(docMatches))
	for id, match := range docMatches {
		graph := docGraphScore[id]
		graphNorm := 0.0
		if maxGraphScore > 0 {
			graphNorm = graph / maxGraphScore
		}
		sim := 1.0 / (1.0 + match.Distance)
		score := alpha*sim + (1.0-alpha)*graphNorm
		results = append(results, ExpandedResult{
			ID:         id,
			Content:    match.Content,
			Distance:   match.Distance,
			GraphScore: graph,
			Score:      score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			if results[i].Distance == results[j].Distance {
				return results[i].ID < results[j].ID
			}
			return results[i].Distance < results[j].Distance
		}
		return results[i].Score > results[j].Score
	})

	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

func candidateIDsFromSeeds(seeds []QueryResult, docGraphScore map[string]float64) []string {
	candidateSet := make(map[string]struct{}, len(seeds)+len(docGraphScore))
	for _, seed := range seeds {
		candidateSet[seed.ID] = struct{}{}
	}
	for id := range docGraphScore {
		candidateSet[id] = struct{}{}
	}

	candidateIDs := make([]string, 0, len(candidateSet))
	for id := range candidateSet {
		candidateIDs = append(candidateIDs, id)
	}
	return candidateIDs
}

func extractSeedDocIDs(seeds []QueryResult) []string {
	seedDocIDs := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		seedDocIDs = append(seedDocIDs, seed.ID)
	}
	return seedDocIDs
}

func copyFloatMap(src map[string]float64) map[string]float64 {
	dst := make(map[string]float64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func normalizeExpansionOptions(topK int, opts *ExpansionOptions) ExpansionOptions {
	defaults := ExpansionOptions{
		SeedK:               max(topK, 10),
		Hops:                2,
		MaxNeighborsPerNode: 25,
		Alpha:               0.7,
		Decay:               0.7,
		MaxEntityResults:    1000,
	}
	if opts == nil {
		return defaults
	}

	normalized := *opts
	if normalized.SeedK <= 0 {
		normalized.SeedK = defaults.SeedK
	}
	if normalized.Hops < 0 {
		normalized.Hops = defaults.Hops
	}
	if normalized.MaxNeighborsPerNode <= 0 {
		normalized.MaxNeighborsPerNode = defaults.MaxNeighborsPerNode
	}
	if normalized.Alpha < 0 || normalized.Alpha > 1 {
		normalized.Alpha = defaults.Alpha
	}
	if normalized.Decay <= 0 || normalized.Decay > 1 {
		normalized.Decay = defaults.Decay
	}
	if normalized.MaxEntityResults <= 0 {
		normalized.MaxEntityResults = defaults.MaxEntityResults
	}

	return normalized
}

func normalizeSearchOptions(opts *SearchOptions) SearchOptions {
	defaults := SearchOptions{
		Mode:           SearchModeVector,
		AdaptiveMinSim: 0.35,
	}
	if opts == nil {
		return defaults
	}
	normalized := *opts
	if normalized.Mode != SearchModeGraph && normalized.Mode != SearchModeAdaptive {
		normalized.Mode = defaults.Mode
	}
	if normalized.AdaptiveMinSim <= 0 || normalized.AdaptiveMinSim > 1 {
		normalized.AdaptiveMinSim = defaults.AdaptiveMinSim
	}
	return normalized
}
