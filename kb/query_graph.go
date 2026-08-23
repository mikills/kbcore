package kb

import (
	"context"
	"fmt"

	"github.com/mikills/minnow/kb/search"
)

type ExpansionOptions = search.ExpansionOptions

type ExpandedResult = search.ExpandedResult

type SearchMode = search.Mode

const (
	SearchModeVector   = search.ModeVector
	SearchModeGraph    = search.ModeGraph
	SearchModeAdaptive = search.ModeAdaptive
	SearchModeBM25     = search.ModeBM25
	SearchModeHybrid   = search.ModeHybrid
)

type SearchOptions = search.Options

type EdgeRow = search.EdgeRow

func (k *KB) FetchVectors(ctx context.Context, kbID string, ids []string) ([]VectorRecord, error) {
	format, err := k.resolveSearchFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}
	return format.FetchVectors(ctx, kbID, ids)
}

func (k *KB) SearchRaw(ctx context.Context, kbID string, queryVec []float32, topK int, filter *search.FilterExpr) ([]QueryResult, error) {
	format, err := k.resolveSearchFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}
	if topK <= 0 {
		return nil, fmt.Errorf("%w: top_k must be > 0", ErrInvalidQueryRequest)
	}
	expanded, err := format.QueryRag(ctx, RagQueryRequest{
		KBID:     kbID,
		QueryVec: queryVec,
		Options:  RagQueryOptions{TopK: topK, Filter: filter},
	})
	if err != nil {
		return nil, err
	}
	out := make([]QueryResult, 0, len(expanded))
	for _, r := range expanded {
		out = append(out, QueryResult{ID: r.ID, Content: r.Content, Distance: r.Distance, MediaRefs: r.MediaRefs, Metadata: r.Metadata})
	}
	return out, nil
}

func (k *KB) Search(
	ctx context.Context,
	kbID string,
	queryVec []float32,
	opts *SearchOptions,
) ([]ExpandedResult, error) {
	format, err := k.resolveSearchFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}

	options := search.NormalizeOptions(opts)
	if options.TopK <= 0 {
		return nil, fmt.Errorf("%w: top_k must be > 0", ErrInvalidQueryRequest)
	}

	if options.ScopeID != "" {
		scope, err := k.GetScope(ctx, kbID, options.ScopeID)
		if err != nil {
			return nil, err
		}
		options.DocumentIDs = scope.DocumentIDs
		expansion := search.NormalizeExpansionOptions(options.TopK, options.Expansion)
		expansion.Hops = 0
		options.Expansion = &expansion
	}

	var results []ExpandedResult
	switch options.Mode {
	case SearchModeGraph:
		results, err = queryGraphSearch(ctx, format, kbID, queryVec, options)
	case SearchModeAdaptive:
		results, err = queryAdaptiveSearch(ctx, format, kbID, queryVec, options)
	case SearchModeBM25:
		results, err = queryBM25Search(ctx, format, kbID, options)
	case SearchModeHybrid:
		results, err = queryHybridSearch(ctx, format, kbID, queryVec, options)
	default:
		results, err = queryVectorSearch(ctx, format, kbID, queryVec, options)
	}
	return results, err
}

func queryGraphSearch(
	ctx context.Context,
	format ArtifactFormat,
	kbID string,
	queryVec []float32,
	options SearchOptions,
) ([]ExpandedResult, error) {
	graphReq := graphQueryRequest(kbID, queryVec, options)
	if err := ValidateGraphQueryRequest(graphReq); err != nil {
		return nil, err
	}
	return format.QueryGraph(ctx, graphReq)
}

func queryAdaptiveSearch(
	ctx context.Context,
	format ArtifactFormat,
	kbID string,
	queryVec []float32,
	options SearchOptions,
) ([]ExpandedResult, error) {
	vectorResults, err := queryVectorSearch(ctx, format, kbID, queryVec, options)
	if err != nil || len(vectorResults) == 0 {
		return vectorResults, err
	}
	if float64(1)/(float64(1)+vectorResults[0].Distance) >= options.AdaptiveMinSim {
		return vectorResults, nil
	}
	return queryGraphSearch(ctx, format, kbID, queryVec, options)
}

func queryVectorSearch(
	ctx context.Context,
	format ArtifactFormat,
	kbID string,
	queryVec []float32,
	options SearchOptions,
) ([]ExpandedResult, error) {
	ragReq := RagQueryRequest{
		KBID:     kbID,
		QueryVec: queryVec,
		Options: RagQueryOptions{
			TopK: options.TopK, MaxDistance: options.MaxDistance, Filter: options.Filter,
			DocumentIDs: options.DocumentIDs,
		},
	}
	if err := ValidateRagQueryRequest(ragReq); err != nil {
		return nil, err
	}
	return format.QueryRag(ctx, ragReq)
}

func graphQueryRequest(kbID string, queryVec []float32, options SearchOptions) GraphQueryRequest {
	return GraphQueryRequest{
		KBID:     kbID,
		QueryVec: queryVec,
		Options: GraphQueryOptions{
			TopK: options.TopK, MaxDistance: options.MaxDistance, Filter: options.Filter,
			Expansion: options.Expansion, DocumentIDs: options.DocumentIDs,
		},
	}
}

func queryBM25Search(
	ctx context.Context,
	format ArtifactFormat,
	kbID string,
	options SearchOptions,
) ([]ExpandedResult, error) {
	if options.QueryText == "" {
		return nil, fmt.Errorf("%w: query_text is required for bm25 mode", ErrInvalidQueryRequest)
	}
	return format.QueryBM25(ctx, BM25QueryRequest{
		KBID:      kbID,
		QueryText: options.QueryText,
		Options: RagQueryOptions{
			TopK: options.TopK, MaxDistance: options.MaxDistance, Filter: options.Filter,
			DocumentIDs: options.DocumentIDs,
		},
	})
}

func queryHybridSearch(
	ctx context.Context,
	format ArtifactFormat,
	kbID string,
	queryVec []float32,
	options SearchOptions,
) ([]ExpandedResult, error) {
	if options.QueryText == "" {
		return nil, fmt.Errorf("%w: query_text is required for hybrid mode", ErrInvalidQueryRequest)
	}

	vectorResults, err := queryVectorSearch(ctx, format, kbID, queryVec, options)
	if err != nil {
		return nil, err
	}
	bm25Results, err := queryBM25Search(ctx, format, kbID, options)
	if err != nil {
		return nil, err
	}

	// Convert ExpandedResult slices to QueryResult for RRF merging.
	vecQR := make([]QueryResult, len(vectorResults))
	for i, r := range vectorResults {
		vecQR[i] = QueryResult{ID: r.ID, Content: r.Content, Distance: r.Distance, MediaRefs: r.MediaRefs, Metadata: r.Metadata}
	}
	bm25QR := make([]QueryResult, len(bm25Results))
	for i, r := range bm25Results {
		bm25QR[i] = QueryResult{ID: r.ID, Content: r.Content, Distance: r.Distance, MediaRefs: r.MediaRefs, Metadata: r.Metadata}
	}

	merged := MergeHybridRRF(vecQR, bm25QR, options.TopK)
	return ExpandedFromVector(merged), nil
}

func NormalizeExpansionOptions(topK int, opts *ExpansionOptions) ExpansionOptions {
	return search.NormalizeExpansionOptions(topK, opts)
}
