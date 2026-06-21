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

	switch options.Mode {
	case SearchModeGraph:
		return queryGraphSearch(ctx, format, kbID, queryVec, options)
	case SearchModeAdaptive:
		return queryAdaptiveSearch(ctx, format, kbID, queryVec, options)
	case SearchModeBM25:
		return queryBM25Search(ctx, format, kbID, options)
	case SearchModeHybrid:
		return queryHybridSearch(ctx, format, kbID, queryVec, options)
	default:
		return queryVectorSearch(ctx, format, kbID, queryVec, options)
	}
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
		Options:  RagQueryOptions{TopK: options.TopK, MaxDistance: options.MaxDistance, Filter: options.Filter},
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
		Options:  GraphQueryOptions{TopK: options.TopK, MaxDistance: options.MaxDistance, Filter: options.Filter, Expansion: options.Expansion},
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
		Options:   RagQueryOptions{TopK: options.TopK, MaxDistance: options.MaxDistance, Filter: options.Filter},
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
