package graph

import (
	"maps"
	"sort"

	kb "github.com/mikills/minnow/kb"
)

type DocMatch struct {
	Content   string
	Distance  float64
	MediaRefs []kb.ChunkMediaRef
	Metadata  map[string]any
}

type ExpandInput struct {
	TopK          int
	Seeds         []kb.QueryResult
	DocMatches    map[string]DocMatch
	DocGraphScore map[string]float64
	Alpha         float64
}

func BuildExpandedResults(input ExpandInput) []kb.ExpandedResult {
	if len(input.DocMatches) == 0 {
		return []kb.ExpandedResult{}
	}
	results := expandedResultsFromMatches(
		input.DocMatches,
		input.DocGraphScore,
		maxFloat64Value(input.DocGraphScore),
		input.Alpha,
	)
	sortExpandedResults(results)
	return Limit(results, input.TopK)
}

func MergeShardResults(shardResults [][]kb.ExpandedResult, topK int) []kb.ExpandedResult {
	if topK <= 0 {
		return []kb.ExpandedResult{}
	}
	flattened := make([]kb.ExpandedResult, 0)
	for _, shard := range shardResults {
		flattened = append(flattened, shard...)
	}
	if len(flattened) == 0 {
		return []kb.ExpandedResult{}
	}
	sort.SliceStable(flattened, func(i, j int) bool { return Less(flattened[i], flattened[j]) })
	return Limit(flattened, topK)
}

func CandidateIDs(seeds []kb.QueryResult, docGraphScore map[string]float64) []string {
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

func SeedDocIDs(seeds []kb.QueryResult) []string {
	seedDocIDs := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		seedDocIDs = append(seedDocIDs, seed.ID)
	}
	return seedDocIDs
}

func CopyFloatMap(src map[string]float64) map[string]float64 {
	dst := make(map[string]float64, len(src))
	maps.Copy(dst, src)
	return dst
}

func expandedResultsFromMatches(
	docMatches map[string]DocMatch,
	docGraphScore map[string]float64,
	maxGraphScore float64,
	alpha float64,
) []kb.ExpandedResult {
	results := make([]kb.ExpandedResult, 0, len(docMatches))
	for id, match := range docMatches {
		results = append(results, expandedResultFromMatch(id, match, docGraphScore[id], maxGraphScore, alpha))
	}
	return results
}

func expandedResultFromMatch(
	id string,
	match DocMatch,
	graph float64,
	maxGraphScore float64,
	alpha float64,
) kb.ExpandedResult {
	graphNorm := 0.0
	if maxGraphScore > 0 {
		graphNorm = graph / maxGraphScore
	}
	sim := 1.0 / (1.0 + match.Distance)
	return kb.ExpandedResult{
		ID:         id,
		Content:    match.Content,
		Distance:   match.Distance,
		GraphScore: graph,
		Score:      alpha*sim + (1.0-alpha)*graphNorm,
		MediaRefs:  match.MediaRefs,
		Metadata:   match.Metadata,
	}
}

func maxFloat64Value(values map[string]float64) float64 {
	var maxValue float64
	for _, value := range values {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func sortExpandedResults(results []kb.ExpandedResult) {
	sort.Slice(results, func(i, j int) bool { return Less(results[i], results[j]) })
}

func Less(left kb.ExpandedResult, right kb.ExpandedResult) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	if left.Distance != right.Distance {
		return left.Distance < right.Distance
	}
	return left.ID < right.ID
}

func Limit(results []kb.ExpandedResult, topK int) []kb.ExpandedResult {
	if len(results) > topK {
		return results[:topK]
	}
	return results
}
