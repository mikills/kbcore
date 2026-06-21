package kb

import "sort"

// MergeHybridRRF merges vector and BM25 result lists using Reciprocal Rank Fusion.
// rrfK=60 is the standard constant; final Distance is 1 - RRF_score so that
// lower values remain "better" (consistent with vector distance convention).
func MergeHybridRRF(vectorResults, bm25Results []QueryResult, topK int) []QueryResult {
	const rrfK = 60
	scores := make(map[string]float64, len(vectorResults)+len(bm25Results))
	first := make(map[string]QueryResult, len(vectorResults)+len(bm25Results))

	for rank, r := range vectorResults {
		scores[r.ID] += 1.0 / float64(rrfK+rank+1)
		if _, ok := first[r.ID]; !ok {
			first[r.ID] = r
		}
	}
	for rank, r := range bm25Results {
		scores[r.ID] += 1.0 / float64(rrfK+rank+1)
		if _, ok := first[r.ID]; !ok {
			first[r.ID] = r
		}
	}

	type scored struct {
		id    string
		score float64
	}
	ranked := make([]scored, 0, len(scores))
	for id, s := range scores {
		ranked = append(ranked, scored{id, s})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].id < ranked[j].id
	})
	if topK > 0 && len(ranked) > topK {
		ranked = ranked[:topK]
	}
	out := make([]QueryResult, 0, len(ranked))
	for _, s := range ranked {
		r := first[s.id]
		r.Distance = 1.0 - s.score
		out = append(out, r)
	}
	return out
}
