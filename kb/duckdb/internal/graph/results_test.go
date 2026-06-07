package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestBuildExpandedResults(t *testing.T) {
	got := BuildExpandedResults(ExpandInput{
		TopK: 1,
		DocMatches: map[string]DocMatch{
			"a": {Content: "a", Distance: 0.1},
			"b": {Content: "b", Distance: 0.2, Metadata: map[string]any{"tag": "winner"}},
		},
		DocGraphScore: map[string]float64{"a": 1, "b": 2},
		Alpha:         0.5,
	})
	require.Len(t, got, 1)
	require.Equal(t, "b", got[0].ID)
	require.Equal(t, map[string]any{"tag": "winner"}, got[0].Metadata)
}

func TestMergeShardResults(t *testing.T) {
	got := MergeShardResults([][]kb.ExpandedResult{{{ID: "a", Score: 1}}, {{ID: "b", Score: 2}}}, 1)
	require.Len(t, got, 1)
	require.Equal(t, "b", got[0].ID)
}
