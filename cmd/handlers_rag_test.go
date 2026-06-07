package cmd

import (
	"testing"

	"github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestRagQueryResultsIncludesMetadata(t *testing.T) {
	results := ragQueryResults([]kb.ExpandedResult{{
		ID:       "doc-1",
		Content:  "content",
		Distance: 0.1,
		Metadata: map[string]any{"tenant": "t1", "rank": float64(1)},
	}}, false)

	require.Len(t, results, 1)
	require.Equal(t, map[string]any{"tenant": "t1", "rank": float64(1)}, results[0].Metadata)
}
