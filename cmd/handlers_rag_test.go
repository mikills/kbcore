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

func TestBuildIngestDocumentsRequiresIDsWhenPreChunked(t *testing.T) {
	graphEnabled := false
	_, _, _, err := buildIngestDocuments(ragIngestRequest{
		GraphEnabled: &graphEnabled,
		PreChunked:   true,
		Documents:    []ragIngestDocIn{{Text: "prepared chunk"}},
	})
	require.ErrorContains(t, err, "require non-empty ids")

	docs, ids, opts, err := buildIngestDocuments(ragIngestRequest{
		GraphEnabled: &graphEnabled,
		PreChunked:   true,
		Documents:    []ragIngestDocIn{{ID: "stable", Text: "prepared chunk"}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"stable"}, ids)
	require.Equal(t, []kb.Document{{ID: "stable", Text: "prepared chunk"}}, docs)
	require.NotNil(t, opts.GraphEnabled)
}
