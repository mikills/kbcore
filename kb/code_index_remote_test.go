package kb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterRemoteCodeSearchResultsUsesPreparedMetadata(t *testing.T) {
	results := []ExpandedResult{
		{
			ID: "one", Content: "first", Distance: 0.1,
			Metadata: map[string]any{
				"code_path": "cmd/main.go", "code_language": "go", "code_symbol": "main",
				"code_kind": "function", "code_start_line": float64(10), "code_end_line": float64(20),
			},
		},
		{ID: "not-code", Content: "ignored", Distance: 0.2},
	}

	filtered := filterRemoteCodeSearchResults(results, CodeSearchOptions{TopK: 10, Path: "cmd/", Language: "GO"})
	require.Equal(t, []CodeSearchResult{{
		ID: "one", Content: "first", Distance: 0.1, Path: "cmd/main.go", Language: "go",
		Symbol: "main", Kind: "function", StartLine: 10, EndLine: 20,
	}}, filtered)
}

func TestFilterCodeSearchResultsFallsBackToPreparedMetadataWithLegacyManifest(t *testing.T) {
	results := []ExpandedResult{{
		ID: "remote", Content: "new", Distance: 0.1,
		Metadata: map[string]any{"code_path": "new.go", "code_language": "go"},
	}}
	manifest := codeIndexManifest{Chunks: map[string]CodeChunkMetadata{"legacy": {Path: "old.go"}}}

	filtered := filterCodeSearchResultsWithMetadata(results, manifest, CodeSearchOptions{TopK: 10})
	require.Len(t, filtered, 1)
	require.Equal(t, "new.go", filtered[0].Path)
}
