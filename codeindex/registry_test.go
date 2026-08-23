package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	root := t.TempDir()
	registry, err := loadCodebaseRegistry(root)
	require.NoError(t, err)
	registry.Indexes["main"] = codebaseRegistryEntry{KBID: "repo", ScopeID: "scope-main", Root: "."}
	require.NoError(t, saveCodebaseRegistry(root, registry))

	registry, err = loadCodebaseRegistry(root)
	require.NoError(t, err)
	require.Equal(t, "scope-main", registry.Indexes["main"].ScopeID)
	registry.Indexes["feature"] = codebaseRegistryEntry{KBID: "repo", ScopeID: "scope-feature", Root: "."}
	delete(registry.Indexes, "main")
	require.NoError(t, saveCodebaseRegistry(root, registry))

	registry, err = loadCodebaseRegistry(root)
	require.NoError(t, err)
	require.NotContains(t, registry.Indexes, "main")
	require.Equal(t, "scope-feature", registry.Indexes["feature"].ScopeID)
}
