package codeindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistryHelpers(t *testing.T) {
	t.Run("sanitize key", func(t *testing.T) {
		require.Equal(t, "api-index", SanitizeKey("api index"))
		require.Equal(t, "default", SanitizeKey(""))
	})

	t.Run("default kb id", func(t *testing.T) {
		require.Equal(t, "code-api", DefaultKBIDForIndexKey("api"))
	})

	t.Run("registry round trip", func(t *testing.T) {
		root := t.TempDir()
		registry := Registry{Indexes: map[string]RegistryEntry{"default": {KBID: "kb1", Root: "."}}}
		require.NoError(t, SaveRegistry(root, registry))
		loaded, err := LoadRegistry(root)
		require.NoError(t, err)
		require.Equal(t, "kb1", loaded.Indexes["default"].KBID)
	})
}
