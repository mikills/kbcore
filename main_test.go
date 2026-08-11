package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/mikills/minnow/kb/config"
	"github.com/stretchr/testify/require"
)

func TestLegacyCodeIndexCommandDoesNotStartMinnowServer(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	code, handled := runTopLevelCommand(context.Background(), []string{"index", "refresh"}, slog.Default())
	require.True(t, handled)
	require.Equal(t, 1, code)
}

func TestLegacyCodeIndexArgsPassSiblingBinaryToHookInstall(t *testing.T) {
	require.Equal(
		t,
		[]string{"index", "hooks", "install", "--binary", "/tmp/codeindex"},
		legacyCodeIndexArgs("/tmp/codeindex", []string{"hooks", "install"}),
	)
	require.Equal(
		t,
		[]string{"index", "hooks", "install", "--binary", "custom"},
		legacyCodeIndexArgs("/tmp/codeindex", []string{"hooks", "install", "--binary", "custom"}),
	)
}

func TestConfigInit(t *testing.T) {
	t.Run("template writes and refuses overwrite without force", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "test-key")
		path := filepath.Join(t.TempDir(), "minnow.yaml")

		require.NoError(t, writeConfigTemplate(path, devOpenAIConfigTemplate(), false))
		require.FileExists(t, path)

		cfg, err := config.Load(path)
		require.NoError(t, err)
		require.Equal(t, "openai_compatible", cfg.Embedder.Provider)
		require.Equal(t, "https://api.openai.com/v1", cfg.Embedder.OpenAICompatible.BaseURL)
		require.Equal(t, "text-embedding-3-small", cfg.Embedder.OpenAICompatible.Model)
		require.Equal(t, "test-key", cfg.Embedder.OpenAICompatible.Token)
		require.True(t, cfg.MCP.Enabled)
		require.Contains(t, cfg.MCP.Transports, "stdio")

		err = writeConfigTemplate(path, devOpenAIConfigTemplate(), false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")

		require.NoError(t, writeConfigTemplate(path, devOpenAIConfigTemplate(), true))
	})

	t.Run("command writes user config", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		t.Setenv("OPENAI_API_KEY", "test-key")

		code := runConfigInit([]string{"dev-openai"})
		require.Equal(t, 0, code)

		path, err := config.UserConfigPath()
		require.NoError(t, err)
		require.FileExists(t, path)
		_, err = os.Stat(filepath.Dir(path))
		require.NoError(t, err)

		cfg, err := config.Load(path)
		require.NoError(t, err)
		require.Equal(t, "openai_compatible", cfg.Embedder.Provider)
	})
}
