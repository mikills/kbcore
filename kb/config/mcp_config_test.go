package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMCPConfig(t *testing.T) {
	t.Run("defaults_when_enabled", func(t *testing.T) {
		cfg := loadMCPTestConfig(t, "mcp:\n  enabled: true\n")
		require.True(t, cfg.MCP.Enabled)
		require.ElementsMatch(t, []string{"stdio", "http"}, cfg.MCP.Transports)
		require.Equal(t, "/mcp", cfg.MCP.HTTPPath)
		require.Equal(t, 30*time.Second, cfg.MCP.DefaultSyncTimeout.AsDuration())
		require.Equal(t, 2*time.Minute, cfg.MCP.MaxSyncTimeout.AsDuration())
		require.Equal(t, 30*time.Minute, cfg.MCP.HTTPSessionTimeout.AsDuration())
		require.Equal(t, 128, cfg.MCP.HTTPMaxSessions)
		require.True(t, cfg.MCPHTTPStateless())
		require.False(t, cfg.MCP.AllowDestructive)
		require.False(t, cfg.MCP.AllowAdmin)
	})

	t.Run("stateful_http_can_be_enabled", func(t *testing.T) {
		cfg := loadMCPTestConfig(t, "mcp:\n  enabled: true\n  http_stateless: false\n")
		require.False(t, cfg.MCPHTTPStateless())
	})

	t.Run("sync_requires_indexing", func(t *testing.T) {
		_, err := loadMCPTestConfigErr(t, "mcp:\n  enabled: true\n  allow_sync_indexing: true\n")
		require.ErrorContains(t, err, "mcp.allow_sync_indexing requires mcp.allow_indexing")
	})

	t.Run("read_only_blocks_mutating_flags", func(t *testing.T) {
		_, err := loadMCPTestConfigErr(t, "mcp:\n  enabled: true\n  read_only: true\n  allow_indexing: true\n")
		require.ErrorContains(t, err, "mcp.read_only cannot be combined")
	})

	t.Run("read only blocks destructive", func(t *testing.T) {
		_, err := loadMCPTestConfigErr(t, "mcp:\n  enabled: true\n  read_only: true\n  allow_destructive: true\n")
		require.ErrorContains(t, err, "mcp.read_only cannot be combined")
	})

	t.Run("invalid_transport", func(t *testing.T) {
		_, err := loadMCPTestConfigErr(t, "mcp:\n  enabled: true\n  transports: [websocket]\n")
		require.ErrorContains(t, err, "unsupported transport")
	})

	t.Run("invalid session timeout", func(t *testing.T) {
		_, err := loadMCPTestConfigErr(t, "mcp:\n  enabled: true\n  http_session_timeout: 500ms\n")
		require.ErrorContains(t, err, "mcp.http_session_timeout")
	})
}

func loadMCPTestConfig(t *testing.T, extra string) *Config {
	t.Helper()
	cfg, err := loadMCPTestConfigErr(t, extra)
	require.NoError(t, err)
	return cfg
}

func loadMCPTestConfigErr(t *testing.T, extra string) (*Config, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "minnow.yaml")
	data := "embedder:\n  provider: local\n  local:\n    dim: 32\n" + extra
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))
	return Load(path)
}
