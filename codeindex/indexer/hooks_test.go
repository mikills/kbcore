package codeindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHooks(t *testing.T) {
	t.Run("rejects unavailable binary before changing hooks", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: filepath.Join(t.TempDir(), "missing-codeindex")})
		require.ErrorContains(t, err, "require executable")
		require.NoFileExists(t, filepath.Join(root, ".git", "hooks", "post-commit"))
	})

	t.Run("install and uninstall", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binDir := t.TempDir()
		writeExecutable(t, filepath.Join(binDir, "codeindex"))
		t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
		status, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, KBID: "code"})
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-commit"))
		require.NoError(t, err)
		require.Contains(t, string(data), shellQuote(filepath.Join(binDir, "codeindex"))+" refresh")
		for _, hook := range CodeHookNames {
			require.True(t, status.Installed[hook], hook)
		}
		status, err = UninstallCodeIndexHooks(ctx, root)
		require.NoError(t, err)
		for _, hook := range CodeHookNames {
			require.False(t, status.Installed[hook], hook)
		}
	})

	t.Run("default hook derives branch identity at refresh time", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: binary})
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-checkout"))
		require.NoError(t, err)
		content := string(data)
		require.NotContains(t, content, "--kb")
		require.NotContains(t, content, "--index-key")
	})

	t.Run("command arguments are quoted", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "minnow bin")
		writeExecutable(t, binary)
		_, err := InstallCodeIndexHooks(
			ctx,
			CodeHookOptions{Root: root, KBID: "code-api", IndexKey: "api", Binary: binary},
		)
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-commit"))
		require.NoError(t, err)
		content := string(data)
		require.Contains(t, content, `CODEINDEX_REPO_ROOT=`)
		require.Contains(t, content, shellQuote(binary)+" refresh")
		require.Contains(t, content, `--kb 'code-api'`)
		require.Contains(t, content, `--index-key 'api'`)
		require.Contains(t, content, `--yes`)
	})

	t.Run("legacy minnow binary keeps the index subcommand", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "minnow")
		writeExecutable(t, binary)
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: binary})
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-commit"))
		require.NoError(t, err)
		require.Contains(t, string(data), shellQuote(binary)+" index refresh")
	})

	t.Run("shell quotes explicit values and persists config", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		config := filepath.Join(t.TempDir(), "config file.yaml")
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{
			Root: root, Binary: binary, KBID: "$(touch /tmp/codeindex-pwn)", Config: config,
		})
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-commit"))
		require.NoError(t, err)
		content := string(data)
		require.Contains(t, content, "--config "+shellQuote(config))
		require.Contains(t, content, "--kb "+shellQuote("$(touch /tmp/codeindex-pwn)"))
	})
}

func writeExecutable(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755))
}
