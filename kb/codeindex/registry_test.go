package codeindex

import (
	"os"
	"os/exec"
	"path/filepath"
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

func TestRepositoryID(t *testing.T) {
	root := t.TempDir()
	run := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	run(root, "init", "-b", "main")
	run(root, "config", "user.email", "codeindex@example.com")
	run(root, "config", "user.name", "Code Index")
	run(root, "commit", "--allow-empty", "-m", "initial")
	worktree := filepath.Join(t.TempDir(), "feature")
	run(root, "worktree", "add", "-b", "feature", worktree)

	require.Equal(t, CodeRepoID(root), CodeRepoID(worktree))
	require.Len(t, CodeRepoID(root), 8)
}

func TestSelection(t *testing.T) {
	root := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	remote := "https://example.com/acme/belter.git"
	run("init", "-b", "main")
	run("remote", "add", "origin", remote)

	selection, err := ResolveSelection(root, "", "")
	require.NoError(t, err)
	repoID := shortIdentity(remote)
	require.Equal(t, "main-"+shortIdentity("main"), selection.IndexKey)
	require.Equal(t, "code-belter-repository-"+repoID+"-"+shortIdentity("."), selection.KBID)
	require.Equal(t, "codeindex-"+identityHash("main\x00."), selection.ScopeID)
}

func TestWorktreeSelection(t *testing.T) {
	root := t.TempDir()
	run := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	run(root, "init", "-b", "main")
	run(root, "config", "user.email", "codeindex@example.com")
	run(root, "config", "user.name", "Code Index")
	run(root, "commit", "--allow-empty", "-m", "initial")
	worktree := filepath.Join(t.TempDir(), "different-name")
	run(root, "worktree", "add", "-b", "feature", worktree)

	main, err := ResolveSelection(root, "", "")
	require.NoError(t, err)
	feature, err := ResolveSelection(worktree, "", "")
	require.NoError(t, err)
	require.Equal(t, main.KBID, feature.KBID)
	require.NotEqual(t, main.ScopeID, feature.ScopeID)
}

func TestSeparateGit(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "primary")
	metadata := filepath.Join(base, "metadata")
	run := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	run(base, "init", "-b", "main", "--separate-git-dir", metadata, root)
	run(root, "config", "user.email", "codeindex@example.com")
	run(root, "config", "user.name", "Code Index")
	run(root, "commit", "--allow-empty", "-m", "initial")
	worktree := filepath.Join(base, "feature-name")
	run(root, "worktree", "add", "-b", "feature", worktree)

	main, err := ResolveSelection(root, "", "")
	require.NoError(t, err)
	mappingDir := filepath.Join(metadata, "minnow", "codeindex")
	require.NoError(t, os.MkdirAll(mappingDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(mappingDir, "repository-root"), []byte(root+"\n"), 0o600))
	feature, err := ResolveSelection(worktree, "", "")
	require.NoError(t, err)
	require.Equal(t, main.KBID, feature.KBID)
	primary, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	require.Equal(t, shortIdentity(primary), CodeRepoID(worktree))
}
