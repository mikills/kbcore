package codeindex

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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
		require.Contains(t, string(data), `codeindex_root=$(git rev-parse --show-toplevel`)
		require.Equal(t, filepath.Join(binDir, "codeindex"), gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.binary"))
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
		require.Contains(t, string(data), `codeindex_root=$(git rev-parse --show-toplevel`)
		require.Empty(t, gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.kb"))
		require.Empty(t, gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.index-key"))
	})

	t.Run("command settings are repository-local", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "minnow bin")
		writeExecutable(t, binary)
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{
			Root: root, KBID: "code-api", IndexKey: "api", Binary: binary,
		})
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-commit"))
		require.NoError(t, err)
		content := string(data)
		require.Contains(t, content, `CODEINDEX_REPO_ROOT="$codeindex_root"`)
		require.Contains(t, content, `"$codeindex_binary" "$@"`)
		require.Equal(t, binary, gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.binary"))
		require.Equal(t, "code-api", gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.kb"))
		require.Equal(t, "api", gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.index-key"))
	})

	t.Run("legacy minnow binary keeps the index subcommand", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "minnow")
		writeExecutable(t, binary)
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: binary})
		require.NoError(t, err)
		require.Equal(t, "index", gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.mode"))
	})

	t.Run("persists values without shell evaluation", func(t *testing.T) {
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
		require.Equal(t, config, gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.config"))
		require.Equal(t, "$(touch /tmp/codeindex-pwn)", gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.kb"))
	})

	t.Run("forced install runs before an existing early exit", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		record := filepath.Join(t.TempDir(), "calls")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeRecorder(t, binary, record)
		hook := filepath.Join(root, ".git", "hooks", "post-commit")
		require.NoError(t, os.WriteFile(hook, []byte("#!/bin/sh\nprintf '%s\\n' \"$1|$2|$3\" >> "+shellQuote(record)+"\nexit 0\n"), 0o755))
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: binary, Force: true})
		require.NoError(t, err)
		cmd := exec.Command(hook, "old", "new", "1")
		cmd.Dir = root
		require.NoError(t, cmd.Run())
		calls, err := os.ReadFile(record)
		require.NoError(t, err)
		root = canonicalPath(t, root)
		require.Contains(t, string(calls), root+"|refresh --root "+root)
		require.Contains(t, string(calls), "old|new|1")
		_, err = InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: binary})
		require.NoError(t, err)
		cmd = exec.Command(hook, "old", "new", "1")
		cmd.Dir = root
		require.NoError(t, cmd.Run())
		calls, err = os.ReadFile(record)
		require.NoError(t, err)
		require.Len(t, strings.Split(strings.TrimSpace(string(calls)), "\n"), 4)
	})

	t.Run("reinstall preserves configured identity overrides", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		_, err := InstallCodeIndexHooks(context.Background(), CodeHookOptions{
			Root: root, Binary: binary, KBID: "shared", IndexKey: "api",
		})
		require.NoError(t, err)
		_, err = InstallCodeIndexHooks(context.Background(), CodeHookOptions{Root: root, Binary: binary})
		require.NoError(t, err)
		require.Equal(t, "shared", gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.kb"))
		require.Equal(t, "api", gitOutputForTest(t, root, "config", "--local", "--get", "minnow.codeindex.index-key"))
	})

	t.Run("legacy reinstall requires explicit migration", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		hook := filepath.Join(root, ".git", "hooks", "post-commit")
		legacy := "#!/bin/sh\n" + codeHookStart + "\n'codeindex' refresh --kb 'shared' --index-key 'api' || true\n" + codeHookEnd + "\n"
		require.NoError(t, os.WriteFile(hook, []byte(legacy), 0o700))
		_, err := InstallCodeIndexHooks(context.Background(), CodeHookOptions{Root: root, Binary: binary})
		require.ErrorContains(t, err, "legacy Minnow hook detected")
		data, readErr := os.ReadFile(hook)
		require.NoError(t, readErr)
		require.Equal(t, legacy, string(data))
	})

	t.Run("forced install preserves private hook permissions", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		hook := filepath.Join(root, ".git", "hooks", "post-commit")
		require.NoError(t, os.WriteFile(hook, []byte("#!/bin/sh\nexit 0\n"), 0o600))
		_, err := InstallCodeIndexHooks(context.Background(), CodeHookOptions{Root: root, Binary: binary, Force: true})
		require.NoError(t, err)
		info, err := os.Stat(hook)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	t.Run("forced install rejects non-shell hooks", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		hook := filepath.Join(root, ".git", "hooks", "post-commit")
		require.NoError(t, os.WriteFile(hook, []byte("#!/usr/bin/env python3\nprint('ok')\n"), 0o755))
		_, err := InstallCodeIndexHooks(context.Background(), CodeHookOptions{Root: root, Binary: binary, Force: true})
		require.ErrorContains(t, err, "non-shell interpreter")
		data, readErr := os.ReadFile(hook)
		require.NoError(t, readErr)
		require.Equal(t, "#!/usr/bin/env python3\nprint('ok')\n", string(data))
	})

	t.Run("reinstall rejects a managed non-shell hook", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeExecutable(t, binary)
		hook := filepath.Join(root, ".git", "hooks", "post-commit")
		content := "#!/usr/bin/env python3\n" + codeHookStart + "\nprint('legacy')\n" + codeHookEnd + "\n"
		require.NoError(t, os.WriteFile(hook, []byte(content), 0o755))
		_, err := InstallCodeIndexHooks(context.Background(), CodeHookOptions{Root: root, Binary: binary, Force: true})
		require.ErrorContains(t, err, "non-shell interpreter")
		data, readErr := os.ReadFile(hook)
		require.NoError(t, readErr)
		require.Equal(t, content, string(data))
	})

	t.Run("shared worktree hook resolves the invoking worktree", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init", "-b", "main")
		runGit(t, root, "config", "user.email", "hooks@example.com")
		runGit(t, root, "config", "user.name", "Hooks")
		require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("main\n"), 0o644))
		runGit(t, root, "add", "file.txt")
		runGit(t, root, "commit", "-m", "initial")
		worktree := filepath.Join(t.TempDir(), "linked worktree")
		runGit(t, root, "worktree", "add", "-b", "feature", worktree)
		record := filepath.Join(t.TempDir(), "calls")
		binary := filepath.Join(t.TempDir(), "codeindex")
		writeRecorder(t, binary, record)
		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, Binary: binary})
		require.NoError(t, err)
		hook := filepath.Join(root, ".git", "hooks", "post-commit")
		for _, workRoot := range []string{root, worktree} {
			cmd := exec.Command(hook)
			cmd.Dir = workRoot
			require.NoError(t, cmd.Run())
		}
		calls, err := os.ReadFile(record)
		require.NoError(t, err)
		root = canonicalPath(t, root)
		worktree = canonicalPath(t, worktree)
		require.Contains(t, string(calls), root+"|refresh --root "+root)
		require.Contains(t, string(calls), worktree+"|refresh --root "+worktree)
	})

	t.Run("shared custom hook path keeps repository-local settings", func(t *testing.T) {
		ctx := context.Background()
		sharedHooks := filepath.Join(t.TempDir(), "shared hooks")
		require.NoError(t, os.MkdirAll(sharedHooks, 0o755))
		sharedBodyRecord := filepath.Join(t.TempDir(), "shared-body")
		require.NoError(t, os.WriteFile(
			filepath.Join(sharedHooks, "post-commit"),
			[]byte("#!/bin/sh\nprintf 'body\\n' >> "+shellQuote(sharedBodyRecord)+"\n"), 0o755,
		))
		roots := []string{t.TempDir(), t.TempDir()}
		records := []string{filepath.Join(t.TempDir(), "one"), filepath.Join(t.TempDir(), "two")}
		for i, root := range roots {
			runGit(t, root, "init")
			runGit(t, root, "config", "core.hooksPath", sharedHooks)
			binary := filepath.Join(t.TempDir(), "codeindex")
			writeRecorder(t, binary, records[i])
			_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{
				Root: root, Binary: binary, IndexKey: fmt.Sprintf("key-%d", i), Force: true,
			})
			require.NoError(t, err)
		}
		hook := filepath.Join(sharedHooks, "post-commit")
		for i, root := range roots {
			cmd := exec.Command(hook)
			cmd.Dir = root
			require.NoError(t, cmd.Run())
			calls, err := os.ReadFile(records[i])
			require.NoError(t, err)
			require.Contains(t, string(calls), "--index-key key-"+strconv.Itoa(i))
		}
		status, err := UninstallCodeIndexHooks(ctx, roots[0])
		require.NoError(t, err)
		require.False(t, status.Installed["post-commit"])
		status, err = CodeIndexHookStatus(ctx, roots[1])
		require.NoError(t, err)
		require.True(t, status.Installed["post-commit"])
		cmd := exec.Command(hook)
		cmd.Dir = roots[1]
		require.NoError(t, cmd.Run())
		cmd = exec.Command(hook)
		cmd.Dir = roots[0]
		require.NoError(t, cmd.Run())
		logPath := gitOutputForTest(t, roots[0], "rev-parse", "--git-path", "minnow-codeindex-hook.log")
		if !filepath.IsAbs(logPath) {
			logPath = filepath.Join(roots[0], logPath)
		}
		logData, err := os.ReadFile(logPath)
		require.NoError(t, err)
		require.Contains(t, string(logData), "hook is not configured")
		bodyData, err := os.ReadFile(sharedBodyRecord)
		require.NoError(t, err)
		require.Len(t, strings.Split(strings.TrimSpace(string(bodyData)), "\n"), 4)
	})

	t.Run("custom hook path preserves legacy blocks with unknown ownership", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		hooksDir := filepath.Join(t.TempDir(), "custom hooks")
		require.NoError(t, os.MkdirAll(hooksDir, 0o755))
		runGit(t, root, "config", "core.hooksPath", hooksDir)
		hook := filepath.Join(hooksDir, "post-commit")
		legacy := "#!/bin/sh\n" + codeHookStart + "\nCODEINDEX_REPO_ROOT='old' 'codeindex' refresh --root 'old' || true\n" + codeHookEnd + "\n"
		require.NoError(t, os.WriteFile(hook, []byte(legacy), 0o755))
		_, err := UninstallCodeIndexHooks(context.Background(), root)
		require.ErrorContains(t, err, "unknown repository ownership")
		data, readErr := os.ReadFile(hook)
		require.NoError(t, readErr)
		require.Equal(t, legacy, string(data))
	})

	t.Run("generated state is locally ignored", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		require.NoError(t, EnsureLocalStateIgnored(root))
		require.NoError(t, os.MkdirAll(filepath.Join(root, ".minnow"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, ".minnow", "state.json"), []byte("{}"), 0o644))
		status := gitOutputForTest(t, root, "status", "--short", "--untracked-files=all")
		require.NotContains(t, status, ".minnow")
	})

	t.Run("tracked generated state is rejected with remediation", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		require.NoError(t, os.MkdirAll(filepath.Join(root, ".minnow"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, ".minnow", "state.json"), []byte("{}"), 0o644))
		runGit(t, root, "add", "-f", ".minnow/state.json")
		err := EnsureLocalStateIgnored(root)
		require.ErrorContains(t, err, "git rm -r --cached .minnow")
	})
}

func writeExecutable(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755))
}

func writeRecorder(t *testing.T, path, record string) {
	t.Helper()
	script := "#!/bin/sh\nprintf '%s\\n' \"$CODEINDEX_REPO_ROOT|$*\" >> " + shellQuote(record) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
}

func canonicalPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)
	return resolved
}

func gitOutputForTest(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return strings.TrimSpace(string(output))
}
