package codeindex

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var CodeHookNames = []string{"post-commit", "post-checkout", "post-merge", "post-rewrite"}

const (
	codeHookStart = "# >>> minnow code index >>>"
	codeHookEnd   = "# <<< minnow code index <<<"
)

type CodeHookOptions struct {
	Root     string
	KBID     string
	IndexKey string
	Binary   string
	Config   string
	Force    bool
}

type CodeHookStatus struct {
	Root      string            `json:"root"`
	HooksDir  string            `json:"hooks_dir"`
	Installed map[string]bool   `json:"installed"`
	Paths     map[string]string `json:"paths"`
}

func InstallCodeIndexHooks(ctx context.Context, opts CodeHookOptions) (CodeHookStatus, error) {
	binary, kbID, indexKey := normalizeCodeHookOptions(opts)
	resolvedBinary, err := resolveCodeIndexBinary(binary)
	if err != nil {
		return CodeHookStatus{}, err
	}
	binary = resolvedBinary
	root, hooksDir, err := gitHooksDir(ctx, opts.Root)
	if err != nil {
		return CodeHookStatus{}, err
	}
	if err := EnsureLocalStateIgnored(root); err != nil {
		return CodeHookStatus{}, err
	}
	block := renderCodeHookBlock()
	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		return CodeHookStatus{}, err
	}
	if err := validateCodeHookInstall(hooksDir, block, opts.Force); err != nil {
		return CodeHookStatus{}, err
	}
	if legacyManagedHooksPresent(hooksDir) {
		if usesCustomHooksPath(ctx, root) {
			return CodeHookStatus{}, fmt.Errorf("custom hooks path contains a legacy Minnow block with unknown repository ownership; remove it manually before installing")
		}
		return CodeHookStatus{}, fmt.Errorf("legacy Minnow hook detected; run `codeindex hooks uninstall`, then reinstall with the original --kb and --index-key overrides")
	}
	kbID, indexKey, err = preserveConfiguredIdentity(ctx, root, kbID, indexKey)
	if err != nil {
		return CodeHookStatus{}, err
	}
	if err := saveCodeHookConfig(ctx, root, binary, kbID, indexKey, opts.Config); err != nil {
		return CodeHookStatus{}, err
	}
	for _, name := range CodeHookNames {
		if err := installCodeHook(hooksDir, name, block, opts.Force); err != nil {
			return CodeHookStatus{}, err
		}
	}
	return CodeIndexHookStatus(ctx, opts.Root)
}

func resolveCodeIndexBinary(binary string) (string, error) {
	resolved, err := exec.LookPath(binary)
	if err != nil {
		return "", fmt.Errorf("codeindex hooks require executable %q; install codeindex or pass --binary: %w", binary, err)
	}
	abs, err := filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("resolve codeindex executable: %w", err)
	}
	return abs, nil
}

func normalizeCodeHookOptions(opts CodeHookOptions) (string, string, string) {
	binary := strings.TrimSpace(opts.Binary)
	if binary == "" {
		binary = "codeindex"
	}
	kbID := strings.TrimSpace(opts.KBID)
	indexKey := strings.TrimSpace(opts.IndexKey)
	return binary, kbID, indexKey
}

func preserveConfiguredIdentity(ctx context.Context, root, kbID, indexKey string) (string, string, error) {
	var err error
	if kbID == "" {
		kbID, err = localGitConfig(ctx, root, "minnow.codeindex.kb")
		if err != nil {
			return "", "", err
		}
	}
	if indexKey == "" {
		indexKey, err = localGitConfig(ctx, root, "minnow.codeindex.index-key")
		if err != nil {
			return "", "", err
		}
	}
	return kbID, indexKey, nil
}

func localGitConfig(ctx context.Context, root, key string) (string, error) {
	output, err := exec.CommandContext(ctx, "git", "-C", root, "config", "--local", "--get", key).Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return "", nil
		}
		return "", fmt.Errorf("read codeindex hook setting %s: %w", key, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func validateCodeHookInstall(hooksDir, block string, force bool) error {
	for _, name := range CodeHookNames {
		data, err := os.ReadFile(filepath.Join(hooksDir, name))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if _, err := updatedCodeHookContent(name, string(data), block, force); err != nil {
			return err
		}
	}
	return nil
}

func installCodeHook(hooksDir string, name string, block string, force bool) error {
	path := filepath.Join(hooksDir, name)
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	mode := os.FileMode(0o755)
	if info, statErr := os.Stat(path); statErr == nil {
		mode = info.Mode().Perm() | 0o100
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	content, err := updatedCodeHookContent(name, string(data), block, force)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func updatedCodeHookContent(name string, content string, block string, force bool) (string, error) {
	if !hasShellShebang(content) {
		return "", fmt.Errorf("git hook %s uses a non-shell interpreter; cannot safely add the Minnow shell block", name)
	}
	if strings.Contains(content, codeHookStart) {
		return replaceManagedHookBlock(content, block), nil
	}
	if strings.TrimSpace(content) != "" && !force {
		return "", fmt.Errorf("git hook %s already exists; rerun with force to add the Minnow managed block", name)
	}
	return appendManagedHookBlock(content, block), nil
}

func hasShellShebang(content string) bool {
	if strings.TrimSpace(content) == "" || !strings.HasPrefix(content, "#!") {
		return true
	}
	firstLine, _, _ := strings.Cut(content, "\n")
	interpreter := filepath.Base(strings.Fields(firstLine)[0])
	if interpreter == "env" {
		fields := strings.Fields(firstLine)
		if len(fields) < 2 {
			return false
		}
		interpreter = filepath.Base(fields[len(fields)-1])
	}
	switch interpreter {
	case "sh", "bash", "dash", "ksh", "zsh":
		return true
	default:
		return false
	}
}

func appendManagedHookBlock(content string, block string) string {
	if strings.TrimSpace(content) == "" {
		return "#!/bin/sh\n" + block
	}
	if strings.HasPrefix(content, "#!") {
		if newline := strings.IndexByte(content, '\n'); newline >= 0 {
			return content[:newline+1] + block + content[newline+1:]
		}
		return content + "\n" + block
	}
	return "#!/bin/sh\n" + block + content
}

func UninstallCodeIndexHooks(ctx context.Context, root string) (CodeHookStatus, error) {
	resolvedRoot, hooksDir, err := gitHooksDir(ctx, root)
	if err != nil {
		return CodeHookStatus{}, err
	}
	if usesCustomHooksPath(ctx, resolvedRoot) {
		if legacyManagedHooksPresent(hooksDir) {
			return CodeHookStatus{}, fmt.Errorf("custom hooks path contains a legacy Minnow block with unknown repository ownership; remove it manually")
		}
		if err := clearCodeHookConfig(ctx, resolvedRoot); err != nil {
			return CodeHookStatus{}, err
		}
		return CodeIndexHookStatus(ctx, resolvedRoot)
	}
	for _, name := range CodeHookNames {
		path := filepath.Join(hooksDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return CodeHookStatus{}, err
		}
		updated := removeManagedHookBlock(string(data))
		if strings.TrimSpace(updated) == "" || strings.TrimSpace(updated) == "#!/bin/sh" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return CodeHookStatus{}, err
			}
			continue
		}
		if err := os.WriteFile(path, []byte(updated), 0o755); err != nil {
			return CodeHookStatus{}, err
		}
	}
	if err := clearCodeHookConfig(ctx, resolvedRoot); err != nil {
		return CodeHookStatus{}, err
	}
	return CodeIndexHookStatus(ctx, root)
}

func clearCodeHookConfig(ctx context.Context, root string) error {
	for _, key := range []string{
		"minnow.codeindex.binary", "minnow.codeindex.config", "minnow.codeindex.kb",
		"minnow.codeindex.index-key", "minnow.codeindex.mode",
	} {
		cmd := exec.CommandContext(ctx, "git", "-C", root, "config", "--local", "--unset-all", key)
		if err := cmd.Run(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 5 {
				continue
			}
			return fmt.Errorf("remove codeindex hook setting %s: %w", key, err)
		}
	}
	return nil
}

func legacyManagedHooksPresent(hooksDir string) bool {
	for _, name := range CodeHookNames {
		data, err := os.ReadFile(filepath.Join(hooksDir, name))
		if err == nil && strings.Contains(string(data), codeHookStart) &&
			!strings.Contains(string(data), "minnow.codeindex.binary") {
			return true
		}
	}
	return false
}

func usesCustomHooksPath(ctx context.Context, root string) bool {
	cmd := exec.CommandContext(ctx, "git", "-C", root, "config", "--get", "core.hooksPath")
	return cmd.Run() == nil
}

func CodeIndexHookStatus(ctx context.Context, root string) (CodeHookStatus, error) {
	resolvedRoot, hooksDir, err := gitHooksDir(ctx, root)
	if err != nil {
		return CodeHookStatus{}, err
	}
	status := CodeHookStatus{
		Root:      resolvedRoot,
		HooksDir:  hooksDir,
		Installed: map[string]bool{},
		Paths:     map[string]string{},
	}
	configured := exec.CommandContext(
		ctx, "git", "-C", resolvedRoot, "config", "--local", "--get", "minnow.codeindex.binary",
	).Run() == nil
	for _, name := range CodeHookNames {
		path := filepath.Join(hooksDir, name)
		status.Paths[name] = path
		data, err := os.ReadFile(path)
		status.Installed[name] = configured && err == nil && strings.Contains(string(data), codeHookStart)
	}
	return status, nil
}

func renderCodeHookBlock() string {
	return fmt.Sprintf(`%s
(
codeindex_log=$(git rev-parse --git-path minnow-codeindex-hook.log 2>/dev/null || printf '%%s' "${TMPDIR:-/tmp}/minnow-codeindex-hook.log")
codeindex_root=$(git rev-parse --show-toplevel 2>>"$codeindex_log") || { printf 'codeindex: cannot resolve repository root\n' >>"$codeindex_log"; exit 0; }
codeindex_binary=$(git config --local --get minnow.codeindex.binary 2>>"$codeindex_log") || { printf 'codeindex: hook is not configured for this repository\n' >>"$codeindex_log"; exit 0; }
codeindex_config=$(git config --local --get minnow.codeindex.config 2>/dev/null || true)
codeindex_kb=$(git config --local --get minnow.codeindex.kb 2>/dev/null || true)
codeindex_key=$(git config --local --get minnow.codeindex.index-key 2>/dev/null || true)
codeindex_mode=$(git config --local --get minnow.codeindex.mode 2>/dev/null || true)
set -- refresh --root "$codeindex_root" --yes --quiet
[ -z "$codeindex_config" ] || set -- "$@" --config "$codeindex_config"
[ -z "$codeindex_kb" ] || set -- "$@" --kb "$codeindex_kb"
[ -z "$codeindex_key" ] || set -- "$@" --index-key "$codeindex_key"
[ "$codeindex_mode" != index ] || set -- index "$@"
CODEINDEX_REPO_ROOT="$codeindex_root" "$codeindex_binary" "$@" >/dev/null 2>>"$codeindex_log" || true
)
%s
`, codeHookStart, codeHookEnd)
}

func saveCodeHookConfig(ctx context.Context, root, binary, kbID, indexKey, config string) error {
	values := map[string]string{
		"minnow.codeindex.binary":    binary,
		"minnow.codeindex.config":    strings.TrimSpace(config),
		"minnow.codeindex.kb":        kbID,
		"minnow.codeindex.index-key": indexKey,
		"minnow.codeindex.mode":      "",
	}
	if strings.TrimSuffix(filepath.Base(binary), filepath.Ext(binary)) == "minnow" {
		values["minnow.codeindex.mode"] = "index"
	}
	for key, value := range values {
		args := []string{"-C", root, "config", "--local", "--replace-all", key, value}
		if output, err := exec.CommandContext(ctx, "git", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("save codeindex hook setting %s: %w: %s", key, err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func gitHooksDir(ctx context.Context, root string) (string, string, error) {
	resolved, err := resolveCodeRoot(root)
	if err != nil {
		return "", "", err
	}
	out, err := exec.CommandContext(ctx, "git", "-C", resolved, "rev-parse", "--git-path", "hooks").Output()
	if err != nil {
		return "", "", fmt.Errorf("git hooks require a git repository: %w", err)
	}
	hooksDir := strings.TrimSpace(string(out))
	if hooksDir == "" {
		return "", "", fmt.Errorf("git hooks dir is empty")
	}
	if !filepath.IsAbs(hooksDir) {
		hooksDir = filepath.Join(resolved, hooksDir)
	}
	return resolved, hooksDir, nil
}

func replaceManagedHookBlock(content, block string) string {
	return appendManagedHookBlock(removeManagedHookBlock(content), block)
}

func removeManagedHookBlock(content string) string {
	start := strings.Index(content, codeHookStart)
	if start < 0 {
		return content
	}
	end := strings.Index(content[start:], codeHookEnd)
	if end < 0 {
		return content
	}
	endAbs := start + end + len(codeHookEnd)
	if endAbs < len(content) && content[endAbs] == '\n' {
		endAbs++
	}
	return strings.TrimRight(content[:start]+content[endAbs:], "\n") + "\n"
}

func resolveCodeRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(abs, ".git")); err == nil {
			return abs, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", fmt.Errorf("not inside a git repository: %s", root)
		}
		abs = parent
	}
}
