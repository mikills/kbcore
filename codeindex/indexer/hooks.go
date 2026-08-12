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
	block := renderCodeHookBlock(binary, kbID, indexKey, root, opts.Config)
	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
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
	if indexKey != "" {
		indexKey = sanitizeCodeIndexKey(indexKey)
	}
	return binary, kbID, indexKey
}

func installCodeHook(hooksDir string, name string, block string, force bool) error {
	path := filepath.Join(hooksDir, name)
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	content, err := updatedCodeHookContent(name, string(data), block, force)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o755)
}

func updatedCodeHookContent(name string, content string, block string, force bool) (string, error) {
	if strings.Contains(content, codeHookStart) {
		return replaceManagedHookBlock(content, block), nil
	}
	if strings.TrimSpace(content) != "" && !force {
		return "", fmt.Errorf("git hook %s already exists; rerun with force to append Minnow managed block", name)
	}
	return appendManagedHookBlock(content, block), nil
}

func appendManagedHookBlock(content string, block string) string {
	if strings.TrimSpace(content) == "" {
		content = "#!/bin/sh\n"
	}
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return content + block
}

func UninstallCodeIndexHooks(ctx context.Context, root string) (CodeHookStatus, error) {
	_, hooksDir, err := gitHooksDir(ctx, root)
	if err != nil {
		return CodeHookStatus{}, err
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
	return CodeIndexHookStatus(ctx, root)
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
	for _, name := range CodeHookNames {
		path := filepath.Join(hooksDir, name)
		status.Paths[name] = path
		data, err := os.ReadFile(path)
		status.Installed[name] = err == nil && strings.Contains(string(data), codeHookStart)
	}
	return status, nil
}

func renderCodeHookBlock(binary, kbID, indexKey, root, config string) string {
	command := shellQuote(binary) + " refresh"
	if strings.TrimSuffix(filepath.Base(binary), filepath.Ext(binary)) == "minnow" {
		command = shellQuote(binary) + " index refresh"
	}
	if strings.TrimSpace(config) != "" {
		command += " --config " + shellQuote(config)
	}
	if kbID != "" {
		command += " --kb " + shellQuote(kbID)
	}
	if indexKey != "" {
		command += " --index-key " + shellQuote(indexKey)
	}
	return fmt.Sprintf(`%s
CODEINDEX_REPO_ROOT=%s %s --root %s --yes --quiet >/dev/null 2>&1 || true
%s
`, codeHookStart, shellQuote(root), command, shellQuote(root), codeHookEnd)
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
	return removeManagedHookBlock(content) + block
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

func sanitizeCodeIndexKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "default"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.':
			return r
		default:
			return '-'
		}
	}, key)
}

func resolveCodeRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
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
