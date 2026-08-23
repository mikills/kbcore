package codeindex

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func ResolveSelection(root, indexKey, kbID string) (Options, error) {
	requestedRoot, err := ResolveRequestedRoot(root)
	if err != nil {
		return Options{}, err
	}
	registryRoot, err := ResolveRoot(requestedRoot)
	if err != nil {
		return Options{}, err
	}
	implicitIndex := strings.TrimSpace(indexKey) == ""
	if implicitIndex {
		indexKey = currentIndexKey(registryRoot, requestedRoot)
	}
	opts := normalizeOptions(Options{Root: requestedRoot, IndexKey: indexKey, KBID: kbID})
	registry, err := loadRegistry(registryRoot)
	if err != nil {
		return Options{}, err
	}
	entry, registered := registry.Indexes[opts.IndexKey]
	if registered {
		if strings.TrimSpace(opts.KBID) == "" {
			opts.KBID = entry.KBID
		}
		opts.Root = RootFromEntry(registryRoot, entry)
		opts.Description = entry.Description
		opts.IncludeUntracked = entry.IncludeUntracked
		opts.ScopeID = entry.ScopeID
	}
	if implicitIndex && !registered {
		derivedKBID, scopeID := defaultScopedSelection(registryRoot, requestedRoot)
		opts.ScopeID = scopeID
		if strings.TrimSpace(opts.KBID) == "" {
			opts.KBID = derivedKBID
		}
	}
	if strings.TrimSpace(opts.KBID) == "" {
		opts.KBID = DefaultKBIDForIndexKey(opts.IndexKey)
	}
	return opts, nil
}

func currentIndexKey(repoRoot, root string) string {
	ref := currentRef(repoRoot)
	if ref == "" {
		return "default"
	}
	key := SanitizeKey(ref) + "-" + shortIdentity(ref)
	if scope := RelativeRoot(repoRoot, root); scope != "." {
		key += "-" + shortIdentity(scope)
	}
	return key
}

func currentRef(repoRoot string) string {
	if ref := gitValue(repoRoot, "branch", "--show-current"); ref != "" {
		return ref
	}
	if sha := gitValue(repoRoot, "rev-parse", "HEAD"); sha != "" {
		return "detached-" + sha
	}
	return ""
}

func defaultScopedSelection(repoRoot, root string) (string, string) {
	ref := currentRef(repoRoot)
	if ref == "" {
		return "", ""
	}
	identity, name := cliRepository(repoRoot)
	repoID := shortIdentity(identity)
	scope := RelativeRoot(repoRoot, root)
	kbID := SanitizeKey("code-" + name + "-repository-" + repoID + "-" + shortIdentity(scope))
	return kbID, "codeindex-" + identityHash(ref+"\x00"+scope)
}

func cliRepository(root string) (string, string) {
	if remote := gitValue(root, "config", "--get", "remote.origin.url"); remote != "" {
		return remote, repositoryName(remote, filepath.Base(root))
	}
	identity := primaryWorktreeRoot(root)
	return identity, filepath.Base(identity)
}

func primaryWorktreeRoot(root string) string {
	if saved := loadRepositoryRoot(root); saved != "" {
		return saved
	}
	if top := gitValue(root, "rev-parse", "--show-toplevel"); top != "" && isPrimaryWorktree(root) {
		if resolved, err := filepath.EvalSymlinks(top); err == nil {
			top = resolved
		}
		return filepath.Clean(top)
	}
	if output := gitValue(root, "worktree", "list", "--porcelain"); output != "" {
		for _, line := range strings.Split(output, "\n") {
			if candidate, found := strings.CutPrefix(line, "worktree "); found {
				if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
					candidate = resolved
				}
				return filepath.Clean(candidate)
			}
		}
	}
	return root
}

func loadRepositoryRoot(root string) string {
	common := gitValue(root, "rev-parse", "--git-common-dir")
	if common == "" {
		return ""
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(root, common)
	}
	data, err := os.ReadFile(filepath.Join(filepath.Clean(common), "minnow", "codeindex", "repository-root"))
	if err != nil {
		return ""
	}
	saved := strings.TrimSpace(string(data))
	resolved, err := filepath.EvalSymlinks(saved)
	if err != nil {
		return ""
	}
	return filepath.Clean(resolved)
}

func isPrimaryWorktree(root string) bool {
	gitDir := gitValue(root, "rev-parse", "--absolute-git-dir")
	common := gitValue(root, "rev-parse", "--git-common-dir")
	if gitDir == "" || common == "" {
		return false
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(root, common)
	}
	return filepath.Clean(gitDir) == filepath.Clean(common)
}

func repositoryName(remote, fallback string) string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(remote), ".git")
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Path != "" {
		if name := filepath.Base(parsed.Path); name != "." && name != "/" {
			return SanitizeKey(name)
		}
	}
	if colon := strings.LastIndex(trimmed, ":"); colon >= 0 {
		trimmed = trimmed[colon+1:]
	}
	if slash := strings.LastIndex(trimmed, "/"); slash >= 0 {
		trimmed = trimmed[slash+1:]
	}
	if trimmed == "" {
		trimmed = fallback
	}
	return SanitizeKey(trimmed)
}

func gitValue(root string, args ...string) string {
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func shortIdentity(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:4])
}

func identityHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func LoadRegistry(root string) (Registry, error) {
	requestedRoot, err := ResolveRequestedRoot(root)
	if err != nil {
		return Registry{}, err
	}
	registryRoot, err := ResolveRoot(requestedRoot)
	if err != nil {
		return Registry{}, err
	}
	return loadRegistry(registryRoot)
}

func loadRegistry(root string) (Registry, error) {
	registry := Registry{SchemaVersion: "minnow.codebase_indexes/v1", Indexes: map[string]RegistryEntry{}}
	data, err := os.ReadFile(codebaseIndexRegistryPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return registry, nil
		}
		return Registry{}, err
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		return Registry{}, err
	}
	if registry.Indexes == nil {
		registry.Indexes = map[string]RegistryEntry{}
	}
	return registry, nil
}

func SaveRegistry(root string, registry Registry) error {
	if registry.SchemaVersion == "" {
		registry.SchemaVersion = "minnow.codebase_indexes/v1"
	}
	if registry.Indexes == nil {
		registry.Indexes = map[string]RegistryEntry{}
	}
	path := codebaseIndexRegistryPath(root)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".codebase-indexes-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func codebaseIndexRegistryPath(root string) string {
	return filepath.Join(root, ".minnow", "codebase-indexes.json")
}

func SanitizeKey(key string) string {
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

func DefaultKBIDForIndexKey(key string) string {
	key = SanitizeKey(key)
	return "code-" + key
}

func DefaultDescription(root, key string) string {
	name := filepath.Base(root)
	if key == "default" {
		return "Default codebase index for " + name
	}
	return fmt.Sprintf("Codebase index %q for %s", key, name)
}

func ResolveRequestedRoot(root string) (string, error) {
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
	info, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("code index root must be a directory: %s", abs)
	}
	return filepath.Clean(abs), nil
}

func RelativeRoot(registryRoot, root string) string {
	rel, err := filepath.Rel(registryRoot, root)
	if err != nil || rel == "." {
		return "."
	}
	return filepath.ToSlash(rel)
}

func RootFromEntry(registryRoot string, entry RegistryEntry) string {
	if entry.Root == "" || entry.Root == "." {
		return registryRoot
	}
	return filepath.Clean(filepath.Join(registryRoot, filepath.FromSlash(entry.Root)))
}

func ResolveRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if out, err := exec.Command("git", "-C", abs, "rev-parse", "--show-toplevel").Output(); err == nil {
		candidate := strings.TrimSpace(string(out))
		if candidate != "" {
			abs = candidate
		}
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("code index root must be a directory: %s", abs)
	}
	return filepath.Clean(abs), nil
}

func codeRepoID(root string) string {
	if out, err := exec.Command("git", "-C", root, "config", "--get", "remote.origin.url").Output(); err == nil {
		if remote := strings.TrimSpace(string(out)); remote != "" {
			sum := sha256.Sum256([]byte(remote))
			return hex.EncodeToString(sum[:4])
		}
	}
	identity := primaryWorktreeRoot(filepath.Clean(root))
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:4])
}

func normalizeOptions(opts Options) Options {
	opts.IndexKey = SanitizeKey(opts.IndexKey)
	return opts
}

func CodeRepoID(root string) string { return codeRepoID(root) }
