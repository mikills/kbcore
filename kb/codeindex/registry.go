package codeindex

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	opts := normalizeOptions(Options{Root: requestedRoot, IndexKey: indexKey, KBID: kbID})
	registry, err := loadRegistry(registryRoot)
	if err != nil {
		return Options{}, err
	}
	if entry, ok := registry.Indexes[opts.IndexKey]; ok {
		if strings.TrimSpace(opts.KBID) == "" {
			opts.KBID = entry.KBID
		}
		opts.Root = RootFromEntry(registryRoot, entry)
		opts.Description = entry.Description
		opts.IncludeUntracked = entry.IncludeUntracked
	}
	if strings.TrimSpace(opts.KBID) == "" {
		opts.KBID = DefaultKBIDForIndexKey(opts.IndexKey)
	}
	return opts, nil
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
			return hex.EncodeToString(sum[:8])
		}
	}
	sum := sha256.Sum256([]byte(filepath.Clean(root)))
	return hex.EncodeToString(sum[:8])
}

func normalizeOptions(opts Options) Options {
	opts.IndexKey = SanitizeKey(opts.IndexKey)
	return opts
}

func CodeRepoID(root string) string { return codeRepoID(root) }
