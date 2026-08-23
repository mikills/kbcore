package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type codebaseRegistry struct {
	SchemaVersion string                           `json:"schema_version"`
	Indexes       map[string]codebaseRegistryEntry `json:"codebase_indexes"`
}

type codebaseRegistryEntry struct {
	KBID             string `json:"kb_id"`
	ScopeID          string `json:"scope_id,omitempty"`
	Root             string `json:"root"`
	Description      string `json:"description,omitempty"`
	IncludeUntracked bool   `json:"include_untracked"`
}

func loadCodebaseRegistry(root string) (codebaseRegistry, error) {
	registry := codebaseRegistry{
		SchemaVersion: "minnow.codebase_indexes/v1",
		Indexes:       make(map[string]codebaseRegistryEntry),
	}
	data, err := os.ReadFile(codebaseRegistryPath(root))
	if errors.Is(err, os.ErrNotExist) {
		return registry, nil
	}
	if err != nil {
		return codebaseRegistry{}, err
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		return codebaseRegistry{}, err
	}
	if registry.Indexes == nil {
		registry.Indexes = make(map[string]codebaseRegistryEntry)
	}
	return registry, nil
}

func saveCodebaseRegistry(root string, registry codebaseRegistry) error {
	if registry.SchemaVersion == "" {
		registry.SchemaVersion = "minnow.codebase_indexes/v1"
	}
	if registry.Indexes == nil {
		registry.Indexes = make(map[string]codebaseRegistryEntry)
	}
	path := codebaseRegistryPath(root)
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

func codebaseRegistryPath(root string) string {
	return filepath.Join(root, ".minnow", "codebase-indexes.json")
}
