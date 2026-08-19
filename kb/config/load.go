package config

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultPath is the working-directory config searched when Load is called
// with an empty path.
const DefaultPath = "./minnow.yaml"

// Load reads, interpolates, strictly decodes, defaults, path-resolves and
// validates the YAML config at path. An empty path resolves first to
// DefaultPath, then to the per-user config path.
//
// Relative paths inside the config (storage.blob.root, storage.cache.dir,
// format.duckdb.extension_dir) are resolved against the YAML file's
// directory, not the process CWD.
func Load(path string) (*Config, error) {
	if path == "" {
		resolved, err := ResolveDefaultPath()
		if err != nil {
			return nil, err
		}
		path = resolved
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	expanded, err := interpolateEnv(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	cfg, err := decodeStrict(expanded)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	if err := OSResolver().ApplyOverrides(cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	cfg.applyDefaults()

	baseDir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("resolve base dir for %q: %w", path, err)
	}
	if err := cfg.resolvePaths(baseDir); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return cfg, nil
}

// UserConfigPath returns the per-user fallback config path used by globally
// installed binaries when no ./minnow.yaml is present.
func UserConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(dir, "minnow", "minnow.yaml"), nil
}

// ResolveDefaultPath returns the config path used when the caller does not pass
// an explicit path. The local working-directory file wins so repo/dev runs keep
// their existing behavior. the user config makes `go install` usable globally.
func ResolveDefaultPath() (string, error) {
	if _, err := os.Stat(DefaultPath); err == nil {
		return DefaultPath, nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("stat config %q: %w", DefaultPath, err)
	}

	userPath, err := UserConfigPath()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(userPath); err == nil {
		return userPath, nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("stat config %q: %w", userPath, err)
	}

	return "", fmt.Errorf(
		"read config: neither %q nor %q exists; set MINNOW_CONFIG or run `minnow config init dev-openai`",
		DefaultPath,
		userPath,
	)
}

// decodeStrict decodes YAML bytes into a Config, rejecting any unknown keys.
func decodeStrict(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}
	// Reject multi-document YAML files: a deployment is one document.
	var extra yaml.Node
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected additional YAML document (only one document per config file)")
		}
		return nil, err
	}
	return &cfg, nil
}

// resolvePaths rewrites any relative path field to be absolute, based on
// baseDir. Absolute paths are left alone (with an audit log). Empty paths
// are left empty (the caller will have already applied defaults, so empty
// here means the feature was deliberately turned off by the schema default).
//
// Threat model: a relative path that resolves outside baseDir (for example
// via template-substituted `../../etc/foo`) lets a misconfigured operator
// trick minnow into reading or writing state at an arbitrary filesystem
// location. Explicit absolute paths remain allowed because there is no
// authoritative base dir to compare against. the operator's choice is
// final, and we only audit-log it.
func (c *Config) resolvePaths(baseDir string) error {
	fields := []struct {
		name string
		p    *string
	}{
		{"storage.blob.root", &c.Storage.Blob.Root},
		{"storage.cache.dir", &c.Storage.Cache.Dir},
		{"format.duckdb.extension_dir", &c.Format.DuckDB.ExtensionDir},
	}
	if c.Storage.Blob.Tiered != nil {
		fields = append(fields, struct {
			name string
			p    *string
		}{"storage.blob.tiered.journal.dir", &c.Storage.Blob.Tiered.Journal.Dir})
	}
	for _, f := range fields {
		if *f.p == "" {
			continue
		}
		if filepath.IsAbs(*f.p) {
			slog.Debug("config path uses explicit absolute path; base-dir containment check skipped",
				"field", f.name, "path", *f.p)
			continue
		}
		resolved := filepath.Clean(filepath.Join(baseDir, *f.p))
		base := filepath.Clean(baseDir)
		if resolved != base && !strings.HasPrefix(resolved, base+string(filepath.Separator)) {
			return fmt.Errorf(
				"%s resolves outside the config's base directory (got %q, base %q)",
				f.name,
				resolved,
				base,
			)
		}
		*f.p = resolved
	}
	return nil
}
