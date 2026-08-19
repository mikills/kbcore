package config

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// envVarRE matches ${VAR} style references. ${VAR:-default} is intentionally
// not supported. defaults live in the schema, not in substitution syntax.
var envVarRE = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

type EnvResolver struct {
	Lookup func(string) (string, bool)
}

func OSResolver() EnvResolver {
	return EnvResolver{Lookup: os.LookupEnv}
}

// ResolveBytes replaces ${VAR} with looked-up values on every occurrence in the
// raw YAML bytes. Unset variables produce an aggregated error listing every
// missing name, so operators see the full set rather than one at a time.
//
// The substitution is purely textual. A literal ${X} inside a YAML comment will
// also be expanded. operators should not put unresolved ${X} patterns in
// comments.
func (r EnvResolver) ResolveBytes(data []byte) ([]byte, error) {
	lookup := r.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	missing := map[string]struct{}{}
	out := envVarRE.ReplaceAllFunc(data, func(match []byte) []byte {
		name := string(match[2 : len(match)-1])
		val, ok := lookup(name)
		if !ok {
			missing[name] = struct{}{}
			return match
		}
		return []byte(val)
	})
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		return nil, fmt.Errorf(
			"unresolved env vars: %s (note: ${VAR} in YAML comments also triggers this error)",
			strings.Join(names, ", "),
		)
	}
	return out, nil
}

func interpolateEnv(data []byte) ([]byte, error) {
	return OSResolver().ResolveBytes(data)
}

// EnvDuckDBMemoryLimit overrides format.duckdb.memory_limit. The config file is
// baked into the container image, so this is the only way to retune a
// deployment without rebuilding it.
const EnvDuckDBMemoryLimit = "MINNOW_DUCKDB_MEMORY_LIMIT"

func (r EnvResolver) ApplyOverrides(cfg *Config) error {
	lookup := r.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if value, ok := lookup(EnvDuckDBMemoryLimit); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return fmt.Errorf("%s must not be empty when set", EnvDuckDBMemoryLimit)
		}
		cfg.Format.DuckDB.MemoryLimit = trimmed
	}
	return nil
}
