package config

import "testing"

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestApplyOverridesDuckDBMemoryLimit(t *testing.T) {
	t.Run("overrides the configured value", func(t *testing.T) {
		cfg := &Config{}
		cfg.Format.DuckDB.MemoryLimit = "128MB"
		resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvDuckDBMemoryLimit: " 512MB "})}
		if err := resolver.ApplyOverrides(cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.Format.DuckDB.MemoryLimit != "512MB" {
			t.Fatalf("got %q, want 512MB", cfg.Format.DuckDB.MemoryLimit)
		}
	})

	t.Run("leaves the config alone when unset", func(t *testing.T) {
		cfg := &Config{}
		cfg.Format.DuckDB.MemoryLimit = "128MB"
		resolver := EnvResolver{Lookup: lookupFrom(map[string]string{})}
		if err := resolver.ApplyOverrides(cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.Format.DuckDB.MemoryLimit != "128MB" {
			t.Fatalf("got %q, want the configured 128MB", cfg.Format.DuckDB.MemoryLimit)
		}
	})

	t.Run("rejects an empty value", func(t *testing.T) {
		cfg := &Config{}
		resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvDuckDBMemoryLimit: "  "})}
		if err := resolver.ApplyOverrides(cfg); err == nil {
			t.Fatal("expected an error for an empty override")
		}
	})
}
