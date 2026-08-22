package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestCacheMaxBytesOverride(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  int64
	}{
		{"plain bytes", "1024", 1024},
		{"megabytes", "512MB", 512 << 20},
		{"gigabytes", " 4gb ", 4 << 30},
		{"explicit bytes", "2048B", 2048},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{}
			cfg.Storage.Cache.MaxBytes = 1
			resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvCacheMaxBytes: tc.value})}
			require.NoError(t, resolver.ApplyOverrides(cfg))
			require.Equal(t, tc.want, cfg.Storage.Cache.MaxBytes)
		})
	}

	for _, bad := range []string{"", "  ", "lots", "-1", "12PB", "1.5GB"} {
		t.Run("refuses "+bad, func(t *testing.T) {
			cfg := &Config{}
			cfg.Storage.Cache.MaxBytes = 7
			resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvCacheMaxBytes: bad})}
			require.Error(t, resolver.ApplyOverrides(cfg))
			require.Equal(t, int64(7), cfg.Storage.Cache.MaxBytes)
		})
	}

	t.Run("leaves the config alone when unset", func(t *testing.T) {
		cfg := &Config{}
		cfg.Storage.Cache.MaxBytes = 99
		resolver := EnvResolver{Lookup: lookupFrom(map[string]string{})}
		require.NoError(t, resolver.ApplyOverrides(cfg))
		require.Equal(t, int64(99), cfg.Storage.Cache.MaxBytes)
	})
}

func TestEmbeddingDimOverride(t *testing.T) {
	t.Run("sets dimensions", func(t *testing.T) {
		cfg := &Config{Embedder: EmbedderConfig{
			OpenAICompatible: &OpenAICompatibleEmbedderConfig{Dimensions: 1536},
		}}
		resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvOpenAIEmbeddingDimensions: " 512 "})}
		require.NoError(t, resolver.ApplyOverrides(cfg))
		require.Equal(t, 512, cfg.Embedder.OpenAICompatible.Dimensions)
	})

	for _, value := range []string{"", "-1", "wide"} {
		t.Run("rejects "+value, func(t *testing.T) {
			cfg := &Config{Embedder: EmbedderConfig{
				OpenAICompatible: &OpenAICompatibleEmbedderConfig{Dimensions: 1536},
			}}
			resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvOpenAIEmbeddingDimensions: value})}
			require.Error(t, resolver.ApplyOverrides(cfg))
			require.Equal(t, 1536, cfg.Embedder.OpenAICompatible.Dimensions)
		})
	}

	t.Run("requires config block", func(t *testing.T) {
		cfg := &Config{}
		resolver := EnvResolver{Lookup: lookupFrom(map[string]string{EnvOpenAIEmbeddingDimensions: "512"})}
		require.Error(t, resolver.ApplyOverrides(cfg))
	})
}
