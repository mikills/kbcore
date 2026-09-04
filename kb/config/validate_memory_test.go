package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMemoryLimit(t *testing.T) {
	t.Run("accepts a size or auto", func(t *testing.T) {
		for _, raw := range []string{"128MB", " 4 GB ", "auto", "AUTO", ""} {
			require.NoErrorf(t, validateMemoryLimit(raw), "rejected %q", raw)
		}
	})

	t.Run("rejects anything DuckDB cannot parse", func(t *testing.T) {
		for _, raw := range []string{"lots", "90%", "4 gigabytes", "-1GB"} {
			require.ErrorContainsf(t, validateMemoryLimit(raw), "memory_limit", "accepted %q", raw)
		}
	})
}
