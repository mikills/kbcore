package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExamplesParse guards against the checked-in examples drifting away
// from the schema. Every example file in examples/ must Load without error.
func TestExamplesParse(t *testing.T) {
	t.Run("examples/minnow.min.yaml", func(t *testing.T) {
		_, err := Load("../../examples/minnow.min.yaml")
		require.NoError(t, err)
	})

	t.Run("examples/minnow.yaml", func(t *testing.T) {
		t.Setenv("MINNOW_MONGO_URI", "mongodb://localhost:27017")
		_, err := Load("../../examples/minnow.yaml")
		require.NoError(t, err)
	})

	t.Run("examples/minnow.dev.openai.yaml", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "test-key")
		_, err := Load("../../examples/minnow.dev.openai.yaml")
		require.NoError(t, err)
	})

	t.Run("examples/minnow.tiered.yaml", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "test-key")
		t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
		_, err := Load("../../examples/minnow.tiered.yaml")
		require.NoError(t, err)
	})

	t.Run("examples/minnow.s3.yaml", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "test-key")
		t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
		_, err := Load("../../examples/minnow.s3.yaml")
		require.NoError(t, err)
	})
}
