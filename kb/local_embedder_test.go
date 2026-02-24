package kb

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalEmbedder(t *testing.T) {
	t.Run("constructor_validation", func(t *testing.T) {
		_, err := NewLocalEmbedder(0)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidEmbeddingDimension)
	})

	t.Run("empty_input", func(t *testing.T) {
		e, err := NewLocalEmbedder(64)
		require.NoError(t, err)
		_, err = e.Embed(context.Background(), "   ")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("deterministic", func(t *testing.T) {
		e, err := NewLocalEmbedder(128)
		require.NoError(t, err)
		v1, err := e.Embed(context.Background(), "Golang vector search")
		require.NoError(t, err)
		v2, err := e.Embed(context.Background(), "Golang vector search")
		require.NoError(t, err)
		assert.Equal(t, v1, v2)
	})

	t.Run("dimension", func(t *testing.T) {
		e, err := NewLocalEmbedder(384)
		require.NoError(t, err)
		vec, err := e.Embed(context.Background(), "semantic retrieval system")
		require.NoError(t, err)
		assert.Len(t, vec, 384)
	})

	t.Run("normalized", func(t *testing.T) {
		e, err := NewLocalEmbedder(256)
		require.NoError(t, err)
		vec, err := e.Embed(context.Background(), "query ranking model")
		require.NoError(t, err)
		norm := vectorNorm(vec)
		assert.InDelta(t, 1.0, norm, 1e-5)
	})

	t.Run("token_overlap_similarity", func(t *testing.T) {
		e, err := NewLocalEmbedder(384)
		require.NoError(t, err)

		a, err := e.Embed(context.Background(), "golang vector search")
		require.NoError(t, err)
		b, err := e.Embed(context.Background(), "golang search")
		require.NoError(t, err)
		c, err := e.Embed(context.Background(), "banana bread recipe")
		require.NoError(t, err)

		ab := dot(a, b)
		ac := dot(a, c)
		assert.Greater(t, ab, ac)
	})

	t.Run("morphological_similarity", func(t *testing.T) {
		e, err := NewLocalEmbedder(384)
		require.NoError(t, err)

		// Words sharing subword structure should be closer than unrelated words.
		a, err := e.Embed(context.Background(), "searching databases")
		require.NoError(t, err)
		b, err := e.Embed(context.Background(), "database search")
		require.NoError(t, err)
		c, err := e.Embed(context.Background(), "tropical rainfall")
		require.NoError(t, err)

		ab := dot(a, b)
		ac := dot(a, c)
		assert.Greater(t, ab, ac, "morphological variants should be closer than unrelated text")
	})

	t.Run("l2_distance_ordering", func(t *testing.T) {
		e, err := NewLocalEmbedder(384)
		require.NoError(t, err)

		query, err := e.Embed(context.Background(), "vector database indexing")
		require.NoError(t, err)
		related, err := e.Embed(context.Background(), "indexed vectors stored")
		require.NoError(t, err)
		unrelated, err := e.Embed(context.Background(), "cooking pasta recipe")
		require.NoError(t, err)

		distRelated := l2dist(query, related)
		distUnrelated := l2dist(query, unrelated)
		assert.Less(t, distRelated, distUnrelated, "related text should have smaller L2 distance")
	})
}

func dot(a, b []float32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	sum := 0.0
	for i := 0; i < n; i++ {
		sum += float64(a[i] * b[i])
	}
	return sum
}

func vectorNorm(v []float32) float64 {
	s := 0.0
	for _, x := range v {
		s += float64(x * x)
	}
	return math.Sqrt(s)
}

func l2dist(a, b []float32) float64 {
	sum := 0.0
	for i := range a {
		d := float64(a[i] - b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}
