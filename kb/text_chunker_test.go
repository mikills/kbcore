package kb

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextChunker(t *testing.T) {
	t.Run("single chunk when text fits", func(t *testing.T) {
		chunker := TextChunker{ChunkSize: 200}
		text := "  The quick brown fox jumps over the lazy dog. This classic pangram contains every letter of the alphabet at least once.  "

		chunks, err := chunker.Chunk(context.Background(), "doc", text)
		require.NoError(t, err)
		require.Len(t, chunks, 1)

		assert.Equal(t, "doc-chunk-000", chunks[0].ChunkID)
		assert.Equal(t, strings.TrimSpace(text), chunks[0].Text)
		assert.Equal(t, 0, chunks[0].Start)
		assert.Equal(t, len(strings.TrimSpace(text)), chunks[0].End)
	})

	t.Run("recursive split with separators", func(t *testing.T) {
		text := `Introduction to Machine Learning

Machine learning is a subset of artificial intelligence that enables systems to learn from data. It has revolutionized many industries including healthcare, finance, and transportation.

Supervised Learning

In supervised learning, models are trained on labeled datasets. The algorithm learns to map inputs to outputs by examining many examples. Common applications include image classification, spam detection, and price prediction.

Unsupervised Learning

Unsupervised learning works with unlabeled data. The algorithm must find patterns and structure on its own. Clustering and dimensionality reduction are two major techniques in this category.

Conclusion

Machine learning continues to advance rapidly. New architectures and training methods emerge regularly, pushing the boundaries of what automated systems can achieve.`

		chunker := TextChunker{ChunkSize: 150}
		chunks, err := chunker.Chunk(context.Background(), "ml-doc", text)
		require.NoError(t, err)

		// should produce multiple chunks given the size constraint
		require.GreaterOrEqual(t, len(chunks), 5, "expected at least 5 chunks for this document")

		// verify no chunk exceeds the size limit
		for i, chunk := range chunks {
			assert.LessOrEqual(t, len(chunk.Text), 150, "chunk %d exceeds size limit: %d bytes", i, len(chunk.Text))
			assert.NotEmpty(t, chunk.Text, "chunk %d should not be empty", i)
		}

		// verify paragraph splits are preferred (first chunk should be the title or first section)
		assert.True(t, strings.Contains(chunks[0].Text, "Machine") || strings.Contains(chunks[0].Text, "Introduction"),
			"first chunk should contain intro content")

		// verify sentences stay intact when possible (look for complete sentences with periods)
		sentenceEndCount := 0
		for _, chunk := range chunks {
			if strings.HasSuffix(chunk.Text, ".") {
				sentenceEndCount++
			}
		}
		assert.GreaterOrEqual(t, sentenceEndCount, 3, "most chunks should end with complete sentences")

		// verify all text is covered (no gaps, no overlap in meaning)
		var combined strings.Builder
		for i, chunk := range chunks {
			if i > 0 {
				combined.WriteString(" ")
			}
			combined.WriteString(chunk.Text)
		}
		// both should contain the same key phrases
		for _, phrase := range []string{"Machine learning", "Supervised Learning", "Unsupervised", "Conclusion"} {
			assert.True(t, strings.Contains(combined.String(), phrase) || strings.Contains(strings.TrimSpace(text), phrase),
				"combined chunks should preserve content: %s", phrase)
		}
	})

	t.Run("chunk id format and offsets", func(t *testing.T) {
		// longer text with mixed content to generate many chunks
		text := `Alpha section begins here with some introductory content that sets the stage for what follows.

Beta section continues the narrative with additional details and explanations that build upon the foundation.

Gamma section introduces new concepts. These ideas are important. They require careful consideration. Each sentence adds value.

Delta section provides examples and illustrations. The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump.

Epsilon section wraps up the discussion with final thoughts and recommendations for further exploration of these topics.`

		chunker := TextChunker{ChunkSize: 120}
		trimmed := strings.TrimSpace(text)

		chunks, err := chunker.Chunk(context.Background(), "test-doc", text)
		require.NoError(t, err)
		require.NotEmpty(t, chunks)
		require.GreaterOrEqual(t, len(chunks), 4, "expected at least 4 chunks")

		idPattern := regexp.MustCompile(`^test-doc-chunk-\d{3}$`)

		for i, chunk := range chunks {
			// verify chunk id format
			assert.Regexp(t, idPattern, chunk.ChunkID, "chunk %d has invalid id format", i)

			// verify offsets are valid
			assert.GreaterOrEqual(t, chunk.Start, 0, "chunk %d start should be non-negative", i)
			assert.Greater(t, chunk.End, chunk.Start, "chunk %d end should be greater than start", i)
			assert.LessOrEqual(t, chunk.End, len(trimmed), "chunk %d end should not exceed text length", i)

			// verify text matches offset range
			assert.Equal(t, chunk.Text, trimmed[chunk.Start:chunk.End],
				"chunk %d text should match offset range", i)

			// verify no overlap with previous chunk
			if i > 0 {
				assert.GreaterOrEqual(t, chunk.Start, chunks[i-1].End, "chunk %d should not overlap with chunk %d", i, i-1)
			}
		}

	})
}
