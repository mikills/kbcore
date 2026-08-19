package codeindex

import (
	"fmt"
	"strings"
	"testing"
)

// A long line at the head of a window with short lines after it makes the
// overlap span the whole window, which used to restart the next chunk on the
// line it began with and spin forever, appending a chunk every pass.
func TestChunkOverlapTermination(t *testing.T) {
	advances := t.Run("next start moves past the current one for every window", func(t *testing.T) {
		lines := splitLineViews(strings.Repeat("ab\n", 4) + strings.Repeat("x", 900))
		for start := range lines {
			for end := start + 1; end <= len(lines); end++ {
				if next := nextChunkStart(lines[start:end], end, 120); next <= start {
					t.Fatalf("start=%d end=%d next=%d did not advance", start, end, next)
				}
			}
		}
	})
	if !advances {
		t.Fatal("chunking cannot terminate, skipping the checks that would hang")
	}

	t.Run("a window the overlap covers yields no repeated chunks", func(t *testing.T) {
		var b strings.Builder
		for i := range 20 {
			fmt.Fprintf(&b, "%s-%d\n", strings.Repeat("x", 900), i)
			for j := range 6 {
				fmt.Fprintf(&b, "short-%d-%d\n", i, j)
			}
		}
		chunks := ChunkText(b.String(), "markdown", 1200, 120)
		if len(chunks) == 0 {
			t.Fatal("expected chunks")
		}
		for i := 1; i < len(chunks); i++ {
			if chunks[i].StartLine <= chunks[i-1].StartLine || chunks[i].EndLine <= chunks[i-1].EndLine {
				t.Fatalf("chunk %d [%d-%d] does not advance past %d [%d-%d]",
					i, chunks[i].StartLine, chunks[i].EndLine,
					i-1, chunks[i-1].StartLine, chunks[i-1].EndLine)
			}
		}
	})

	t.Run("sizes the caller never normalized still terminate", func(t *testing.T) {
		text := "ab\n" + strings.Repeat("x", 3000)
		for _, tc := range []struct{ size, overlap int }{{0, 0}, {-5, 0}, {1200, -500}, {100, 100}} {
			if got := ChunkText(text, "markdown", tc.size, tc.overlap); len(got) == 0 {
				t.Fatalf("chunkSize=%d overlap=%d produced no chunks", tc.size, tc.overlap)
			}
		}
	})
}
