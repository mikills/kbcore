package kb

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// defaultSeparators defines the hierarchy of separators for recursive splitting,
// ordered from most semantic to least semantic:
//   - "\n\n" paragraph breaks
//   - "\n"   line breaks
//   - "."    sentence endings (period kept with sentence)
//   - " "    word boundaries
//   - ""     character level (last resort)
var defaultSeparators = []string{"\n\n", "\n", ".", " ", ""}

// TextChunker splits text using a recursive character text splitter algorithm
// with no overlap between chunks.
//
// The algorithm tries to split on the most semantically meaningful boundaries
// first (paragraphs), falling back to less meaningful ones (sentences, words,
// characters) only when necessary to fit within ChunkSize.
//
// This approach is inspired by LangChain's RecursiveCharacterTextSplitter and
// similar implementations used in RAG pipelines.
type TextChunker struct {
	ChunkSize int
}

// Chunk splits a document into stable chunk IDs with byte offsets.
//
// The recursive splitting process:
//  1. try to split by paragraph breaks ("\n\n")
//  2. merge adjacent pieces while they fit within ChunkSize
//  3. if any piece is still too large, recursively split using line breaks ("\n")
//  4. continue with sentence breaks (". "), word breaks (" "), then characters
//
// No overlap is applied between chunks.
func (c TextChunker) Chunk(ctx context.Context, docID string, text string) ([]Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, nil
	}

	chunkSize := c.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 500
	}

	// perform recursive split
	pieces := recursiveSplit(trimmed, defaultSeparators, chunkSize)

	// build chunks with offsets
	chunks := make([]Chunk, 0, len(pieces))
	pos := 0
	trimmedLen := len(trimmed)
	for i, piece := range pieces {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if pos < 0 {
			pos = 0
		}
		if pos > trimmedLen {
			pos = trimmedLen
		}

		// find piece in original text starting from pos
		idx := strings.Index(trimmed[pos:], piece)
		if idx < 0 {
			// fallback: best-effort search across full text; if still missing, anchor at pos
			if absoluteIdx := strings.Index(trimmed, piece); absoluteIdx >= 0 {
				idx = absoluteIdx - pos
			} else {
				idx = 0
			}
		}
		start := pos + idx
		if start < 0 {
			start = 0
		}
		if start > trimmedLen {
			start = trimmedLen
		}
		end := start + len(piece)
		if end > trimmedLen {
			end = trimmedLen
		}
		if end < start {
			end = start
		}

		chunk := Chunk{
			DocID:   docID,
			ChunkID: fmt.Sprintf("%s-chunk-%03d", docID, i),
			Text:    piece,
			Start:   start,
			End:     end,
		}
		chunks = append(chunks, chunk)
		pos = end
	}

	return chunks, nil
}

// recursiveSplit splits text using a hierarchy of separators.
// it tries the first separator, merges small pieces, and recursively splits
// any pieces that are still too large using the remaining separators.
func recursiveSplit(text string, separators []string, chunkSize int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	// if text fits, return as-is
	if len(text) <= chunkSize {
		return []string{text}
	}

	// no separators left: hard split by runes
	if len(separators) == 0 {
		return hardSplitByRunes(text, chunkSize)
	}

	sep := separators[0]
	remainingSeps := separators[1:]

	// empty separator means character-level split
	if sep == "" {
		return hardSplitByRunes(text, chunkSize)
	}

	// split by current separator
	parts := strings.Split(text, sep)

	// for sentence-ending separators, keep the separator with the left part
	keepSepLeft := (sep == ".")

	// merge small adjacent parts back together
	merged := mergeSmallPieces(parts, sep, chunkSize, keepSepLeft)

	// recursively split any pieces that are still too large
	var result []string
	for _, piece := range merged {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}
		if len(piece) <= chunkSize {
			result = append(result, piece)
		} else {
			// recursively split with next separator
			subPieces := recursiveSplit(piece, remainingSeps, chunkSize)
			result = append(result, subPieces...)
		}
	}

	return result
}

// mergeSmallPieces combines adjacent pieces with the separator while they fit
// within chunkSize. this reassembles text that was split too aggressively.
// if keepSepLeft is true, the separator is appended to the left part (e.g., for periods).
func mergeSmallPieces(parts []string, sep string, chunkSize int, keepSepLeft bool) []string {
	if len(parts) == 0 {
		return nil
	}

	var result []string
	var current strings.Builder

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if current.Len() == 0 {
			// start new chunk
			current.WriteString(part)
			continue
		}

		if keepSepLeft {
			// separator goes with left part (e.g., period at end of sentence)
			// calculate size: current + sep + space + part
			newLen := current.Len() + len(sep) + 1 + len(part)

			if newLen <= chunkSize {
				// fits: append separator to current, then space and part
				current.WriteString(sep)
				current.WriteString(" ")
				current.WriteString(part)
			} else {
				// doesn't fit: append separator to current, flush, start new
				current.WriteString(sep)
				result = append(result, current.String())
				current.Reset()
				current.WriteString(part)
			}
		} else {
			// separator goes between parts normally
			newLen := current.Len() + len(sep) + len(part)

			if newLen <= chunkSize {
				// fits: append with separator
				current.WriteString(sep)
				current.WriteString(part)
			} else {
				// doesn't fit: flush current and start new
				result = append(result, current.String())
				current.Reset()
				current.WriteString(part)
			}
		}
	}

	// flush remaining
	if current.Len() > 0 {
		// if keepSepLeft and there are more parts after this, we already added sep
		// but if this is the last part without trailing sep, that's fine
		result = append(result, current.String())
	}

	return result
}

// hardSplitByRunes splits text into chunks of at most chunkSize bytes,
// respecting utf-8 rune boundaries. this is the last resort when no
// semantic separator can break the text small enough.
func hardSplitByRunes(text string, chunkSize int) []string {
	if len(text) <= chunkSize {
		return []string{text}
	}

	var result []string
	var current strings.Builder

	for _, r := range text {
		runeBytes := utf8.RuneLen(r)

		// if adding this rune would exceed chunk size, flush current
		if current.Len() > 0 && current.Len()+runeBytes > chunkSize {
			result = append(result, current.String())
			current.Reset()
		}

		current.WriteRune(r)
	}

	// flush remaining
	if current.Len() > 0 {
		s := strings.TrimSpace(current.String())
		if s != "" {
			result = append(result, s)
		}
	}

	return result
}

// trimBounds returns the leading whitespace count and the end position after
// trimming trailing whitespace. kept for potential future use.
func trimBounds(s string) (int, int) {
	leftTrimmed := strings.TrimLeftFunc(s, unicode.IsSpace)
	leading := len(s) - len(leftTrimmed)
	bothTrimmed := strings.TrimRightFunc(leftTrimmed, unicode.IsSpace)
	trailing := leading + len(bothTrimmed)
	return leading, trailing
}
