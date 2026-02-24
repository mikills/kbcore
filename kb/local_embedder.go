package kb

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"unicode"
)

const (
	defaultLocalEmbedDim = 384
	defaultMinNgram      = 3
	defaultMaxNgram      = 6
)

var (
	seedIndexBytes = []byte("kbcore-subword-idx-v1::")
	seedSignBytes  = []byte("kbcore-subword-sgn-v1::")
)

// LocalEmbedder is a deterministic, pure-Go embedding implementation using
// character n-gram hashing (FastText-style). Words sharing subword structure
// produce similar vectors, giving morphological similarity that works with
// L2-distance based search.
type LocalEmbedder struct {
	dim      int
	minNgram int
	maxNgram int
}

// NewLocalEmbedder creates a local deterministic embedder with the given
// output dimensionality.
func NewLocalEmbedder(dim int) (*LocalEmbedder, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidEmbeddingDimension, dim)
	}
	return &LocalEmbedder{dim: dim, minNgram: defaultMinNgram, maxNgram: defaultMaxNgram}, nil
}

// Embed returns a deterministic embedding for the input text.
// Each word is decomposed into character n-grams (3-6), hashed into the
// vector space, then word vectors are averaged and L2-normalized.
func (e *LocalEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	normalized := normalizeLocalInput(input)
	if normalized == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}

	tokens := tokenizeLocalInput(normalized)
	vec := make([]float32, e.dim)
	var wordCount int

	for _, tok := range tokens {
		if _, skip := localEmbedStopwords[tok]; skip {
			continue
		}
		e.addWordVector(vec, tok)
		wordCount++
	}
	if wordCount == 0 {
		return nil, fmt.Errorf("input has no indexable tokens after stopword filtering")
	}

	// Average word vectors.
	scale := 1.0 / float32(wordCount)
	for i := range vec {
		vec[i] *= scale
	}

	if !normalizeLocalVector(vec) {
		return nil, fmt.Errorf("failed to build embedding: zero vector")
	}
	return vec, nil
}

// addWordVector adds the subword-hashed vector for a single word into vec.
// The word is wrapped with boundary markers (<word>) and decomposed into
// character n-grams. Each n-gram and the whole bounded word are hashed
// into the vector using the feature hashing trick (random index + sign).
func (e *LocalEmbedder) addWordVector(vec []float32, word string) {
	bounded := "<" + word + ">"
	runes := []rune(bounded)

	// Whole-word feature.
	addFeature(vec, bounded, e.dim)

	// Character n-grams.
	for n := e.minNgram; n <= e.maxNgram && n <= len(runes); n++ {
		for i := 0; i <= len(runes)-n; i++ {
			ngram := string(runes[i : i+n])
			addFeature(vec, ngram, e.dim)
		}
	}
}

func addFeature(vec []float32, feature string, dim int) {
	idx := int(stableHash(seedIndexBytes, feature) % uint64(dim))
	if stableHash(seedSignBytes, feature)%2 == 1 {
		vec[idx] -= 1.0
	} else {
		vec[idx] += 1.0
	}
}

func normalizeLocalInput(input string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(input))), " ")
}

func tokenizeLocalInput(input string) []string {
	tokens := make([]string, 0, len(input)/4)
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, b.String())
		b.Reset()
	}

	for _, r := range input {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return tokens
}

func stableHash(seedWithSep []byte, token string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(seedWithSep)
	_, _ = h.Write([]byte(token))
	return h.Sum64()
}

func normalizeLocalVector(vec []float32) bool {
	sumSq := 0.0
	for _, v := range vec {
		fv := float64(v)
		sumSq += fv * fv
	}
	if sumSq == 0 {
		return false
	}
	norm := float32(math.Sqrt(sumSq))
	for i := range vec {
		vec[i] /= norm
	}
	return true
}

var localEmbedStopwords = map[string]struct{}{
	"a": {}, "about": {}, "above": {}, "after": {}, "again": {}, "against": {}, "all": {}, "am": {}, "an": {}, "and": {},
	"any": {}, "are": {}, "as": {}, "at": {}, "be": {}, "because": {}, "been": {}, "before": {}, "being": {}, "below": {},
	"between": {}, "both": {}, "but": {}, "by": {}, "can": {}, "did": {}, "do": {}, "does": {}, "doing": {}, "down": {},
	"during": {}, "each": {}, "few": {}, "for": {}, "from": {}, "further": {}, "had": {}, "has": {}, "have": {}, "having": {},
	"he": {}, "her": {}, "here": {}, "hers": {}, "herself": {}, "him": {}, "himself": {}, "his": {}, "how": {}, "i": {},
	"if": {}, "in": {}, "into": {}, "is": {}, "it": {}, "its": {}, "itself": {}, "just": {}, "me": {}, "more": {},
	"most": {}, "my": {}, "myself": {}, "no": {}, "nor": {}, "not": {}, "now": {}, "of": {}, "off": {}, "on": {},
	"once": {}, "only": {}, "or": {}, "other": {}, "our": {}, "ours": {}, "ourselves": {}, "out": {}, "over": {}, "own": {},
	"same": {}, "she": {}, "should": {}, "so": {}, "some": {}, "such": {}, "than": {}, "that": {}, "the": {}, "their": {},
	"theirs": {}, "them": {}, "themselves": {}, "then": {}, "there": {}, "these": {}, "they": {}, "this": {}, "those": {},
	"through": {}, "to": {}, "too": {}, "under": {}, "until": {}, "up": {}, "very": {}, "was": {}, "we": {}, "were": {},
	"what": {}, "when": {}, "where": {}, "which": {}, "while": {}, "who": {}, "whom": {}, "why": {}, "with": {}, "you": {},
	"your": {}, "yours": {}, "yourself": {}, "yourselves": {},
}
