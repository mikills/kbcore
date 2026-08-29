package duckdb_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
)

// BenchmarkVectorQuery measures full QueryRag latency at several corpus sizes.
// Run with:
//
//	go test ./kb/duckdb/ -bench=BenchmarkVectorQuery -run=^$ -benchtime=1x -v -timeout=90m
func BenchmarkVectorQuery(b *testing.B) {
	cases, err := benchCases(os.Getenv(envBenchCases))
	if err != nil {
		b.Fatalf("%s: %v", envBenchCases, err)
	}

	suiteStart := time.Now()
	totals := make([]struct {
		name  string
		total time.Duration
	}, 0, len(cases))

	for _, tc := range cases {
		caseStart := time.Now()
		b.Run(tc.name, func(b *testing.B) {
			result := runVectorQueryBench(b, tc.corpusSize, tc.vectorDim, tc.realCorpus)
			result.Name = tc.name
			if err := appendBenchResult(os.Getenv(envBenchJSON), result); err != nil {
				b.Errorf("write %s: %v", envBenchJSON, err)
			}
		})
		totals = append(totals, struct {
			name  string
			total time.Duration
		}{tc.name, time.Since(caseStart)})
	}

	b.Log("")
	b.Log("wall-time summary")
	for _, t := range totals {
		b.Logf("  %-14s %v", t.name, t.total)
	}
	b.Logf("  %-14s %v", "suite total", time.Since(suiteStart))
}

const (
	envBenchCases       = "MINNOW_BENCH_CASES"
	envBenchMemoryLimit = "MINNOW_BENCH_MEMORY_LIMIT"
	envBenchJSON        = "MINNOW_BENCH_JSON"
)

type benchCase struct {
	name       string
	corpusSize int
	vectorDim  int
	realCorpus bool
}

// BenchResult is one case's measurements, appended as JSON lines so a CI run
// can be compared against a previous one without reparsing test output.
type BenchResult struct {
	Name        string  `json:"name"`
	CorpusSize  int     `json:"corpus_size"`
	VectorDim   int     `json:"vector_dim"`
	RealCorpus  bool    `json:"real_corpus"`
	Embedder    string  `json:"embedder"`
	MemoryLimit string  `json:"memory_limit"`
	Samples     int     `json:"samples"`
	SeedSeconds float64 `json:"seed_seconds"`
	SeedDocsSec float64 `json:"seed_docs_per_sec"`
	QueriesSec  float64 `json:"queries_per_sec"`
	P50Millis   float64 `json:"p50_ms"`
	P90Millis   float64 `json:"p90_ms"`
	P99Millis   float64 `json:"p99_ms"`
	MaxMillis   float64 `json:"max_ms"`
}

func defaultBenchCases() []benchCase {
	return []benchCase{
		{"10k_dim384", 10_000, 384, false},
		{"100k_dim384", 100_000, 384, false},
		{"1M_dim384", 1_000_000, 384, false},
		{"10k_dim512", 10_000, 512, false},
		{"100k_dim512", 100_000, 512, false},
		{"1M_dim512", 1_000_000, 512, false},
		{"10k_dim768", 10_000, 768, false},
		{"100k_dim768", 100_000, 768, false},
		{"1M_dim768", 1_000_000, 768, false},
		{"10k_real_dim384", 10_000, 384, true},
		{"10k_real_dim512", 10_000, 512, true},
		{"10k_real_dim768", 10_000, 768, true},
	}
}

// benchCases parses a comma-separated list like "2M_dim384,5M_dim384". An empty
// spec keeps the default suite, so an unset variable runs what it always ran.
func benchCases(spec string) ([]benchCase, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return defaultBenchCases(), nil
	}
	var cases []benchCase
	for _, name := range strings.Split(spec, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		tc, err := parseBenchCase(name)
		if err != nil {
			return nil, err
		}
		cases = append(cases, tc)
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("no cases in %q", spec)
	}
	return cases, nil
}

var benchCaseRE = regexp.MustCompile(`^(\d+)([kM]?)(_real)?_dim(\d+)$`)

func parseBenchCase(name string) (benchCase, error) {
	m := benchCaseRE.FindStringSubmatch(name)
	if m == nil {
		return benchCase{}, fmt.Errorf("%q is not <count>[k|M][_real]_dim<n>", name)
	}
	size, err := strconv.Atoi(m[1])
	if err != nil {
		return benchCase{}, fmt.Errorf("%q: %w", name, err)
	}
	switch m[2] {
	case "k":
		size *= 1_000
	case "M":
		size *= 1_000_000
	}
	dim, err := strconv.Atoi(m[4])
	if err != nil {
		return benchCase{}, fmt.Errorf("%q: %w", name, err)
	}
	if size <= 0 || dim <= 0 {
		return benchCase{}, fmt.Errorf("%q: count and dim must be positive", name)
	}
	return benchCase{name: name, corpusSize: size, vectorDim: dim, realCorpus: m[3] != ""}, nil
}

func benchMemoryLimit() string {
	if v := strings.TrimSpace(os.Getenv(envBenchMemoryLimit)); v != "" {
		return v
	}
	return "16GB"
}

func appendBenchResult(path string, result BenchResult) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(encoded, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func runVectorQueryBench(b *testing.B, corpusSize, vectorDim int, realCorpus bool) BenchResult {
	const (
		topK    = 10
		warmups = 100
		samples = 1_000
		kbID    = "bench-kb"
	)

	ctx := context.Background()

	blobRoot := filepath.Join(b.TempDir(), "blobs")
	require.NoError(b, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(b.TempDir(), "cache")
	require.NoError(b, os.MkdirAll(cacheDir, 0o755))

	blobStore := &kb.LocalBlobStore{Root: blobRoot}
	manifestStore := &kb.BlobManifestStore{Store: blobStore}
	embedder, embedderLabel := pickEmbedder(b, vectorDim, realCorpus)
	memoryLimit := benchMemoryLimit()

	loader := kb.NewKB(blobStore, cacheDir,
		kb.WithEmbedder(embedder),
		kb.WithManifestStore(manifestStore),
	)

	af, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
		BlobStore:      loader.BlobStore,
		ManifestStore:  loader.ManifestStore,
		CacheDir:       cacheDir,
		MemoryLimit:    memoryLimit,
		ShardingPolicy: loader.ShardingPolicy,
		Embed:          loader.Embed,
		GraphBuilder:   func() *kb.GraphBuilder { return loader.GraphBuilder },
		EvictCacheIfNeeded: func(ctx context.Context, protectKBID string) error {
			return loader.EvictCacheIfNeeded(ctx, protectKBID)
		},
		LockFor: loader.LockFor,
		AcquireWriteLease: func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
			return loader.AcquireWriteLease(ctx, kbID)
		},
		EnqueueReplacedShardsForGC: loader.EnqueueReplacedShardsForGC,
		Metrics:                    loader,
	})
	require.NoError(b, err)
	require.NoError(b, loader.RegisterFormat(af))

	var docs []kb.Document
	var queryTexts []string
	if realCorpus {
		needed := corpusSize + warmups + samples
		all, err := loadRealCorpus(0)
		if err != nil {
			b.Skipf("real corpus unavailable: %v (run: go run ./scripts/fetch_corpus/)", err)
		}
		good := collectEmbeddableDocs(ctx, embedder, all, needed)
		if len(good) < needed {
			b.Skipf("real corpus has %d embeddable chunks after filter, need %d", len(good), needed)
		}
		docs = good[:corpusSize]
		corpusSize = len(docs)
		overflow := good[len(docs):]
		queryTexts = make([]string, 0, warmups+samples)
		for i := range warmups + samples {
			queryTexts = append(queryTexts, overflow[i%len(overflow)].Text)
		}
	} else {
		docs = make([]kb.Document, corpusSize)
		for i := range docs {
			docs[i] = kb.Document{ID: fmt.Sprintf("doc-%07d", i), Text: fmt.Sprintf("doc-%07d", i)}
		}
	}

	seedStart := time.Now()
	require.NoError(b, loader.UpsertDocsAndUpload(ctx, kbID, docs))
	seedElapsed := time.Since(seedStart)

	queryVecs := make([][]float32, warmups+samples)
	if realCorpus {
		for i, text := range queryTexts {
			vec, err := embedder.Embed(ctx, text)
			require.NoError(b, err)
			queryVecs[i] = vec
		}
	} else {
		rng := rand.New(rand.NewSource(42))
		for i := range queryVecs {
			vec := make([]float32, vectorDim)
			for j := range vec {
				vec[j] = float32(rng.NormFloat64())
			}
			queryVecs[i] = normalizeVec(vec)
		}
	}

	warmupStart := time.Now()
	for i := range warmups {
		_, err := af.QueryRag(ctx, kb.RagQueryRequest{
			KBID:     kbID,
			QueryVec: queryVecs[i],
			Options:  kb.RagQueryOptions{TopK: topK},
		})
		require.NoError(b, err)
	}
	warmupElapsed := time.Since(warmupStart)

	b.ResetTimer()

	durations := make([]time.Duration, 0, samples)
	runStart := time.Now()
	for i := range samples {
		qs := time.Now()
		_, err := af.QueryRag(ctx, kb.RagQueryRequest{
			KBID:     kbID,
			QueryVec: queryVecs[warmups+i],
			Options:  kb.RagQueryOptions{TopK: topK},
		})
		durations = append(durations, time.Since(qs))
		require.NoError(b, err)
	}
	runElapsed := time.Since(runStart)

	b.StopTimer()

	slices.Sort(durations)
	pct := func(p int) time.Duration { return durations[(len(durations)*p)/100] }

	b.Logf("corpus=%d dim=%d topk=%d embedder=%s memory_limit=%s warmups=%d samples=%d",
		corpusSize, vectorDim, topK, embedderLabel, memoryLimit, warmups, samples)
	b.Logf("seed:    %v  (%.1f docs/s)",
		seedElapsed, float64(corpusSize)/seedElapsed.Seconds())
	b.Logf("warmup:  %v", warmupElapsed)
	b.Logf("measure: %v  (%.1f qps)",
		runElapsed, float64(samples)/runElapsed.Seconds())
	b.Logf("latency  p50=%v  p90=%v  p99=%v  max=%v",
		pct(50), pct(90), pct(99), durations[len(durations)-1])

	ms := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }
	return BenchResult{
		CorpusSize:  corpusSize,
		VectorDim:   vectorDim,
		RealCorpus:  realCorpus,
		Embedder:    embedderLabel,
		MemoryLimit: memoryLimit,
		Samples:     samples,
		SeedSeconds: seedElapsed.Seconds(),
		SeedDocsSec: float64(corpusSize) / seedElapsed.Seconds(),
		QueriesSec:  float64(samples) / runElapsed.Seconds(),
		P50Millis:   ms(pct(50)),
		P90Millis:   ms(pct(90)),
		P99Millis:   ms(pct(99)),
		MaxMillis:   ms(durations[len(durations)-1]),
	}
}

// collectEmbeddableDocs returns up to needed documents that the embedder
// accepts. Short/empty chunks are rejected by a cheap heuristic first. the
// embedder is only called on the remainder. Returns early once `needed` good
// docs have been found, so Ollama-backed embedders aren't asked to pre-embed
// the entire corpus just to filter a handful of bad chunks.
func collectEmbeddableDocs(ctx context.Context, embedder kb.Embedder, in []kb.Document, needed int) []kb.Document {
	out := make([]kb.Document, 0, needed)
	for _, d := range in {
		if !looksEmbeddable(d.Text) {
			continue
		}
		if _, err := embedder.Embed(ctx, d.Text); err != nil {
			continue
		}
		out = append(out, d)
		if len(out) >= needed {
			return out
		}
	}
	return out
}

// looksEmbeddable is a cheap pre-filter: require a minimum length and at least
// one run of 4+ consecutive letters. Keeps junk (pure whitespace, digits-only
// page numbers, short stopword blocks) out of the embedder path.
func looksEmbeddable(text string) bool {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 40 {
		return false
	}
	run := 0
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			run++
			if run >= 4 {
				return true
			}
			continue
		}
		run = 0
	}
	return false
}

// pickEmbedder returns the embedder to use for a case plus a short label.
// Synthetic cases use the FNV-hash fixture embedder (uniform pseudo-random
// vectors). Real-corpus cases use a real embedder that produces clustered
// vectors: Ollama all-minilm at 384 dim, the in-repo LocalEmbedder at 768.
// If Ollama is selected but unreachable, the bench skips cleanly.
func pickEmbedder(b *testing.B, vectorDim int, realCorpus bool) (kb.Embedder, string) {
	if !realCorpus {
		return mustBenchLocalEmbedder(b, vectorDim), fmt.Sprintf("local-subword-%d", vectorDim)
	}
	if vectorDim == 768 {
		const model = "nomic-embed-text"
		emb := kb.NewOllamaEmbedder("http://localhost:11434", model)
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := emb.Ping(pingCtx); err != nil {
			b.Skipf("ollama unavailable: %v (start ollama and `ollama pull %s`)", err, model)
		}
		return emb, "ollama-" + strings.TrimSuffix(model, ":latest")
	}
	le, err := kb.NewLocalEmbedder(vectorDim)
	require.NoError(b, err)
	return le, fmt.Sprintf("local-subword-%d", vectorDim)
}

// loadRealCorpus reads JSONL files produced by scripts/fetch_corpus, returning
// up to limit Documents. Files are globbed from testdata/corpus/10k-filings/
// relative to the nearest ancestor with a go.mod file.
func loadRealCorpus(limit int) ([]kb.Document, error) {
	root, err := findRepoRoot()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "testdata", "corpus", "10k-filings")
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no corpus files under %s", dir)
	}
	slices.Sort(matches)

	docs := make([]kb.Document, 0, limit)
	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for sc.Scan() {
			var rec struct {
				ID   string `json:"id"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				f.Close()
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			docs = append(docs, kb.Document{ID: rec.ID, Text: rec.Text})
			if limit > 0 && len(docs) >= limit {
				f.Close()
				return docs, nil
			}
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("corpus files empty")
	}
	return docs, nil
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from cwd upward")
		}
		dir = parent
	}
}

func normalizeVec(vec []float32) []float32 {
	var sumSq float32
	for _, v := range vec {
		sumSq += v * v
	}
	norm := float32(math.Sqrt(float64(sumSq)))
	if norm == 0 {
		return vec
	}
	normalized := make([]float32, len(vec))
	for i, v := range vec {
		normalized[i] = v / norm
	}
	return normalized
}

func mustBenchLocalEmbedder(b *testing.B, dim int) kb.Embedder {
	b.Helper()
	embedder, err := kb.NewLocalEmbedder(dim)
	require.NoError(b, err)
	return embedder
}
