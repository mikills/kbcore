package kb

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKnowledgeBaseCacheIsReused(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-cache"

	harness := NewTestHarness(t, kbID).
		WithEmbedder(newDemoEmbedder()).
		Setup()
	defer harness.Cleanup()

	if err := buildDemoKB(kbID, harness.BlobRoot(), newDemoEmbedder()); err != nil {
		require.FailNow(t, "test failed", err)
	}

	db, err := harness.KB().Load(ctx, kbID)
	require.NoError(t, err)
	db.Close()

	db, err = harness.KB().Load(ctx, kbID)
	if err != nil {
		require.FailNow(t, "test failed", err)
	}
	defer db.Close()
}

func buildDemoKB(kbID, blobRoot string, embedder Embedder) error {
	if embedder == nil {
		return fmt.Errorf("embedder is required")
	}

	buildDir := filepath.Join(blobRoot, "build", kbID)
	os.MkdirAll(buildDir, 0o755)

	dbPath := filepath.Join(buildDir, "vectors.duckdb")
	db, err := sql.Open("duckdb", dbPath+"?access_mode=read_write")
	if err != nil {
		return err
	}
	defer db.Close()

	if err := testLoadVSS(db); err != nil {
		return err
	}

	// Enable HNSW experimental persistence
	_, err = db.Exec(`SET hnsw_enable_experimental_persistence = true`)
	if err != nil {
		return fmt.Errorf("failed to enable hnsw persistence: %w", err)
	}

	// Create table and insert data
	_, err = db.Exec(`
		CREATE TABLE docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[3]
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	embeddingA, err := embedder.Embed(context.Background(), "hello world")
	if err != nil {
		return fmt.Errorf("failed to embed doc a: %w", err)
	}
	if len(embeddingA) != 3 {
		return fmt.Errorf("expected 3-dimensional embedding for doc a, got %d", len(embeddingA))
	}

	embeddingB, err := embedder.Embed(context.Background(), "goodbye")
	if err != nil {
		return fmt.Errorf("failed to embed doc b: %w", err)
	}
	if len(embeddingB) != 3 {
		return fmt.Errorf("expected 3-dimensional embedding for doc b, got %d", len(embeddingB))
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO docs VALUES
			('a', 'hello world', %s),
			('b', 'goodbye',     %s)
	`, formatVectorForSQL(embeddingA), formatVectorForSQL(embeddingB))
	if _, err := db.Exec(insertSQL); err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	// Create VSS index
	_, err = db.Exec(`
		CREATE INDEX docs_vec_idx
		ON docs
		USING HNSW (embedding)
	`)
	if err != nil {
		return fmt.Errorf("failed to create vss index: %w", err)
	}

	// Close DB to checkpoint WAL before publishing shard artifacts.
	db.Close()

	publisher := NewKB(
		&LocalBlobStore{Root: blobRoot},
		filepath.Join(buildDir, "publisher-cache"),
		WithMemoryLimit("128MB"),
		WithEmbedder(embedder),
		WithDuckDBExtensionDir(TestExtensionDir()),
	)
	if _, err := publisher.UploadSnapshotShardedIfMatch(context.Background(), kbID, dbPath, "", defaultSnapshotShardSize); err != nil {
		return fmt.Errorf("publish sharded snapshot: %w", err)
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

type demoEmbedder struct{}

func newDemoEmbedder() Embedder {
	return demoEmbedder{}
}

func (demoEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	trimmed := strings.TrimSpace(input)
	switch trimmed {
	case "hello world":
		return []float32{0.1, 0.2, 0.3}, nil
	case "goodbye":
		return []float32{0.2, 0.1, 0.4}, nil
	default:
		return nil, fmt.Errorf("unknown demo input: %q", trimmed)
	}
}

type fixtureEmbedder struct {
	dim int
}

func newFixtureEmbedder(dim int) Embedder {
	return fixtureEmbedder{dim: dim}
}

func (e fixtureEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}

	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(input))
	seed := int64(hasher.Sum64())

	rng := rand.New(rand.NewSource(seed))
	vec := make([]float32, e.dim)
	for i := range vec {
		vec[i] = float32(rng.NormFloat64())
	}

	return normalizeVector(vec), nil
}

func normalizeVector(vec []float32) []float32 {
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

// simpleTokenEmbedder produces embeddings by hashing individual tokens.
// Documents sharing tokens get similar vectors, making it suitable for
// correctness tests that verify semantic recall.
type simpleTokenEmbedder struct{ dim int }

func newSimpleTokenEmbedder(dim int) Embedder { return simpleTokenEmbedder{dim: dim} }

func (e simpleTokenEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}
	vec := make([]float32, e.dim)
	for _, tok := range strings.Fields(strings.ToLower(input)) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		vec[int(h.Sum32())%e.dim] += 1.0
	}
	return normalizeVector(vec), nil
}

// TestOfflineExtensionLoad verifies openConfiguredDB behavior with offline
// extension loading. This catches regressions where DuckDB's autoinstall
// silently downloads extensions.
func TestOfflineExtensionLoad(t *testing.T) {
	localExtDir := TestExtensionDir()

	tests := []struct {
		name         string
		extDir       string
		offline      bool
		wantErr      bool
		errContains  string
		skipIfNoExts bool
	}{
		{
			name:         "offline with valid local extensions",
			extDir:       localExtDir,
			offline:      true,
			wantErr:      false,
			skipIfNoExts: true,
		},
		{
			name:        "offline with empty extension dir fails",
			extDir:      "", // replaced with t.TempDir() below
			offline:     true,
			wantErr:     true,
			errContains: "offline mode",
		},
		{
			name:         "online with valid local extensions",
			extDir:       localExtDir,
			offline:      false,
			wantErr:      false,
			skipIfNoExts: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipIfNoExts && localExtDir == "" {
				t.Skip("pre-downloaded extensions not found")
			}

			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.duckdb")

			extDir := tt.extDir
			if extDir == "" {
				extDir = t.TempDir()
			}

			k := &KB{
				MemoryLimit:  "128MB",
				ExtensionDir: extDir,
				OfflineExt:   tt.offline,
			}

			db, err := k.openConfiguredDB(context.Background(), dbPath)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			defer db.Close()

			var loaded bool
			err = db.QueryRow(`SELECT installed FROM duckdb_extensions() WHERE extension_name = 'vss'`).Scan(&loaded)
			require.NoError(t, err)
			require.True(t, loaded, "vss extension should be loaded")
		})
	}
}
