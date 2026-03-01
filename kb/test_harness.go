package kb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test convention: multi-node tests must use SharedBlobRoot(t) + WithBlobRoot.
// Never use a shared WithTempDir value across multiple harnesses.

// KBTestConfig defines parameters for benchmark KB construction.
type KBTestConfig struct {
	Name             string `json:"name"`
	NumDocs          int    `json:"num_docs"`
	EmbeddingDim     int    `json:"embedding_dim"`
	ContentLength    int    `json:"content_length"`
	BatchSize        int    `json:"batch_size"`
	NumClusters      int    `json:"num_clusters"`
	IndexM           int    `json:"index_m"`
	IndexEFConstruct int    `json:"index_ef_construct"`
}

// TestFixture describes a pre-built benchmark KB stored on disk.
type TestFixture struct {
	Config    KBTestConfig `json:"config"`
	BlobPath  string       `json:"blob_path"`
	DBSize    int64        `json:"db_size"`
	Checksum  string       `json:"checksum"`
	CreatedAt time.Time    `json:"created_at"`
}

// TestHarness provides a fluent API for setting up test environments.
// Use this in tests to reduce boilerplate setup code.
//
// Example:
//
//	harness := NewTestHarness(t, "mykb").
//	    WithEmbedder(NewFixtureEmbedder(384)).
//	    Setup()
//	defer harness.Cleanup()
//
//	results, err := harness.KB().Search(ctx, "mykb", queryVec, &SearchOptions{TopK: 10})
type TestHarness struct {
	t    *testing.T
	kbID string
	kb   *KB
	db   *sql.DB

	// Configuration options
	tempDir      string
	blobRoot     string
	cacheDir     string
	embedder     Embedder
	memLimit     string
	fixturePath  string
	extraOptions []KBOption

	// Internal state
	initialized bool
	cleanedUp   bool
}

// NewTestHarness creates a new test harness for the given KB ID.
// The harness will create temporary directories and clean them up automatically.
func NewTestHarness(t *testing.T, kbID string) *TestHarness {
	return &TestHarness{
		t:        t,
		kbID:     kbID,
		memLimit: "128MB",
	}
}

// WithEmbedder sets a custom embedder for the harness.
func (h *TestHarness) WithEmbedder(embedder Embedder) *TestHarness {
	h.embedder = embedder
	return h
}

// WithMemLimit sets the memory limit for DuckDB (e.g., "128MB", "1GB").
func (h *TestHarness) WithMemLimit(limit string) *TestHarness {
	h.memLimit = limit
	return h
}

// WithOptions adds additional KB options that will be applied when constructing the KB.
// Use this to pass features like HNSW policies, observers, graph builders, etc.
func (h *TestHarness) WithOptions(opts ...KBOption) *TestHarness {
	h.extraOptions = append(h.extraOptions, opts...)
	return h
}

// WithTempDir uses a specific temp directory instead of creating one.
// Useful when you need to persist test data between runs.
// NOTE: this shares both blob root and cache dir. To share only blob storage
// across harnesses (simulating separate nodes), use WithBlobRoot instead.
func (h *TestHarness) WithTempDir(dir string) *TestHarness {
	h.tempDir = dir
	return h
}

// WithBlobRoot sets a shared blob storage root while keeping an independent
// cache directory. Use this when simulating multiple nodes that share remote
// storage but maintain separate local caches.
func (h *TestHarness) WithBlobRoot(dir string) *TestHarness {
	h.blobRoot = dir
	return h
}

// WithFixture copies a pre-built fixture into the harness blob root during Setup.
func (h *TestHarness) WithFixture(fixture *TestFixture) *TestHarness {
	if fixture == nil {
		h.fixturePath = ""
		return h
	}
	h.fixturePath = fixture.BlobPath
	return h
}

// Setup initializes the test environment.
// This creates temporary directories and initializes the KB.
func (h *TestHarness) Setup() *TestHarness {
	if h.initialized {
		h.t.Fatal("Harness already initialized")
	}

	// Create temp directories
	if h.tempDir == "" {
		h.tempDir = h.t.TempDir()
	}
	// only derive blobRoot from tempDir when not explicitly set via WithBlobRoot
	if h.blobRoot == "" {
		h.blobRoot = filepath.Join(h.tempDir, "blobs")
	}
	h.cacheDir = filepath.Join(h.tempDir, "cache")

	if err := os.MkdirAll(h.blobRoot, 0o755); err != nil {
		h.t.Fatalf("Failed to create blob root: %v", err)
	}
	if err := os.MkdirAll(h.cacheDir, 0o755); err != nil {
		h.t.Fatalf("Failed to create cache dir: %v", err)
	}

	if h.fixturePath != "" {
		dst := filepath.Join(h.blobRoot, h.kbID+".duckdb")
		if err := copyHarnessFixture(h.fixturePath, dst); err != nil {
			h.t.Fatalf("Failed to copy fixture into harness: %v", err)
		}
	}

	// Create KB instance
	blobStore := &LocalBlobStore{Root: h.blobRoot}

	// Build options list
	opts := []KBOption{WithMemoryLimit(h.memLimit)}
	if h.embedder != nil {
		opts = append(opts, WithEmbedder(h.embedder))
	}
	opts = append(opts, h.extraOptions...)

	h.kb = NewKB(blobStore, h.cacheDir, opts...)

	h.initialized = true
	return h
}

// Cleanup releases resources and cleans up temporary directories.
// Call this with defer immediately after Setup().
func (h *TestHarness) Cleanup() {
	if h.cleanedUp {
		return
	}

	if h.db != nil {
		h.db.Close()
		h.db = nil
	}

	// KB doesn't have a Close method, but temp dirs are cleaned up by t.TempDir()

	h.cleanedUp = true
}

// DB returns a database connection to the KB.
// The connection is cached - subsequent calls return the same connection.
func (h *TestHarness) DB() *sql.DB {
	return h.DBContext(context.Background())
}

// DBContext returns a database connection to the KB using the provided context.
// The connection is cached - subsequent calls return the same connection.
func (h *TestHarness) DBContext(ctx context.Context) *sql.DB {
	if !h.initialized {
		h.t.Fatal("Harness not initialized. Call Setup() first.")
	}

	if h.db == nil {
		db, err := h.kb.Load(ctx, h.kbID)
		if err != nil {
			h.t.Fatalf("Failed to load KB: %v", err)
		}
		h.db = db
	}

	return h.db
}

// KB returns the KB instance.
func (h *TestHarness) KB() *KB {
	if !h.initialized {
		h.t.Fatal("Harness not initialized. Call Setup() first.")
	}
	return h.kb
}

// KBID returns the KB ID.
func (h *TestHarness) KBID() string {
	return h.kbID
}

// BlobRoot returns the blob storage root directory.
func (h *TestHarness) BlobRoot() string {
	return h.blobRoot
}

// CacheDir returns the cache directory.
func (h *TestHarness) CacheDir() string {
	return h.cacheDir
}

// SharedBlobRoot creates and returns a temporary shared blob storage directory.
// use this when setting up multi-node test scenarios where writer and reader
// harnesses need the same blob store but separate local caches.
//
// example:
//
//	blobRoot := SharedBlobRoot(t)
//	writer := NewTestHarness(t, kbID).WithBlobRoot(blobRoot).WithEmbedder(e).Setup()
//	reader := NewTestHarness(t, kbID).WithBlobRoot(blobRoot).WithEmbedder(e).Setup()
func SharedBlobRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "shared-blobs")
}

func copyHarnessFixture(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}

// testOpenConfiguredDB opens a DuckDB file with the standard configuration
// (vss extension, HNSW persistence, memory limit, threads) using the KB's
// artifact format. Tests should use this instead of the removed KB.openConfiguredDB.
// When the KB has no ArtifactFormat, a temporary DuckDBArtifactFormat is
// created with the KB's config values for test convenience.
func testOpenConfiguredDB(kb *KB, ctx context.Context, dbPath string) (*sql.DB, error) {
	if f, ok := kb.ArtifactFormat.(*DuckDBArtifactFormat); ok {
		return f.openConfiguredDB(ctx, dbPath)
	}
	// Fallback for tests that construct a bare KB{} without NewKB.
	f := &DuckDBArtifactFormat{
		deps: DuckDBArtifactDeps{
			MemoryLimit:  kb.MemoryLimit,
			ExtensionDir: kb.ExtensionDir,
			OfflineExt:   kb.OfflineExt,
		},
	}
	return f.openConfiguredDB(ctx, dbPath)
}

// testLoadVSS loads the vss extension into a raw *sql.DB for test fixture
// builders that bypass openConfiguredDB. It uses the local extension directory
// so no network access is needed.
func testLoadVSS(db *sql.DB) error {
	extDir := resolveExtensionDir()
	if extDir != "" {
		if _, err := db.Exec(fmt.Sprintf(`SET extension_directory = '%s'`, extDir)); err != nil {
			return fmt.Errorf("set extension_directory: %w", err)
		}
	}
	if _, err := db.Exec(`SET autoinstall_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoinstall: %w", err)
	}
	if _, err := db.Exec(`LOAD vss`); err != nil {
		return fmt.Errorf("load vss: %w", err)
	}
	return nil
}
