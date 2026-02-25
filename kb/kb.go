package kb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Embedder generates embeddings for text inputs.
type Embedder interface {
	Embed(ctx context.Context, input string) ([]float32, error)
}

// QueryResult represents a single vector search result.
type QueryResult struct {
	ID       string  `json:"id"`       // Document ID
	Content  string  `json:"content"`  // Document text stored at ingestion time
	Distance float64 `json:"distance"` // Euclidean distance from query vector
}

// Document is a source document for ingestion pipelines.
type Document struct {
	ID   string
	Text string
}

// Chunk is a text segment with provenance.
type Chunk struct {
	DocID   string
	ChunkID string
	Text    string
	Start   int
	End     int
}

// Chunker produces chunks from raw text.
type Chunker interface {
	Chunk(ctx context.Context, docID string, text string) ([]Chunk, error)
}

// KB is the core knowledge base orchestrator for loading, querying,
// mutating, uploading, and maintaining HNSW indices.
type KB struct {
	BlobStore     BlobStore
	ManifestStore ManifestStore
	CacheDir      string
	MemoryLimit   string
	ExtensionDir  string
	OfflineExt    bool
	Embedder      Embedder
	GraphBuilder  *GraphBuilder

	WriteLeaseManager WriteLeaseManager
	WriteLeaseTTL     time.Duration
	RetryObserver     MutationRetryObserver
	ShardingPolicy    ShardingPolicy
	MaxCacheBytes     int64
	CacheEntryTTL     time.Duration

	cacheBytesCurrent        int64
	cacheEvictionsTTLTotal   uint64
	cacheEvictionsSizeTotal  uint64
	cacheEvictionErrorsTotal uint64
	cacheBudgetExceededTotal uint64
	shardMetricsByKB         map[string]shardMetrics

	mu      sync.Mutex
	locks   map[string]*sync.Mutex
	shardGC []delayedShardGCEntry
}

// KBOption configures KB instances.
type KBOption func(*KB)

// WithMemoryLimit sets the memory limit for DuckDB.
func WithMemoryLimit(limit string) KBOption {
	return func(kb *KB) {
		if limit != "" {
			kb.MemoryLimit = limit
		}
	}
}

// WithEmbedder sets the embedder for generating document embeddings.
func WithEmbedder(embedder Embedder) KBOption {
	return func(kb *KB) {
		kb.Embedder = embedder
	}
}

// DefaultExtensionDir is the directory name for pre-downloaded DuckDB extensions.
const DefaultExtensionDir = ".duckdb/extensions"

// resolveExtensionDir walks up from the working directory to find a
// DefaultExtensionDir directory. Returns the absolute path if found, or "".
func resolveExtensionDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		candidate := filepath.Join(dir, DefaultExtensionDir)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// WithDuckDBExtensionDir sets the root directory for pre-downloaded DuckDB extensions.
func WithDuckDBExtensionDir(dir string) KBOption {
	return func(kb *KB) {
		kb.ExtensionDir = dir
	}
}

// WithDuckDBOfflineExtensions controls offline extension mode.
// When true, extensions are only LOADed (never INSTALLed at runtime).
func WithDuckDBOfflineExtensions(offline bool) KBOption {
	return func(kb *KB) {
		kb.OfflineExt = offline
	}
}

// WithManifestStore sets a custom ManifestStore implementation.
func WithManifestStore(store ManifestStore) KBOption {
	return func(kb *KB) {
		kb.ManifestStore = store
	}
}

// WithGraphBuilder sets the graph builder for RAG functionality.
func WithGraphBuilder(builder *GraphBuilder) KBOption {
	return func(kb *KB) {
		kb.GraphBuilder = builder
	}
}

// WithWriteLeaseManager sets the write lease manager for distributed coordination.
func WithWriteLeaseManager(mgr WriteLeaseManager) KBOption {
	return func(kb *KB) {
		if mgr == nil {
			kb.WriteLeaseManager = NewInMemoryWriteLeaseManager()
			return
		}
		kb.WriteLeaseManager = mgr
	}
}

// WithWriteLeaseTTL sets the TTL for write leases.
func WithWriteLeaseTTL(ttl time.Duration) KBOption {
	return func(kb *KB) {
		if ttl <= 0 {
			kb.WriteLeaseTTL = defaultWriteLeaseTTL
			return
		}
		kb.WriteLeaseTTL = ttl
	}
}

// WithShardingPolicy sets sharding/query/compaction policy thresholds.
func WithShardingPolicy(policy ShardingPolicy) KBOption {
	return func(kb *KB) {
		kb.ShardingPolicy = normalizeShardingPolicy(policy)
	}
}

// WithMaxCacheBytes configures on-disk cache size limit for snapshots.
func WithMaxCacheBytes(max int64) KBOption {
	return func(kb *KB) {
		kb.MaxCacheBytes = max
	}
}

// WithCacheEntryTTL configures time-based eviction for cached KB snapshots.
func WithCacheEntryTTL(ttl time.Duration) KBOption {
	return func(kb *KB) {
		kb.CacheEntryTTL = ttl
	}
}

// NewKB creates a new KB instance with the given blob store and cache directory.
func NewKB(bs BlobStore, cacheDir string, opts ...KBOption) *KB {
	kb := &KB{
		BlobStore:         bs,
		CacheDir:          cacheDir,
		MemoryLimit:       "128MB",
		ExtensionDir:      resolveExtensionDir(),
		OfflineExt:        true,
		WriteLeaseManager: NewInMemoryWriteLeaseManager(),
		WriteLeaseTTL:     defaultWriteLeaseTTL,
		ShardingPolicy:    normalizeShardingPolicy(ShardingPolicy{}),
		locks:             make(map[string]*sync.Mutex),
		shardMetricsByKB:  make(map[string]shardMetrics),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(kb)
		}
	}

	if kb.ManifestStore == nil {
		kb.ManifestStore = &BlobManifestStore{Store: kb.BlobStore}
	}

	return kb
}

// NewKBWithMemLimit creates a KB with a specific memory limit.
func NewKBWithMemLimit(bs BlobStore, cacheDir, memLimit string) *KB {
	return NewKB(bs, cacheDir, WithMemoryLimit(memLimit))
}

// NewKBWithEmbedder creates a KB with a specific embedder.
func NewKBWithEmbedder(bs BlobStore, cacheDir, memLimit string, embedder Embedder) *KB {
	return NewKB(bs, cacheDir, WithMemoryLimit(memLimit), WithEmbedder(embedder))
}

func (l *KB) SetWriteLeaseManager(mgr WriteLeaseManager) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if mgr == nil {
		l.WriteLeaseManager = NewInMemoryWriteLeaseManager()
		return
	}
	l.WriteLeaseManager = mgr
}

func (l *KB) SetWriteLeaseTTL(ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ttl <= 0 {
		l.WriteLeaseTTL = defaultWriteLeaseTTL
		return
	}
	l.WriteLeaseTTL = ttl
}

// SetMaxCacheBytes updates max cache bytes for local snapshot eviction.
func (l *KB) SetMaxCacheBytes(max int64) {
	l.mu.Lock()
	l.MaxCacheBytes = max
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), defaultCacheEvictionRetryWindow)
	defer cancel()
	_ = l.evictCacheIfNeeded(ctx, "")
}

// SetCacheEntryTTL updates TTL for time-based cache eviction.
func (l *KB) SetCacheEntryTTL(ttl time.Duration) {
	l.mu.Lock()
	l.CacheEntryTTL = ttl
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), defaultCacheEvictionRetryWindow)
	defer cancel()
	_ = l.evictCacheIfNeeded(ctx, "")
}

func (l *KB) SetGraphBuilder(builder *GraphBuilder) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.GraphBuilder = builder
}

func (l *KB) lockFor(kbID string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.locks[kbID]; !ok {
		l.locks[kbID] = &sync.Mutex{}
	}
	return l.locks[kbID]
}

func (l *KB) Load(ctx context.Context, kbID string) (*sql.DB, error) {
	kbDir := filepath.Join(l.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")

	lock := l.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	if err := l.ensureMutableShardDBLocked(ctx, kbID, kbDir, dbPath, 0, false); err != nil {
		return nil, err
	}
	if err := l.evictCacheIfNeeded(ctx, kbID); err != nil {
		return nil, err
	}

	return l.openConfiguredDB(ctx, dbPath)
}

func (l *KB) openConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	if l.ExtensionDir != "" {
		if _, err := db.Exec(fmt.Sprintf(`SET extension_directory = '%s'`, l.ExtensionDir)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set extension_directory: %w", err)
		}
	}

	if l.OfflineExt {
		// Disable autoinstall to prevent DuckDB from downloading extensions
		// behind the scenes when LOAD can't find them locally.
		if _, err := db.Exec(`SET autoinstall_known_extensions = false`); err != nil {
			db.Close()
			return nil, fmt.Errorf("disable autoinstall: %w", err)
		}
	}

	if _, err := db.Exec(`LOAD vss`); err != nil {
		if l.OfflineExt {
			db.Close()
			return nil, fmt.Errorf("failed to load vss extension in offline mode (check extension_directory %q): %w", l.ExtensionDir, err)
		}
		if _, installErr := db.Exec(`INSTALL vss`); installErr != nil {
			db.Close()
			return nil, fmt.Errorf("failed to install vss: %w", installErr)
		}
		if _, loadErr := db.Exec(`LOAD vss`); loadErr != nil {
			db.Close()
			return nil, fmt.Errorf("failed to load vss after install: %w", loadErr)
		}
	}

	if _, err := db.Exec(`SET hnsw_enable_experimental_persistence = true`); err != nil {
		db.Close()
		return nil, err
	}

	_, err = db.Exec(fmt.Sprintf(`
		SET memory_limit = '%s';
		PRAGMA threads = 1;
	`, l.MemoryLimit))
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// Embed returns an embedding for input using the configured Embedder.
func (k *KB) Embed(ctx context.Context, input string) ([]float32, error) {
	if k.Embedder == nil {
		return nil, fmt.Errorf("embedder is not configured")
	}
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}
	return k.Embedder.Embed(ctx, input)
}
