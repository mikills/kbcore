package kb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/graphbuild"
	"github.com/mikills/minnow/kb/metrics"
)

type Embedder interface {
	Embed(ctx context.Context, input string) ([]float32, error)
}

type BatchEmbedder interface {
	EmbedBatch(ctx context.Context, inputs []string) ([][]float32, error)
}

type QueryResult struct {
	ID        string          `json:"id"`       // Document ID
	Content   string          `json:"content"`  // Document text stored at ingestion time
	Distance  float64         `json:"distance"` // Euclidean distance from query vector
	MediaRefs []ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata  map[string]any  `json:"metadata,omitempty"`
}

type Document struct {
	ID        string
	Text      string
	Embedding []float32      // if non-empty, skips the embedder
	MediaIDs  []string
	MediaRefs []ChunkMediaRef
	Metadata  map[string]any
}

type Chunk = graphbuild.Chunk

type Chunker interface {
	Chunk(ctx context.Context, docID string, text string) ([]Chunk, error)
}

type KB struct {
	BlobStore     BlobStore
	ManifestStore ManifestStore
	CacheDir      string
	Embedder      Embedder
	GraphBuilder  *GraphBuilder

	WriteLeaseManager WriteLeaseManager
	WriteLeaseTTL     time.Duration
	RetryObserver     MutationRetryObserver
	ShardingPolicy    ShardingPolicy
	MaxCacheBytes     int64
	CacheEntryTTL     time.Duration

	Clock Clock

	EventStore EventStore
	EventInbox EventInbox

	MediaStore MediaStore

	MediaGC MediaGCConfig

	MediaContentTypeAllowlist []string

	cacheBytesCurrent        int64
	cacheEvictionsTTLTotal   uint64
	cacheEvictionsSizeTotal  uint64
	cacheEvictionErrorsTotal uint64
	cacheBudgetExceededTotal uint64
	shardMetrics             *metrics.ShardRegistry

	formatMu          sync.RWMutex
	formatRegistry    map[string]ArtifactFormat
	defaultFormatKind string
	initErr           error

	mu      sync.Mutex
	locks   map[string]*sync.Mutex
	shardGC []delayedShardGCEntry
}

type KBOption func(*KB)

func WithEmbedder(embedder Embedder) KBOption {
	return func(kb *KB) {
		kb.Embedder = embedder
	}
}

func WithManifestStore(store ManifestStore) KBOption {
	return func(kb *KB) {
		kb.ManifestStore = store
	}
}

func WithArtifactFormat(format ArtifactFormat) KBOption {
	return func(kb *KB) {
		if err := kb.RegisterFormat(format); err != nil {
			kb.setInitErr(fmt.Errorf("with artifact format: %w", err))
		}
	}
}

func WithFormats(formats ...ArtifactFormat) KBOption {
	return func(kb *KB) {
		for _, f := range formats {
			if err := kb.RegisterFormat(f); err != nil {
				kb.setInitErr(fmt.Errorf("with formats: %w", err))
			}
		}
	}
}

func WithGraphBuilder(builder *GraphBuilder) KBOption {
	return func(kb *KB) {
		kb.GraphBuilder = builder
	}
}

func WithWriteLeaseManager(mgr WriteLeaseManager) KBOption {
	return func(kb *KB) {
		if mgr == nil {
			kb.WriteLeaseManager = NewInMemoryWriteLeaseManager()
			return
		}
		kb.WriteLeaseManager = mgr
	}
}

func WithWriteLeaseTTL(ttl time.Duration) KBOption {
	return func(kb *KB) {
		if ttl <= 0 {
			kb.WriteLeaseTTL = defaultWriteLeaseTTL
			return
		}
		kb.WriteLeaseTTL = ttl
	}
}

func WithShardingPolicy(policy ShardingPolicy) KBOption {
	return func(kb *KB) {
		kb.ShardingPolicy = NormalizeShardingPolicy(policy)
	}
}

func WithCompactionEnabled(enabled bool) KBOption {
	return func(kb *KB) {
		policy := kb.ShardingPolicy
		policy.CompactionEnabled = enabled
		policy.CompactionEnabledSet = true
		kb.ShardingPolicy = NormalizeShardingPolicy(policy)
	}
}

func WithMaxCacheBytes(max int64) KBOption {
	return func(kb *KB) {
		kb.MaxCacheBytes = max
	}
}

func WithCacheEntryTTL(ttl time.Duration) KBOption {
	return func(kb *KB) {
		kb.CacheEntryTTL = ttl
	}
}

func WithEventStore(store EventStore) KBOption {
	return func(kb *KB) { kb.EventStore = store }
}

func WithEventInbox(inbox EventInbox) KBOption {
	return func(kb *KB) { kb.EventInbox = inbox }
}

func WithMediaStore(store MediaStore) KBOption {
	return func(kb *KB) { kb.MediaStore = store }
}

func WithMediaGCConfig(cfg MediaGCConfig) KBOption {
	return func(kb *KB) { kb.MediaGC = cfg }
}

func WithMediaContentTypeAllowlist(list []string) KBOption {
	return func(kb *KB) { kb.MediaContentTypeAllowlist = list }
}

func NewKB(bs BlobStore, cacheDir string, opts ...KBOption) *KB {
	kb := &KB{
		BlobStore:         bs,
		CacheDir:          cacheDir,
		WriteLeaseManager: NewInMemoryWriteLeaseManager(),
		WriteLeaseTTL:     defaultWriteLeaseTTL,
		ShardingPolicy:    NormalizeShardingPolicy(ShardingPolicy{}),
		Clock:             RealClock,
		formatRegistry:    make(map[string]ArtifactFormat),
		locks:             make(map[string]*sync.Mutex),
		shardMetrics:      metrics.NewShardRegistry(),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(kb)
		}
	}

	if kb.ManifestStore == nil {
		kb.ManifestStore = &BlobManifestStore{Store: kb.BlobStore}
	}
	if kb.Clock == nil {
		kb.Clock = RealClock
	}
	kb.propagateClockToDefaults()

	return kb
}

type clockAware interface {
	SetClock(Clock)
}

func (l *KB) propagateClockToDefaults() {
	if l.Clock == nil {
		return
	}
	for _, s := range []any{
		l.WriteLeaseManager,
		l.EventStore,
		l.EventInbox,
		l.MediaStore,
		l.ManifestStore,
		l.BlobStore,
	} {
		if s == nil {
			continue
		}
		if ca, ok := s.(clockAware); ok {
			ca.SetClock(l.Clock)
		}
	}
}

func WithClock(c Clock) KBOption {
	return func(kb *KB) { kb.Clock = c }
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

var cacheMutationRootContext = context.Background()

func (l *KB) SetMaxCacheBytes(max int64) {
	l.mu.Lock()
	l.MaxCacheBytes = max
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(cacheMutationRootContext, defaultCacheEvictionRetryWindow)
	defer cancel()
	if err := l.evictCacheIfNeeded(ctx, ""); err != nil {
		slog.Default().Warn("cache eviction after max bytes update failed", logKeyError, err)
	}
}

func (l *KB) SetCacheEntryTTL(ttl time.Duration) {
	l.mu.Lock()
	l.CacheEntryTTL = ttl
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(cacheMutationRootContext, defaultCacheEvictionRetryWindow)
	defer cancel()
	if err := l.evictCacheIfNeeded(ctx, ""); err != nil {
		slog.Default().Warn("cache eviction after ttl update failed", logKeyError, err)
	}
}

func (l *KB) SetGraphBuilder(builder *GraphBuilder) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.GraphBuilder = builder
}

func (l *KB) LockFor(kbID string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.locks[kbID]; !ok {
		l.locks[kbID] = &sync.Mutex{}
	}
	return l.locks[kbID]
}

func (l *KB) EvictCacheIfNeeded(ctx context.Context, protectKBID string) error {
	return l.evictCacheIfNeeded(ctx, protectKBID)
}

func (l *KB) Close() {
	for _, format := range l.registeredFormatsSnapshot() {
		if c, ok := format.(io.Closer); ok {
			c.Close()
		}
	}
}

func (l *KB) RegisterFormat(format ArtifactFormat) error {
	if format == nil {
		return fmt.Errorf("%w: nil format", ErrInvalidArtifactFormat)
	}
	kind := strings.TrimSpace(format.Kind())
	if kind == "" {
		return fmt.Errorf("%w: empty format kind", ErrInvalidArtifactFormat)
	}

	l.formatMu.Lock()
	defer l.formatMu.Unlock()

	if len(l.formatRegistry) == 0 {
		l.defaultFormatKind = kind
	}

	l.formatRegistry[kind] = format

	return nil
}

func (l *KB) SetDefaultFormatKind(kind string) error {
	l.formatMu.Lock()
	defer l.formatMu.Unlock()

	if _, ok := l.formatRegistry[kind]; !ok {
		return fmt.Errorf("%w: %s", ErrFormatNotRegistered, kind)
	}

	l.defaultFormatKind = kind

	return nil
}

func (l *KB) HasFormat() bool {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()
	return len(l.formatRegistry) > 0
}

func (l *KB) FormatByKind(kind string) ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if f, ok := l.formatRegistry[kind]; ok {
		return f
	}

	return nil
}

func (l *KB) DefaultFormat() ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if f, ok := l.formatRegistry[l.defaultFormatKind]; ok {
		return f
	}

	return nil
}

func (l *KB) resolveSearchFormat(ctx context.Context, kbID string) (ArtifactFormat, error) {
	if err := l.getInitErr(); err != nil {
		return nil, err
	}
	if format := l.singleRegisteredDefaultFormat(); format != nil {
		return format, nil
	}
	return l.resolveFormat(ctx, kbID)
}

func (l *KB) resolveFormat(ctx context.Context, kbID string) (ArtifactFormat, error) {
	if err := l.getInitErr(); err != nil {
		return nil, err
	}
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			if format := l.DefaultFormat(); format != nil {
				return format, nil
			}

			return nil, ErrArtifactFormatNotConfigured
		}

		return nil, err
	}

	return l.resolveFormatByKind(doc.Manifest.FormatKind)
}

func (l *KB) singleRegisteredDefaultFormat() ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()
	if len(l.formatRegistry) != 1 {
		return nil
	}
	return l.formatRegistry[l.defaultFormatKind]
}

func (l *KB) setInitErr(err error) {
	if err == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.initErr == nil {
		l.initErr = err
	}
}

func (l *KB) getInitErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.initErr
}

func (l *KB) resolveFormatByKind(kind string) (ArtifactFormat, error) {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if f, ok := l.formatRegistry[kind]; ok {
		return f, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrFormatNotRegistered, kind)
}

func (l *KB) registeredFormatsSnapshot() []ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if len(l.formatRegistry) == 0 {
		return nil
	}

	formats := make([]ArtifactFormat, 0, len(l.formatRegistry))
	for _, format := range l.formatRegistry {
		formats = append(formats, format)
	}

	return formats
}

func (k *KB) Embed(ctx context.Context, input string) ([]float32, error) {
	if k.Embedder == nil {
		return nil, fmt.Errorf("embedder is not configured")
	}

	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}

	return k.Embedder.Embed(ctx, input)
}

type Compactor interface {
	CompactIfNeeded(ctx context.Context, kbID string) (*CompactionPublishResult, error)
}

func (l *KB) CompactIfNeeded(ctx context.Context, kbID string) (*CompactionPublishResult, error) {
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}
	if c, ok := format.(Compactor); ok {
		return c.CompactIfNeeded(ctx, kbID)
	}
	return &CompactionPublishResult{}, nil
}

type (
	AppMetrics       = metrics.App
	RouteStats       = metrics.RouteStats
	EmbedStats       = metrics.EmbedStats
	QueryStats       = metrics.QueryStats
	IngestStats      = metrics.IngestStats
	WorkerStats      = metrics.WorkerStats
	MediaUploadStats = metrics.MediaUploadStats
	RecentRequest    = metrics.RecentRequest
	RuntimeStats     = metrics.RuntimeStats
	MetricsSnapshot  = metrics.MetricsSnapshot
	NoopAppMetrics   = metrics.NoopApp
	InMemAppMetrics  = metrics.InMemApp
)

var NewInMemAppMetrics = metrics.NewInMemApp

type CompactionPublishResult struct {
	Performed          bool
	ReplacedShards     []SnapshotShardMetadata
	ReplacementShards  []SnapshotShardMetadata
	ManifestVersionOld string
	ManifestVersionNew string
}

type (
	BlobObjectInfo = blobstore.ObjectInfo
	BlobStore      = blobstore.Store
	LocalBlobStore = blobstore.LocalBlobStore
	S3BlobStore    = blobstore.S3BlobStore
)

var NewS3BlobStore = blobstore.NewS3BlobStore
