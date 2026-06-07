package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/codeindex"
	"github.com/mikills/minnow/kb/eventing"
	"github.com/mikills/minnow/kb/graphbuild"
	"github.com/mikills/minnow/kb/lease"
	"github.com/mikills/minnow/kb/localembedder"
	"github.com/mikills/minnow/kb/manifest"
	mediapkg "github.com/mikills/minnow/kb/media"
	"github.com/mikills/minnow/kb/mutationretry"
	"github.com/mikills/minnow/kb/ollama"
	"github.com/mikills/minnow/kb/openaiembedder"
	schedulerpkg "github.com/mikills/minnow/kb/scheduler"
	"github.com/mikills/minnow/kb/sharding"
	"github.com/mikills/minnow/kb/textsplit"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	logKeyError               = "error"
	logKeyKBID                = "kb_id"
	logKeyEventID             = "event_id"
	logKeyReason              = "reason"
	logKeyDurationMS          = "duration_ms"
	DefaultCodeEmbedBatchSize = codeindex.DefaultEmbedBatchSize
	DefaultCodeMaxBatchBytes  = codeindex.DefaultMaxBatchBytes
	DefaultCodeMaxHeapBytes   = codeindex.DefaultMaxHeapBytes
	DefaultCodeMaxRSSBytes    = codeindex.DefaultMaxRSSBytes
	DefaultCodeThrottle       = codeindex.DefaultThrottle
	DefaultCodeLargeRepoFiles = codeindex.DefaultLargeRepoFiles
	DefaultTextChunkSize      = textsplit.DefaultChunkSize
)

type (
	CodeHookOptions         = codeindex.CodeHookOptions
	CodeHookStatus          = codeindex.CodeHookStatus
	CodeChunkMetadata       = codeindex.ChunkMetadata
	codeScannedFile         = codeindex.ScannedFile
	CodeIndexResourcePolicy = codeindex.ResourcePolicy
)

var CodeHookNames = codeindex.CodeHookNames

func InstallCodeIndexHooks(ctx context.Context, opts CodeHookOptions) (CodeHookStatus, error) {
	return codeindex.InstallCodeIndexHooks(ctx, opts)
}

func UninstallCodeIndexHooks(ctx context.Context, root string) (CodeHookStatus, error) {
	return codeindex.UninstallCodeIndexHooks(ctx, root)
}

func CodeIndexHookStatus(ctx context.Context, root string) (CodeHookStatus, error) {
	return codeindex.CodeIndexHookStatus(ctx, root)
}

func buildCodeDocuments(
	ctx context.Context,
	root, repoID string,
	file codeScannedFile,
	opts CodeIndexOptions,
) ([]Document, []CodeChunkMetadata, error) {
	docs, metas, err := codeindex.BuildDocuments(ctx, root, repoID, file, opts)
	if err != nil {
		return nil, nil, err
	}
	out := make([]Document, 0, len(docs))
	for _, doc := range docs {
		out = append(out, Document{ID: doc.ID, Text: doc.Text, Metadata: doc.Metadata})
	}
	return out, metas, nil
}

type TextChunker struct{ ChunkSize int }

func (c TextChunker) Chunk(ctx context.Context, docID string, text string) ([]Chunk, error) {
	chunks, err := textsplit.Splitter{ChunkSize: c.ChunkSize}.Chunk(ctx, docID, text)
	if err != nil {
		return nil, err
	}
	out := make([]Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		out = append(
			out,
			Chunk{DocID: chunk.DocID, ChunkID: chunk.ChunkID, Text: chunk.Text, Start: chunk.Start, End: chunk.End},
		)
	}
	return out, nil
}

type ManifestDocument = manifest.Document
type ManifestStore = manifest.Store
type BlobManifestStore = manifest.BlobStoreManifest
type MongoManifestStore = manifest.MongoStore

func NewMongoManifestStore(collection *mongo.Collection) *MongoManifestStore {
	return manifest.NewMongoStore(collection)
}

type (
	SchedulerOutcome       = schedulerpkg.Outcome
	SchedulerObserver      = schedulerpkg.Observer
	SchedulerJob           = schedulerpkg.Job
	Scheduler              = schedulerpkg.Scheduler
	RedisWriteLeaseManager = lease.RedisManager
)

const (
	DefaultSchedulerStopTimeout  = schedulerpkg.DefaultStopTimeout
	SchedulerOutcomeSuccess      = schedulerpkg.OutcomeSuccess
	SchedulerOutcomeSkippedLease = schedulerpkg.OutcomeSkippedLease
	SchedulerOutcomeFailed       = schedulerpkg.OutcomeFailed
)

func NewScheduler(
	leaseMgr WriteLeaseManager,
	tickEvery time.Duration,
	disabled []string,
	observer SchedulerObserver,
) *Scheduler {
	return schedulerpkg.New(leaseMgr, tickEvery, disabled, observer)
}

func NewRedisWriteLeaseManager(client redis.UniversalClient, prefix string) (*RedisWriteLeaseManager, error) {
	return lease.NewRedisManager(client, prefix)
}

type (
	MediaState         = mediapkg.MediaState
	MediaLocator       = mediapkg.MediaLocator
	ChunkMediaRef      = mediapkg.ChunkMediaRef
	MediaObject        = mediapkg.MediaObject
	MediaPage          = mediapkg.MediaPage
	MediaUploadInput   = mediapkg.MediaUploadInput
	MediaUploadResult  = mediapkg.MediaUploadResult
	InMemoryMediaStore = mediapkg.InMemoryMediaStore
	MongoMediaStore    = mediapkg.MongoMediaStore
)

const (
	MediaStatePending           = mediapkg.MediaStatePending
	MediaStateActive            = mediapkg.MediaStateActive
	MediaStateTombstoned        = mediapkg.MediaStateTombstoned
	DefaultMaxUploadBytes int64 = mediapkg.DefaultMaxUploadBytes
	MediaPageDefaultLimit       = mediapkg.MediaPageDefaultLimit
	MediaPageMaxLimit           = mediapkg.MediaPageMaxLimit
)

type MediaStore interface {
	Put(ctx context.Context, m MediaObject) error
	Get(ctx context.Context, mediaID string) (*MediaObject, error)
	FindByIdempotency(ctx context.Context, kbID, idempotencyKey string) (*MediaObject, error)
	List(ctx context.Context, kbID, prefix string, after string, limit int) (MediaPage, error)
	UpdateState(ctx context.Context, mediaID string, state MediaState, tombstonedAtMs int64) error
	ListByState(ctx context.Context, kbID string, state MediaState, after string, limit int) (MediaPage, error)
	Delete(ctx context.Context, mediaID string) error
}

var ErrMediaNotFound = mediapkg.ErrMediaNotFound
var ErrMediaDuplicateKey = mediapkg.ErrMediaDuplicateKey

func NewInMemoryMediaStore() *InMemoryMediaStore { return mediapkg.NewInMemoryMediaStore() }
func NewMongoMediaStore(ctx context.Context, coll *mongo.Collection) (*MongoMediaStore, error) {
	return mediapkg.NewMongoMediaStore(ctx, coll)
}
func MediaBlobKey(kbID, mediaID, filename string) string {
	return mediapkg.MediaBlobKey(kbID, mediaID, filename)
}
func SanitizeMediaFilename(name string) (string, error) { return mediapkg.SanitizeMediaFilename(name) }
func newMediaID() string                                { return mediapkg.NewIDAt(RealClock.Now()) }
func (l *KB) newMediaID() string                        { return mediapkg.NewIDAt(l.Clock.Now()) }

type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

var RealClock Clock = realClock{}

type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	if t.IsZero() {
		t = time.Unix(0, 0).UTC()
	}
	return &FakeClock{now: t.UTC()}
}
func (c *FakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *FakeClock) Advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }
func (c *FakeClock) Set(t time.Time)         { c.mu.Lock(); defer c.mu.Unlock(); c.now = t.UTC() }
func nowFrom(c Clock) time.Time {
	if c == nil {
		return RealClock.Now()
	}
	return c.Now()
}

type LocalEmbedder = localembedder.Embedder

func NewLocalEmbedder(dim int) (*LocalEmbedder, error) {
	return localembedder.New(dim, ErrInvalidEmbeddingDimension)
}

type OpenAICompatibleEmbedder = openaiembedder.OpenAICompatibleEmbedder
type OpenAICompatibleEmbedderConfig = openaiembedder.OpenAICompatibleEmbedderConfig

func NewOpenAICompatibleEmbedder(cfg OpenAICompatibleEmbedderConfig) (*OpenAICompatibleEmbedder, error) {
	return openaiembedder.NewOpenAICompatibleEmbedder(cfg)
}

type ShardingPolicy = sharding.Policy

func DefaultShardingPolicy() ShardingPolicy { return sharding.DefaultPolicy() }

func NormalizeShardingPolicy(policy ShardingPolicy) ShardingPolicy {
	return sharding.NormalizePolicy(policy)
}

type ShardMetricsObserver interface {
	RecordShardCount(kbID string, count int)
	RecordShardExecutionFailure(kbID string)
	RecordShardFanout(kbID string, fanout int, capped bool)
	RecordShardExecution(kbID string, count int)
	RecordShardCacheAccess(kbID string, hit bool)
}

func (l *KB) RecordShardCount(kbID string, count int) { l.recordShardCount(kbID, count) }

func (l *KB) RecordShardExecutionFailure(kbID string) { l.recordShardExecutionFailure(kbID) }

func (l *KB) RecordShardFanout(kbID string, fanout int, capped bool) {
	l.recordShardFanout(kbID, fanout, capped)
}

func (l *KB) RecordShardExecution(kbID string, count int) { l.recordShardExecution(kbID, count) }

func (l *KB) RecordShardCacheAccess(kbID string, hit bool) { l.recordShardCacheAccess(kbID, hit) }

func (l *KB) recordShardCount(kbID string, shardCount int) {
	l.shardMetrics.RecordCount(kbID, shardCount)
}

func (l *KB) recordShardFanout(kbID string, fanout int, capped bool) {
	l.shardMetrics.RecordFanout(kbID, fanout, capped)
}

func (l *KB) recordShardExecution(kbID string, shardCount int) {
	l.shardMetrics.RecordExecution(kbID, shardCount)
}

func (l *KB) recordShardExecutionFailure(kbID string) { l.shardMetrics.RecordExecutionFailure(kbID) }

func (l *KB) recordCompactionResult(kbID string, duration time.Duration, result *CompactionPublishResult, err error) {
	l.shardMetrics.RecordCompaction(kbID, duration, result != nil && result.Performed, err)
}

func (l *KB) recordManifestCASConflict(kbID string) {
	slog.Default().Warn("manifest CAS conflict", logKeyKBID, kbID, logKeyReason, "manifest_cas_conflict")
	l.shardMetrics.RecordManifestCASConflict(kbID)
}

func (l *KB) recordShardCacheAccess(kbID string, hit bool) {
	l.shardMetrics.RecordCacheAccess(kbID, hit)
}

func (l *KB) ShardingOpenMetricsText() string { return l.shardMetrics.OpenMetricsText() }

const defaultWriteLeaseTTL = lease.DefaultTTL

type WriteLease = lease.Lease

type WriteLeaseManager = lease.Manager

func (l *KB) writeLeaseManagerAndTTL() (WriteLeaseManager, time.Duration) {
	l.mu.Lock()
	leaseManager := l.WriteLeaseManager
	ttl := l.WriteLeaseTTL
	l.mu.Unlock()

	if leaseManager == nil {
		leaseManager = NewInMemoryWriteLeaseManager()
	}
	if ttl <= 0 {
		ttl = defaultWriteLeaseTTL
	}

	return leaseManager, ttl
}

func (l *KB) AcquireWriteLease(ctx context.Context, kbID string) (WriteLeaseManager, *WriteLease, error) {
	leaseManager, ttl := l.writeLeaseManagerAndTTL()
	lease, err := leaseManager.Acquire(ctx, kbID, ttl)
	if err != nil {
		if errors.Is(err, ErrWriteLeaseConflict) {
			slog.Default().
				WarnContext(ctx, "write lease acquisition conflict", logKeyKBID, kbID, logKeyReason, "lease_conflict", "ttl", ttl.String())
		} else {
			slog.Default().ErrorContext(ctx, "write lease acquisition failed", logKeyKBID, kbID, logKeyReason, "lease_acquire_failed", logKeyError, err)
		}
		return nil, nil, fmt.Errorf("acquire write lease: %w", err)
	}

	return leaseManager, lease, nil
}

type InMemoryWriteLeaseManager = lease.InMemoryManager

func NewInMemoryWriteLeaseManager() *InMemoryWriteLeaseManager {
	return lease.NewInMemoryManager()
}

type OllamaEmbedder = ollama.Embedder

func NewOllamaEmbedder(baseURL, model string) *OllamaEmbedder {
	return ollama.NewEmbedder(baseURL, model)
}

type OllamaGrapher = ollama.Grapher

func NewOllamaGrapher(baseURL, model string) *OllamaGrapher {
	return ollama.NewGrapher(baseURL, model)
}

type (
	EventKind          = eventing.EventKind
	EventStatus        = eventing.EventStatus
	KBEvent            = eventing.Event
	EventStore         = eventing.Store
	TransactionRunner  = eventing.TransactionRunner
	InMemoryEventStore = eventing.InMemoryStore
	ErrInvalidEvent    = eventing.ErrInvalidEvent
	ErrInboxDuplicate  = eventing.ErrInboxDuplicate
	EventInbox         = eventing.EventInbox
	InMemoryEventInbox = eventing.InMemoryEventInbox
	MongoEventStore    = eventing.MongoStore
	MongoEventInbox    = eventing.MongoEventInbox
)

const (
	EventMediaUpload            = eventing.EventMediaUpload
	EventDocumentUpsert         = eventing.EventDocumentUpsert
	EventMediaUploaded          = eventing.EventMediaUploaded
	EventDocumentChunked        = eventing.EventDocumentChunked
	EventDocumentEmbedded       = eventing.EventDocumentEmbedded
	EventDocumentGraphExtracted = eventing.EventDocumentGraphExtracted
	EventKBPublished            = eventing.EventKBPublished
	EventWorkerFailed           = eventing.EventWorkerFailed
	EventStatusPending          = eventing.EventStatusPending
	EventStatusClaimed          = eventing.EventStatusClaimed
	EventStatusDone             = eventing.EventStatusDone
	EventStatusFailed           = eventing.EventStatusFailed
	EventStatusDead             = eventing.EventStatusDead
	DefaultEventMaxAttempts     = eventing.DefaultEventMaxAttempts
)

var (
	ErrEventDuplicateKey  = eventing.ErrDuplicateKey
	ErrEventNoneAvailable = eventing.ErrNoneAvailable
	ErrEventNotFound      = eventing.ErrNotFound
	ErrEventStateChanged  = eventing.ErrStateChanged
	NewInMemoryEventStore = eventing.NewInMemoryStore
	IsInboxDuplicate      = eventing.IsInboxDuplicate
	NewInMemoryEventInbox = eventing.NewInMemoryEventInbox
)

func IsEventDuplicateKey(err error) bool { return errors.Is(err, ErrEventDuplicateKey) }

func NewMongoEventStore(ctx context.Context, coll *mongo.Collection, client *mongo.Client) (*MongoEventStore, error) {
	return eventing.NewMongoStore(ctx, coll, client)
}

func NewMongoEventInbox(ctx context.Context, coll *mongo.Collection) (*MongoEventInbox, error) {
	return eventing.NewMongoEventInbox(ctx, coll)
}

func BuildInClausePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}

func MapKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func FileContentSHA256(ctx context.Context, path string) (string, error) {
	return blobstore.FileContentSHA256(ctx, path)
}

func ExpandedFromVector(results []QueryResult) []ExpandedResult {
	expanded := make([]ExpandedResult, 0, len(results))
	for _, r := range results {
		sim := float64(1) / (float64(1) + r.Distance)
		expanded = append(expanded, ExpandedResult{
			ID:         r.ID,
			Content:    r.Content,
			Distance:   r.Distance,
			GraphScore: 0,
			Score:      sim,
			MediaRefs:  r.MediaRefs,
			Metadata:   r.Metadata,
		})
	}
	return expanded
}

func TopNEntityScores(scores map[string]float64, n int) map[string]float64 {
	if n <= 0 || len(scores) <= n {
		return scores
	}
	type pair struct {
		id    string
		score float64
	}
	pairs := make([]pair, 0, len(scores))
	for id, score := range scores {
		pairs = append(pairs, pair{id: id, score: score})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score == pairs[j].score {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].score > pairs[j].score
	})
	if len(pairs) > n {
		pairs = pairs[:n]
	}
	result := make(map[string]float64, len(pairs))
	for _, p := range pairs {
		result[p.id] = p.score
	}
	return result
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	return mutationretry.Sleep(ctx, d)
}

type MutationRetryStats = mutationretry.Stats
type MutationRetryObserver = mutationretry.Observer
type MutationRetryObserverFunc = mutationretry.ObserverFunc

func WithMutationRetryObserver(observer MutationRetryObserver) KBOption {
	return func(kb *KB) { kb.RetryObserver = observer }
}

func (l *KB) SetMutationRetryObserver(observer MutationRetryObserver) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.RetryObserver = observer
}

func runWithUploadRetry(
	ctx context.Context,
	operation string,
	maxRetries int,
	observer MutationRetryObserver,
	op func() error,
) error {
	return mutationretry.Run(ctx, mutationretry.RunConfig{
		Operation:  operation,
		MaxRetries: maxRetries,
		Observer:   observer,
		Conflict: func(err error) bool {
			return errors.Is(err, ErrBlobVersionMismatch)
		},
		Op: op,
	})
}

type GraphSink interface {
	EnsureGraphTables(ctx context.Context) error
	InsertGraphBuildResult(ctx context.Context, result *GraphBuildResult) error
}

type EntityCandidate = graphbuild.EntityCandidate
type EdgeCandidate = graphbuild.EdgeCandidate
type GraphExtraction = graphbuild.Extraction
type Grapher = graphbuild.Grapher
type Canonicalizer = graphbuild.Canonicalizer
type GraphEntity = graphbuild.Entity
type GraphEdge = graphbuild.Edge
type EntityChunkMapping = graphbuild.EntityChunkMapping
type GraphBuildResult = graphbuild.BuildResult

type GraphBuilder struct {
	Chunker       Chunker
	Grapher       Grapher
	Canonicalizer Canonicalizer
	BatchSize     int
}

func (b *GraphBuilder) Build(ctx context.Context, docs []Document) (*GraphBuildResult, error) {
	builder, buildDocs, err := b.delegate(docs)
	if err != nil {
		return nil, err
	}
	return builder.Build(ctx, buildDocs)
}

func (b *GraphBuilder) BuildAndInsert(ctx context.Context, sink GraphSink, docs []Document) (*GraphBuildResult, error) {
	builder, buildDocs, err := b.delegate(docs)
	if err != nil {
		return nil, err
	}
	return builder.BuildAndInsert(ctx, sink, buildDocs)
}

func (b *GraphBuilder) delegate(docs []Document) (*graphbuild.Builder, []graphbuild.Document, error) {
	if b == nil {
		return nil, nil, fmt.Errorf("graph builder is nil")
	}
	buildDocs := make([]graphbuild.Document, 0, len(docs))
	for _, doc := range docs {
		buildDocs = append(buildDocs, graphbuild.Document{ID: doc.ID, Text: doc.Text})
	}
	return &graphbuild.Builder{
		Chunker:       b.Chunker,
		Grapher:       b.Grapher,
		Canonicalizer: b.Canonicalizer,
		BatchSize:     b.BatchSize,
	}, buildDocs, nil
}

const (
	ShardGCJobID            = "shard-gc"
	ShardGCJobExpr          = "*/2 * * * *"
	EventReaperJobID        = "event-reaper"
	EventReaperJobExpr      = "* * * * *"
	InboxCleanupJobID       = "inbox-cleanup"
	InboxCleanupJobExpr     = "0 * * * *"
	MediaGCMarkJobID        = "media-gc-mark"
	MediaGCMarkJobExpr      = "*/5 * * * *"
	MediaGCSweepJobID       = "media-gc-sweep"
	MediaGCSweepJobExpr     = "*/15 * * * *"
	MediaUploadAbortJobID   = "media-upload-abort"
	MediaUploadAbortJobExpr = "*/5 * * * *"
	EventCleanupJobID       = "event-cleanup"
	EventCleanupJobExpr     = "0 */6 * * *"
)

const InboxRetention = 7 * 24 * time.Hour
const EventRetention = 30 * 24 * time.Hour

func (l *KB) RegisterDefaultJobs(s *Scheduler) error {
	return schedulerpkg.RegisterDefaultNamedJobs(
		s,
		schedulerpkg.JobIDs{
			ShardGC:          ShardGCJobID,
			EventReaper:      EventReaperJobID,
			EventCleanup:     EventCleanupJobID,
			InboxCleanup:     InboxCleanupJobID,
			MediaGCMark:      MediaGCMarkJobID,
			MediaGCSweep:     MediaGCSweepJobID,
			MediaUploadAbort: MediaUploadAbortJobID,
		},
		schedulerpkg.JobExpressions{
			ShardGC:          ShardGCJobExpr,
			EventReaper:      EventReaperJobExpr,
			EventCleanup:     EventCleanupJobExpr,
			InboxCleanup:     InboxCleanupJobExpr,
			MediaGCMark:      MediaGCMarkJobExpr,
			MediaGCSweep:     MediaGCSweepJobExpr,
			MediaUploadAbort: MediaUploadAbortJobExpr,
		},
		schedulerpkg.EnabledJobs{
			ShardGC:          true,
			EventJobs:        l.EventStore != nil,
			InboxCleanup:     l.EventInbox != nil,
			MediaJobs:        l.MediaStore != nil,
			MediaUploadAbort: l.EventStore != nil && l.BlobStore != nil,
		},
		l.runScheduledJob,
	)
}

func (l *KB) runScheduledJob(ctx context.Context, jobID string) error {
	switch jobID {
	case ShardGCJobID:
		_, err := l.SweepDelayedShardGC(ctx, time.Time{})
		return err
	case EventReaperJobID:
		_, err := l.EventStore.Requeue(ctx, l.Clock.Now())
		return err
	case EventCleanupJobID:
		_, err := l.EventStore.Cleanup(ctx, l.Clock.Now().Add(-EventRetention))
		return err
	case InboxCleanupJobID:
		_, err := l.EventInbox.Cleanup(ctx, l.Clock.Now().Add(-InboxRetention))
		return err
	case MediaGCMarkJobID:
		_, err := l.SweepMediaGCMark(ctx, time.Time{})
		return err
	case MediaGCSweepJobID:
		_, err := l.SweepMediaGCDelete(ctx, time.Time{})
		return err
	case MediaUploadAbortJobID:
		_, err := l.SweepAbortedMediaUploads(ctx, time.Time{})
		return err
	default:
		return fmt.Errorf("scheduler: unknown default job %q", jobID)
	}
}
