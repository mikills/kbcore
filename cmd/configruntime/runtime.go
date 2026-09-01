// Package configruntime assembles a live minnow deployment from a
// config.Config value: KB, artifact format, HTTP app, scheduler, and
// worker pools. It is the only package that bridges the schema (kb/config)
// to the concrete backends (kb/duckdb, Mongo drivers, local blob store).
package configruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	appcmd "github.com/mikills/minnow/cmd"
	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/blobstore/localjournal"
	"github.com/mikills/minnow/kb/blobstore/tiered"
	"github.com/mikills/minnow/kb/cacheevict"
	"github.com/mikills/minnow/kb/config"
	kbduckdb "github.com/mikills/minnow/kb/duckdb"
	"github.com/mikills/minnow/kb/lease"
	"github.com/mikills/minnow/mcpserver"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BuildOptions tunes how Build assembles the runtime.
type BuildOptions struct {
	// DryRun: construct every object but do not open network connections,
	// bind ports, mutate the filesystem, or start goroutines.
	DryRun bool
	// Logger is used for informational startup logs. If nil, slog.Default().
	Logger *slog.Logger
	// ReplicationJournal replaces the built-in persistent local journal for
	// tiered blob storage. Implementations must satisfy journal.Store's durable
	// payload-ownership contract. It is opened and closed by Runtime.
	ReplicationJournal journal.Store
}

// Runtime is a fully wired but not-yet-started deployment. Call Start to
// take traffic. call Stop to shut down cleanly.
type Runtime struct {
	cfg               *config.Config
	logger            *slog.Logger
	dryRun            bool
	kb                *kb.KB
	format            *kbduckdb.DuckDBArtifactFormat
	app               *appcmd.App
	scheduler         *kb.Scheduler
	workerPools       []*kb.WorkerPool
	cleanups          []func(context.Context) error
	warmCancel        context.CancelFunc
	warmDone          chan struct{}
	budget            *budget.Manager
	tieredStore       *tiered.Store
	customJournal     journal.Store
	backgroundStarted bool
	lifecycleMu       sync.Mutex
	started           bool
	stopping          bool
	stopped           bool
}

const logKeyError = "error"

func (r *Runtime) KB() *kb.KB { return r.kb }

// App returns the HTTP app. Nil is possible only before Build returns.
func (r *Runtime) App() *appcmd.App { return r.app }

// Scheduler returns the configured scheduler, or nil if disabled.
func (r *Runtime) Scheduler() *kb.Scheduler { return r.scheduler }

// WorkerPools returns the configured event worker pools.
func (r *Runtime) WorkerPools() []*kb.WorkerPool { return r.workerPools }

func (r *Runtime) MCPConfig() mcpserver.Config { return mcpConfigFromConfig(r.cfg) }

// Build constructs the deployment. It does not open the HTTP listener, mutate
// the filesystem, connect to Mongo in dry-run mode, or start any goroutine.
func Build(ctx context.Context, cfg *config.Config, opts BuildOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configruntime: cfg must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	rt := &Runtime{cfg: cfg, logger: logger, dryRun: opts.DryRun, customJournal: opts.ReplicationJournal}

	// Before buildKB: a config-shaped error should not tear down a live KB, and
	// the Go limit should be in force for the first allocations, not after.
	memoryLimit, err := resolveMemoryLimit(cfg.Format.DuckDB.MemoryLimit, logger, opts.DryRun)
	if err != nil {
		return nil, err
	}

	k, err := rt.buildKB(ctx, cfg)
	if err != nil {
		rt.cleanupBuildFailure(ctx)
		return nil, err
	}
	rt.kb = k

	af, err := kbduckdb.NewArtifactFormat(kbduckdb.NewDepsFromKB(k,
		kbduckdb.WithMemoryLimit(memoryLimit),
		kbduckdb.WithTempDir(cfg.Format.DuckDB.TempDir),
		kbduckdb.WithBuildThreads(cfg.Format.DuckDB.BuildThreads),
		kbduckdb.WithEmbedParallelism(cfg.Format.DuckDB.EmbedParallelism),
		kbduckdb.WithExtensionDir(cfg.Format.DuckDB.ExtensionDir),
		kbduckdb.WithOfflineExt(cfg.Format.DuckDB.Offline),
	))
	if err != nil {
		rt.cleanupBuildFailure(ctx)
		return nil, fmt.Errorf("build duckdb artifact format: %w", err)
	}
	if err := k.RegisterFormat(af); err != nil {
		_ = af.Close()
		rt.cleanupBuildFailure(ctx)
		return nil, fmt.Errorf("register artifact format: %w", err)
	}
	rt.format = af

	rt.app = appcmd.NewApp(k, appConfigFromConfig(cfg, logger))
	if err := rt.wireSchedulerAndWorkers(cfg); err != nil {
		rt.cleanupBuildFailure(ctx)
		return nil, err
	}
	return rt, nil
}

// logMemoryGovernor says which mechanism is holding the line, or that none is.
// A deployment whose cgroup is read-only otherwise gets no back-pressure and no
// hint that it does not.
func (r *Runtime) logMemoryGovernor() {
	source, enforced, err := r.budget.Armed()
	if err != nil {
		r.logger.Warn("no memory back-pressure", "reason", err.Error())
		return
	}
	r.logger.Info("memory back-pressure armed", "source", source, "kernel_enforced", enforced)
}

// stopGovernor releases the governor and puts memory.high back.
func (r *Runtime) stopGovernor() {
	if r.budget != nil {
		r.budget.StopGovernor()
		r.budget = nil
	}
}

// logMemoryPressure records every transition. A process that silently drops to
// one build thread and 64MiB databases is the hardest kind of slow to diagnose.
func (r *Runtime) logMemoryPressure(from, to budget.Pressure, usage memlimit.Usage) {
	level := slog.LevelInfo
	if to > from {
		level = slog.LevelWarn
	}
	r.logger.Log(context.Background(), level, "memory pressure changed",
		"from", from.String(),
		"to", to.String(),
		"usage_mb", usage.Bytes>>20,
		"source", usage.Source,
		"kernel_enforced", r.budget != nil && r.budget.MemoryEnforced(),
	)
}

func (r *Runtime) cleanupBuildFailure(ctx context.Context) {
	if r.kb != nil {
		r.kb.Close()
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		if err := r.cleanups[i](cleanupCtx); err != nil {
			r.logger.Warn("runtime build rollback", logKeyError, err)
		}
	}
	r.cleanups = nil
}

func (r *Runtime) buildKB(ctx context.Context, cfg *config.Config) (*kb.KB, error) {
	blobStore, s3Store, err := r.buildBlobStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	kbOpts, err := r.buildKBOptions(ctx, cfg, s3Store)
	if err != nil {
		return nil, err
	}
	k := kb.NewKB(blobStore, cfg.Storage.Cache.Dir, kbOpts...)
	if cfg.Storage.Cache.MaxBytes > 0 {
		k.SetMaxCacheBytes(cfg.Storage.Cache.MaxBytes)
	}
	if d := cfg.Storage.Cache.EntryTTL.AsDuration(); d > 0 {
		k.SetCacheEntryTTL(d)
	}
	return k, nil
}

func (r *Runtime) buildKBOptions(ctx context.Context, cfg *config.Config, s3Store *blobstore.S3BlobStore) ([]kb.KBOption, error) {
	embedder, err := buildEmbedder(cfg, r.logger)
	if err != nil {
		return nil, err
	}
	kbOpts := append(baseKBOptions(cfg, embedder), kb.WithDeferredPublish(cfg.DeferredPublishEnabled()))
	kbOpts = append(kbOpts, graphKBOptions(cfg, r.logger)...)
	leasePrefix := ""
	if cfg.Storage.Blob.S3 != nil {
		leasePrefix = cfg.Storage.Blob.S3.LeasePrefix
		if cfg.Storage.Blob.Kind == "tiered" {
			leasePrefix = normalizeLeasePrefix(leasePrefix) + "kb/"
		}
	}
	if leaseOpt := buildLeaseOption(s3Store, leasePrefix, cfg.Storage.Cache.Dir, r.logger); leaseOpt != nil {
		kbOpts = append(kbOpts, leaseOpt)
	}
	mongoOpts, err := r.wireMongo(ctx, cfg)
	if err != nil {
		return nil, err
	}
	kbOpts = append(kbOpts, mongoOpts...)
	kbOpts = append(kbOpts, fallbackKBOptions(cfg, mongoOpts)...)
	return kbOpts, nil
}

func baseKBOptions(cfg *config.Config, embedder kb.Embedder) []kb.KBOption {
	return []kb.KBOption{
		kb.WithEmbedder(embedder),
		kb.WithShardingPolicy(cfg.ShardingPolicy()),
		kb.WithMediaGCConfig(cfg.MediaGCConfig()),
		kb.WithMediaContentTypeAllowlist(cfg.Media.ContentTypeAllowlist),
		kb.WithCacheWatermarks(
			cfg.Storage.Cache.HighWatermarkPercent,
			cfg.Storage.Cache.LowWatermarkPercent,
			cfg.Storage.Cache.MinFreeBytes,
		),
	}
}

func graphKBOptions(cfg *config.Config, logger *slog.Logger) []kb.KBOption {
	if !cfg.Graph.Enabled {
		return nil
	}
	grapher := kb.NewOllamaGrapher(cfg.Graph.URL, cfg.Graph.Model)
	grapher.MaxParallel = cfg.GraphParallelism()
	logger.Info(
		"configured ollama grapher",
		"url",
		cfg.Graph.URL,
		"model",
		cfg.Graph.Model,
		"parallelism",
		cfg.GraphParallelism(),
	)
	return []kb.KBOption{
		kb.WithGraphBuilder(
			&kb.GraphBuilder{Chunker: &kb.TextChunker{ChunkSize: kb.DefaultTextChunkSize}, Grapher: grapher},
		),
	}
}

func fallbackKBOptions(cfg *config.Config, mongoOpts []kb.KBOption) []kb.KBOption {
	var opts []kb.KBOption
	if !hasEventStore(mongoOpts) {
		opts = append(opts, kb.WithEventStore(kb.NewInMemoryEventStore()))
	}
	if !hasEventInbox(mongoOpts) {
		opts = append(opts, kb.WithEventInbox(kb.NewInMemoryEventInbox()))
	}
	if cfg.Media.Enabled && !hasMediaStore(mongoOpts) {
		opts = append(opts, kb.WithMediaStore(kb.NewInMemoryMediaStore()))
	}
	return opts
}

func appConfigFromConfig(cfg *config.Config, logger *slog.Logger) appcmd.AppConfig {
	return appcmd.AppConfig{
		Address:               cfg.HTTP.Address,
		ReadHeaderTimeout:     cfg.HTTPReadHeaderTimeout(),
		ShutdownTimeout:       cfg.HTTPShutdownTimeout(),
		CacheEvictionInterval: cfg.CacheEvictInterval(),
		MaxMediaBytes:         cfg.Media.MaxBytes,
		MCP:                   mcpConfigFromConfig(cfg),
		DeferredPublish:       cfg.DeferredPublishEnabled(),
		Logger:                logger,
	}
}

func (r *Runtime) wireSchedulerAndWorkers(cfg *config.Config) error {
	if cfg.SchedulerEnabled() {
		r.scheduler = kb.NewScheduler(r.kb.WriteLeaseManager, cfg.SchedulerTick(), cfg.Scheduler.DisabledJobs, nil)
		if err := r.kb.RegisterDefaultJobs(r.scheduler); err != nil {
			return fmt.Errorf("register scheduler jobs: %w", err)
		}
	}
	if r.kb.EventStore == nil {
		return nil
	}
	pools, err := buildWorkerPools(r.kb, cfg, r.app)
	if err != nil {
		return err
	}
	r.workerPools = pools
	return nil
}

// Start takes the runtime live: ensures filesystem state, starts the
// scheduler, starts worker pools, and binds the HTTP listener. Safe to call
// only once. In DryRun mode, Start returns nil without side effects (no
// mkdir, no port bind, no goroutines).
func (r *Runtime) Start(ctx context.Context) error {
	return r.start(ctx, true)
}

// StartBackground starts filesystem state, scheduler, and worker pools without
// binding the HTTP app. It is used by stdio MCP mode.
func (r *Runtime) StartBackground(ctx context.Context) error {
	return r.start(ctx, false)
}

func (r *Runtime) start(ctx context.Context, withHTTP bool) (err error) {
	if r.dryRun {
		return nil
	}
	r.lifecycleMu.Lock()
	if r.started || r.stopping || r.stopped {
		r.lifecycleMu.Unlock()
		return kb.ErrAlreadyStarted
	}
	r.started = true
	// Held on the runtime, not looked up again at stop: a second Build swaps
	// the shared manager, and stopping whichever one is current then leaves the
	// first governor running with nothing able to reach it.
	r.budget = budget.Process()
	r.budget.StartGovernor(context.WithoutCancel(ctx), r.logMemoryPressure)
	r.logMemoryGovernor()
	tieredStarted := false
	if r.cfg.Storage.Blob.Kind == "local" {
		if err = os.MkdirAll(r.cfg.Storage.Blob.Root, 0o755); err != nil {
			err = fmt.Errorf("create blob root %q: %w", r.cfg.Storage.Blob.Root, err)
		}
	}
	if err == nil {
		err = os.MkdirAll(r.cfg.Storage.Cache.Dir, 0o755)
		if err != nil {
			err = fmt.Errorf("create cache dir %q: %w", r.cfg.Storage.Cache.Dir, err)
		}
	}
	if err == nil && r.tieredStore != nil {
		err = r.tieredStore.Start(ctx)
		if err == nil {
			tieredStarted = true
			// Revalidate after both directories exist so symlink aliases cannot
			// place the durable journal under the evictable cache root.
			err = r.cfg.Validate()
		}
		if err != nil {
			err = fmt.Errorf("start tiered blob store: %w", err)
		}
	}
	if err == nil && r.scheduler != nil {
		r.scheduler.Start()
		r.logger.Info("scheduler started", "tick_interval", r.cfg.SchedulerTick(), "jobs", r.scheduler.JobIDs())
	}
	if err == nil {
		r.backgroundStarted = true
		for _, pool := range r.workerPools {
			if startErr := pool.Start(ctx); startErr != nil {
				err = fmt.Errorf("start worker pool: %w", startErr)
				break
			}
		}
	}
	if err == nil {
		if n := r.cfg.Storage.Cache.WarmShards; n > 0 && r.format != nil {
			warmCtx, cancel := context.WithCancel(ctx)
			r.warmCancel = cancel
			r.warmDone = make(chan struct{})
			go func() {
				defer close(r.warmDone)
				r.format.WarmCache(warmCtx, n, r.logger)
			}()
		}
	}
	if err == nil && withHTTP {
		if startErr := r.app.Start(); startErr != nil {
			err = fmt.Errorf("start app: %w", startErr)
		} else {
			r.logger.Info("minnow listening", "address", r.app.Address())
		}
	}
	retryableEarlyFailure := err != nil && !tieredStarted && !r.backgroundStarted
	if retryableEarlyFailure {
		r.started = false
		// This path returns without Stop, so the governor has to be released
		// here or it stays parked with memory.high still written.
		r.stopGovernor()
	}
	r.lifecycleMu.Unlock()
	if err == nil {
		return nil
	}
	if retryableEarlyFailure {
		return err
	}
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return errors.Join(err, r.Stop(rollbackCtx))
}

func mcpConfigFromConfig(cfg *config.Config) mcpserver.Config {
	transports := map[string]bool{}
	for _, t := range cfg.MCP.Transports {
		transports[strings.ToLower(strings.TrimSpace(t))] = true
	}
	return mcpserver.Config{
		Enabled:            cfg.MCP.Enabled,
		HTTPEnabled:        transports["http"],
		StdioEnabled:       transports["stdio"],
		HTTPPath:           cfg.MCP.HTTPPath,
		ReadOnly:           cfg.MCP.ReadOnly,
		AllowIndexing:      cfg.MCP.AllowIndexing,
		AllowSyncIndexing:  cfg.MCP.AllowSyncIndexing,
		AllowDestructive:   cfg.MCP.AllowDestructive,
		AllowAdmin:         cfg.MCP.AllowAdmin,
		DefaultSyncTimeout: cfg.MCP.DefaultSyncTimeout.AsDuration(),
		MaxSyncTimeout:     cfg.MCP.MaxSyncTimeout.AsDuration(),
		HTTPJSONResponse:   cfg.MCP.HTTPJSONResponse,
		HTTPStateless:      cfg.MCPHTTPStateless(),
		HTTPStateful:       !cfg.MCPHTTPStateless(),
		HTTPSessionTimeout: cfg.MCP.HTTPSessionTimeout.AsDuration(),
		HTTPMaxSessions:    cfg.MCP.HTTPMaxSessions,
		CodeIndex: mcpserver.CodeIndexDefaults{
			Include:          append([]string(nil), cfg.CodeIndex.Include...),
			Exclude:          append([]string(nil), cfg.CodeIndex.Exclude...),
			MaxFileBytes:     cfg.CodeIndex.MaxFileBytes,
			ChunkSize:        cfg.CodeIndex.ChunkSize,
			ChunkOverlap:     cfg.CodeIndex.ChunkOverlap,
			IncludeUntracked: cfg.CodeIndex.IncludeUntracked,
			ResourcePolicy: kb.CodeIndexResourcePolicy{
				EmbedBatchSize: cfg.CodeIndex.EmbedBatchSize,
				MaxBatchBytes:  cfg.CodeIndex.MaxBatchBytes,
				Throttle:       cfg.CodeIndex.Throttle.AsDuration(),
				MaxHeapBytes:   cfg.CodeIndex.MaxHeapBytes,
				MaxRSSBytes:    cfg.CodeIndex.MaxRSSBytes,
				LargeRepoFiles: cfg.CodeIndex.LargeRepoFiles,
			},
			RequireConfirm: cfg.CodeIndex.RequireConfirm,
		},
	}
}

// Wait blocks until the HTTP app exits (on ctx cancellation or error).
// In DryRun mode, returns immediately.
func (r *Runtime) Wait() error {
	if r.dryRun || r.app == nil {
		return nil
	}
	return r.app.Wait()
}

// Stop first closes HTTP intake, then drains background work before closing
// database handles and external connections. Safe to call multiple times.
func (r *Runtime) Stop(ctx context.Context) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		return nil
	}
	if r.stopping {
		return errors.New("runtime stop already in progress")
	}
	r.stopping = true
	completed := false
	defer func() {
		r.stopping = false
		if completed {
			r.stopped = true
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}

	var stopErr error
	if !r.dryRun && r.app != nil {
		if err := r.app.Stop(ctx); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("stop HTTP app: %w", err))
		}
	}
	if r.warmCancel != nil {
		r.warmCancel()
	}
	if r.backgroundStarted {
		if r.scheduler != nil {
			r.scheduler.BeginStop()
		}
		for _, pool := range r.workerPools {
			pool.BeginStop()
		}
		if r.scheduler != nil {
			stopErr = errors.Join(stopErr, r.scheduler.StopContext(ctx))
		}
		for _, pool := range r.workerPools {
			stopErr = errors.Join(stopErr, pool.StopContext(ctx))
		}
	}
	// After the pools, not before: the drain is when in-flight seals finish
	// allocating, which is exactly when back-pressure still has a job to do.
	r.stopGovernor()
	if r.warmDone != nil {
		select {
		case <-r.warmDone:
		case <-ctx.Done():
			stopErr = errors.Join(stopErr, ctx.Err())
		}
	}
	if r.tieredStore != nil {
		r.tieredStore.BeginStop()
		if err := r.tieredStore.Stop(ctx); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("stop tiered blob store: %w", err))
		}
	}
	if stopErr == nil && r.kb != nil {
		r.kb.Close()
	}
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		if err := r.cleanups[i](ctx); err != nil {
			stopErr = errors.Join(stopErr, err)
		}
	}
	completed = stopErr == nil
	return stopErr
}

func (r *Runtime) buildBlobStore(ctx context.Context, cfg *config.Config) (kb.BlobStore, *blobstore.S3BlobStore, error) {
	switch cfg.Storage.Blob.Kind {
	case "local":
		return &kb.LocalBlobStore{Root: cfg.Storage.Blob.Root}, nil, nil
	case "s3":
		store, err := newS3BlobStore(ctx, cfg.Storage.Blob.S3)
		return store, store, err
	case "tiered":
		remote, err := newS3BlobStore(ctx, cfg.Storage.Blob.S3)
		if err != nil {
			return nil, nil, err
		}
		journalStore := r.customJournal
		if journalStore == nil {
			jcfg := cfg.Storage.Blob.Tiered.Journal
			journalStore = localjournal.New(jcfg.Dir, journal.Config{
				MaxPendingEntries: jcfg.MaxPendingEntries,
				MaxPendingBytes:   jcfg.MaxPendingBytes,
				MinFreeBytes:      jcfg.MinFreeBytes,
			})
		}
		tcfg := cfg.Storage.Blob.Tiered
		ownerPrefix := normalizeLeasePrefix(cfg.Storage.Blob.S3.LeasePrefix)
		store, err := tiered.New(remote, journalStore, tiered.Config{
			Durability:    tiered.Durability(tcfg.Durability),
			PollInterval:  tcfg.Replication.PollInterval.AsDuration(),
			RetryBase:     tcfg.Replication.RetryBase.AsDuration(),
			RetryMax:      tcfg.Replication.RetryMax.AsDuration(),
			MaxAttempts:   tcfg.Replication.MaxAttempts,
			ControlPrefix: ownerPrefix,
			OwnerKey:      ownerPrefix + "journal/owner.lock",
		})
		if err != nil {
			return nil, nil, err
		}
		r.tieredStore = store
		return store, remote, nil
	default:
		return nil, nil, fmt.Errorf("configruntime: blob kind %q not supported", cfg.Storage.Blob.Kind)
	}
}

func newS3BlobStore(ctx context.Context, s3cfg *config.S3BlobConfig) (*blobstore.S3BlobStore, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(s3cfg.Region),
	}
	if s3cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3cfg.AccessKeyID, s3cfg.SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if s3cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(s3cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	return blobstore.NewS3BlobStore(client, s3cfg.Bucket, s3cfg.Prefix), nil
}

// leaseDirName comes from the package that decides what a cache sweep may
// delete, so the exclusion and the location cannot drift apart.
const leaseDirName = cacheevict.LeaseDirName

func normalizeLeasePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "leases/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

// buildLeaseOption prefers S3 so several instances coordinate on one store,
// and otherwise disk, which keeps a lease across a restart.
func buildLeaseOption(
	s3Store *blobstore.S3BlobStore,
	prefix, cacheDir string,
	logger *slog.Logger,
) kb.KBOption {
	if s3Store != nil {
		mgr, err := lease.NewS3Manager(s3Store, prefix)
		if err == nil {
			logger.Info("using S3-native distributed write lease")
			return kb.WithWriteLeaseManager(mgr)
		}
		logger.Warn("failed to build s3 lease manager", logKeyError, err)
	}
	if strings.TrimSpace(cacheDir) == "" {
		return nil
	}
	dir := filepath.Join(cacheDir, leaseDirName)
	mgr, err := lease.NewFileManager(dir)
	if err != nil {
		logger.Warn("failed to build file lease manager, falling back to in-memory", logKeyError, err)
		return nil
	}
	logger.Info("using file-backed write lease", "dir", dir)
	return kb.WithWriteLeaseManager(mgr)
}

func buildEmbedder(cfg *config.Config, logger *slog.Logger) (kb.Embedder, error) {
	switch cfg.Embedder.Provider {
	case "ollama":
		logger.Info("configured ollama embedder", "url", cfg.Embedder.Ollama.URL, "model", cfg.Embedder.Ollama.Model)
		return kb.NewOllamaEmbedder(cfg.Embedder.Ollama.URL, cfg.Embedder.Ollama.Model), nil
	case "local":
		e, err := kb.NewLocalEmbedder(cfg.Embedder.Local.Dim)
		if err != nil {
			return nil, fmt.Errorf("build local embedder: %w", err)
		}
		logger.Info("configured local embedder", "dim", cfg.Embedder.Local.Dim)
		return e, nil
	case "openai_compatible":
		oc := cfg.Embedder.OpenAICompatible
		e, err := kb.NewOpenAICompatibleEmbedder(kb.OpenAICompatibleEmbedderConfig{
			BaseURL:    oc.BaseURL,
			Model:      oc.Model,
			Token:      oc.Token,
			Dimensions: oc.Dimensions,
		})
		if err != nil {
			return nil, fmt.Errorf("build openai compatible embedder: %w", err)
		}
		logger.Info(
			"configured openai compatible embedder",
			"base_url",
			oc.BaseURL,
			"model",
			oc.Model,
			"dimensions",
			oc.Dimensions,
		)
		return e, nil
	default:
		return nil, fmt.Errorf("configruntime: embedder provider %q not supported", cfg.Embedder.Provider)
	}
}

func CodeIndexOptionsFromConfig(cfg *config.Config, kbID, root string) kb.CodeIndexOptions {
	return kb.CodeIndexOptions{
		KBID:             kbID,
		Root:             root,
		Include:          append([]string(nil), cfg.CodeIndex.Include...),
		Exclude:          append([]string(nil), cfg.CodeIndex.Exclude...),
		MaxFileBytes:     cfg.CodeIndex.MaxFileBytes,
		ChunkSize:        cfg.CodeIndex.ChunkSize,
		ChunkOverlap:     cfg.CodeIndex.ChunkOverlap,
		IncludeUntracked: cfg.CodeIndex.IncludeUntracked,
		EmbedBatchSize:   cfg.CodeIndex.EmbedBatchSize,
		MaxBatchBytes:    cfg.CodeIndex.MaxBatchBytes,
		Throttle:         cfg.CodeIndex.Throttle.AsDuration(),
		MaxHeapBytes:     cfg.CodeIndex.MaxHeapBytes,
		MaxRSSBytes:      cfg.CodeIndex.MaxRSSBytes,
		LargeRepoFiles:   cfg.CodeIndex.LargeRepoFiles,
		RequireConfirm:   cfg.CodeIndex.RequireConfirm,
	}
}

// wireMongo connects to Mongo and returns the KBOption slice that wires
// the manifest store, event store, inbox, and (when media is enabled) the
// media store. In DryRun mode or when cfg.Mongo is nil, returns no options.
func (r *Runtime) wireMongo(ctx context.Context, cfg *config.Config) ([]kb.KBOption, error) {
	if cfg.Mongo == nil || r.dryRun {
		return nil, nil
	}

	// The mongo driver may echo the connection URI (and therefore any
	// embedded credentials) in its error text. Log the underlying error
	// against a redacted URI internally, and return a fixed message to the
	// caller: never let the raw driver error escape to logs or API responses
	// that might include the secret.
	redactedURI := redactMongoURI(cfg.Mongo.URI)
	client, err := mongo.Connect(mongoopts.Client().ApplyURI(cfg.Mongo.URI))
	if err != nil {
		r.logger.Error("mongo connect failed", "uri", redactedURI, logKeyError, err.Error())
		return nil, errors.New("mongo connect failed: check MINNOW_MONGO_URI")
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer disconnectCancel()
		if disconnectErr := client.Disconnect(disconnectCtx); disconnectErr != nil {
			r.logger.Warn(
				"mongo disconnect after ping failure failed",
				"uri",
				redactedURI,
				logKeyError,
				disconnectErr.Error(),
			)
		}
		r.logger.Error("mongo ping failed", "uri", redactedURI, logKeyError, err.Error())
		return nil, errors.New("mongo ping failed: check MINNOW_MONGO_URI and network reachability")
	}

	r.cleanups = append(r.cleanups, func(ctx context.Context) error {
		return client.Disconnect(ctx)
	})

	db := client.Database(cfg.Mongo.Database)
	initCtx, cancelInit := context.WithTimeout(ctx, 10*time.Second)
	defer cancelInit()

	eventStore, err := kb.NewMongoEventStore(initCtx, db.Collection(cfg.Mongo.Collections.Events), client)
	if err != nil {
		return nil, fmt.Errorf("mongo event store: %w", err)
	}
	inbox, err := kb.NewMongoEventInbox(initCtx, db.Collection(cfg.Mongo.Collections.Inbox))
	if err != nil {
		return nil, fmt.Errorf("mongo event inbox: %w", err)
	}

	out := []kb.KBOption{
		kb.WithManifestStore(kb.NewMongoManifestStore(db.Collection(cfg.Mongo.Collections.Manifests))),
		kb.WithEventStore(eventStore),
		kb.WithEventInbox(inbox),
	}

	if cfg.Media.Enabled {
		mediaStore, err := kb.NewMongoMediaStore(initCtx, db.Collection(cfg.Mongo.Collections.Media))
		if err != nil {
			return nil, fmt.Errorf("mongo media store: %w", err)
		}
		out = append(out, kb.WithMediaStore(mediaStore))
	}

	r.logger.Info("configured mongo stores",
		"db", cfg.Mongo.Database,
		"manifests", cfg.Mongo.Collections.Manifests,
		"events", cfg.Mongo.Collections.Events,
		"inbox", cfg.Mongo.Collections.Inbox,
		"media", cfg.Mongo.Collections.Media,
		"media_enabled", cfg.Media.Enabled,
	)

	return out, nil
}

// optionsApply runs every KBOption against a scratch KB so we can introspect
// which fields the slice would set, so the local fallback doesn't clobber a
// store that Mongo wiring already provided.
func optionsApply(opts []kb.KBOption) *kb.KB {
	scratch := &kb.KB{}
	for _, opt := range opts {
		if opt != nil {
			opt(scratch)
		}
	}
	return scratch
}

func hasMediaStore(opts []kb.KBOption) bool {
	if len(opts) == 0 {
		return false
	}
	return optionsApply(opts).MediaStore != nil
}

func hasEventStore(opts []kb.KBOption) bool {
	if len(opts) == 0 {
		return false
	}
	return optionsApply(opts).EventStore != nil
}

func hasEventInbox(opts []kb.KBOption) bool {
	if len(opts) == 0 {
		return false
	}
	return optionsApply(opts).EventInbox != nil
}

func buildWorkerPools(k *kb.KB, cfg *config.Config, app *appcmd.App) ([]*kb.WorkerPool, error) {
	type entry struct {
		worker kb.Worker
		pool   config.WorkerPool
	}
	// Document workers are unconditional. media-upload is only constructed
	// when media is enabled. Constructing the worker without a wired
	// MediaStore would let it claim media.upload events from the queue and
	// fail every Handle call with "media subsystem not configured", driving
	// retries and dead-letters for events the operator explicitly disabled.
	entries := []entry{
		{&kb.StagingCleanupWorker{KB: k, ID: "staging-cleanup-worker"}, cfg.Workers.DocumentUpsert},
		{&kb.DocumentUpsertWorker{KB: k, ID: "document-upsert-worker"}, cfg.Workers.DocumentUpsert},
		{&kb.DocumentChunkedWorker{KB: k, ID: "document-chunked-worker"}, cfg.Workers.DocumentChunked},
		{
			&kb.DocumentPublishWorker{
				KB:        k,
				ID:        "document-publish-embedded-worker",
				KindValue: kb.EventDocumentEmbedded,
			},
			cfg.Workers.DocumentPublish,
		},
		{
			&kb.DocumentPublishWorker{
				KB:        k,
				ID:        "document-publish-graph-worker",
				KindValue: kb.EventDocumentGraphExtracted,
			},
			cfg.Workers.DocumentPublish,
		},
		{
			&kb.SessionCommitWorker{
				KB:             k,
				ID:             "session-commit-worker",
				ReleaseSession: k.IngestSessionsFor().Release,
				FinalizeScope:  k.FinalizeSessionScope,
			},
			cfg.Workers.DocumentPublish,
		},
	}
	if cfg.Media.Enabled {
		entries = append(
			entries,
			entry{&kb.MediaUploadWorker{KB: k, ID: "media-upload-worker"}, cfg.Workers.MediaUpload},
		)
	}

	pools := make([]*kb.WorkerPool, 0, len(entries))
	for _, e := range entries {
		poolCfg := cfg.PoolConfigFor(e.pool)
		poolCfg.Clock = k.Clock
		pool, err := kb.NewWorkerPool(e.worker, k.EventStore, k.EventInbox, poolCfg)
		if err != nil {
			return nil, fmt.Errorf("build worker pool for %T: %w", e.worker, err)
		}
		if m := app.Metrics(); m != nil {
			pool.SetMetrics(workerMetricsAdapter{m})
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

// redactMongoURI strips credentials (user:password@) from a mongo URI so it
// can be safely included in logs. Non-URI input is returned unchanged except
// for a fixed sentinel so operators can tell redaction ran.
func redactMongoURI(uri string) string {
	if uri == "" {
		return ""
	}
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" {
		return "<unparseable-uri>"
	}
	u.User = nil
	return u.String()
}

// workerMetricsAdapter bridges kb.WorkerMetrics to appcmd.AppMetrics.
type workerMetricsAdapter struct{ m kb.AppMetrics }

func (a workerMetricsAdapter) OnWorkerTick(
	kind kb.EventKind,
	workerID, outcome string,
	duration time.Duration,
	_ error,
) {
	a.m.RecordWorkerTick(string(kind), workerID, outcome, duration.Milliseconds())
}
