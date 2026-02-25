package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	appcmd "github.com/mikills/kbcore/cmd"
	kb "github.com/mikills/kbcore/kb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	logFormat := getenvDefault("KBCORE_LOG_FORMAT", "text")
	logger := newLogger(logFormat)

	blobRoot := getenvDefault("KBCORE_BLOB_ROOT", "./.temp/fixtures")
	cacheDir := getenvDefault("KBCORE_CACHE_DIR", "./.temp/cache")
	memLimit := getenvDefault("KBCORE_MEMORY_LIMIT", "128MB")
	ollamaURL := getenvDefault("KBCORE_OLLAMA_URL", "http://localhost:11434")
	ollamaModel := getenvDefault("KBCORE_OLLAMA_MODEL", "all-minilm")
	graphEnabled := getenvBoolDefault(logger, "KBCORE_GRAPH_ENABLED", false)
	graphURL := getenvDefault("KBCORE_GRAPH_URL", "http://localhost:11434")
	graphModel := getenvDefault("KBCORE_GRAPH_MODEL", "gemma3:4b")
	graphParallelism := getenvIntDefault(logger, "KBCORE_GRAPH_PARALLELISM", 4)
	extDir := getenvDefault("KBCORE_DUCKDB_EXTENSION_DIR", kb.DefaultExtensionDir)
	extOffline := getenvBoolDefault(logger, "KBCORE_DUCKDB_EXTENSION_OFFLINE", true)
	shardingPolicy := shardingPolicyFromEnv(logger)
	addr := getenvDefault("KBCORE_HTTP_ADDR", "127.0.0.1:8080")
	cacheEvictInterval := getenvDurationDefault(logger, "KBCORE_CACHE_EVICT_INTERVAL", 30*time.Second)

	store := &kb.LocalBlobStore{Root: blobRoot}

	embedProvider := getenvDefault("KBCORE_EMBEDDER_PROVIDER", "local")
	embedder := getEmbedder(logger, embedProvider, ollamaURL, ollamaModel)

	// Manifest store: MongoDB or blob-backed (default).
	mongoManifestURI := os.Getenv("KBCORE_MANIFEST_MONGO_URI")
	var manifestStoreOpt kb.KBOption
	if mongoManifestURI != "" {
		mongoManifestDB := getenvDefault("KBCORE_MANIFEST_MONGO_DB", "kbcore")
		mongoManifestColl := getenvDefault("KBCORE_MANIFEST_MONGO_COLLECTION", "manifests")

		mongoClient, err := mongo.Connect(mongooptions.Client().ApplyURI(mongoManifestURI))
		if err != nil {
			logger.Error("mongo connect", "error", err)
			os.Exit(1)
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pingCancel()
		if err := mongoClient.Ping(pingCtx, nil); err != nil {
			logger.Error("mongo ping", "error", err)
			os.Exit(1)
		}
		defer func() {
			disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mongoClient.Disconnect(disconnectCtx)
		}()
		coll := mongoClient.Database(mongoManifestDB).Collection(mongoManifestColl)
		manifestStoreOpt = kb.WithManifestStore(kb.NewMongoManifestStore(coll))
		logger.Info("configured mongo manifest store",
			"db", mongoManifestDB,
			"collection", mongoManifestColl,
		)
	}

	kbOpts := []kb.KBOption{
		kb.WithMemoryLimit(memLimit),
		kb.WithEmbedder(embedder),
		kb.WithShardingPolicy(shardingPolicy),
		kb.WithDuckDBExtensionDir(extDir),
		kb.WithDuckDBOfflineExtensions(extOffline),
	}
	if manifestStoreOpt != nil {
		kbOpts = append(kbOpts, manifestStoreOpt)
	}
	if graphEnabled {
		grapher := kb.NewOllamaGrapher(graphURL, graphModel)
		grapher.MaxParallel = graphParallelism
		gb := &kb.GraphBuilder{Chunker: &kb.TextChunker{}, Grapher: grapher}
		kbOpts = append(kbOpts, kb.WithGraphBuilder(gb))
		logger.Info("configured ollama grapher", "url", graphURL, "model", graphModel, "parallelism", graphParallelism)
	} else {
		logger.Info("graph extraction disabled", "hint", "set KBCORE_GRAPH_ENABLED=true to enable")
	}
	loader := kb.NewKB(store, cacheDir, kbOpts...)

	logger.Info(
		"configured sharding policy",
		"shard_trigger_bytes", shardingPolicy.ShardTriggerBytes,
		"shard_trigger_vector_rows", shardingPolicy.ShardTriggerVectorRows,
		"target_shard_bytes", shardingPolicy.TargetShardBytes,
		"max_vector_rows_per_shard", shardingPolicy.MaxVectorRowsPerShard,
		"query_shard_fanout", shardingPolicy.QueryShardFanout,
		"query_shard_fanout_adaptive_max", shardingPolicy.QueryShardFanoutAdaptiveMax,
		"query_shard_parallelism", shardingPolicy.QueryShardParallelism,
		"small_kb_max_shards", shardingPolicy.SmallKBMaxShards,
		"compaction_enabled", shardingPolicy.CompactionEnabled,
		"compaction_min_shard_count", shardingPolicy.CompactionMinShardCount,
		"compaction_tombstone_ratio", shardingPolicy.CompactionTombstoneRatio,
	)

	appCfg := appcmd.AppConfig{
		Address:               addr,
		ReadHeaderTimeout:     5 * time.Second,
		ShutdownTimeout:       10 * time.Second,
		CacheEvictionInterval: cacheEvictInterval,
		Logger:                logger,
	}
	app := appcmd.NewApp(loader, appCfg)

	if err := app.Start(); err != nil {
		logger.Error("start app", "error", err)
		os.Exit(1)
	}
	logger.Info("kbcore listening", "address", app.Address())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), appCfg.ShutdownTimeout)
		defer cancel()
		if err := app.Stop(shutdownCtx); err != nil {
			logger.Error("shutdown error", "error", err)
		}
	}()

	if err := app.Wait(); err != nil {
		logger.Error("app exited with error", "error", err)
		os.Exit(1)
	}
}

func newLogger(format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

func getenvDefault(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getenvDurationDefault(logger *slog.Logger, key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("invalid duration env var", "key", key, "value", v, "error", err)
		os.Exit(1)
	}
	return d
}

func getenvIntDefault(logger *slog.Logger, key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("invalid integer env var", "key", key, "value", v, "error", err)
		os.Exit(1)
	}
	return n
}

func getenvBoolDefault(logger *slog.Logger, key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("invalid boolean env var", "key", key, "value", v, "error", err)
		os.Exit(1)
	}
	return b
}

func shardingPolicyFromEnv(logger *slog.Logger) kb.ShardingPolicy {
	policy, err := parseShardingPolicyFromEnv(os.Getenv)
	if err != nil {
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("invalid sharding policy env", "error", err)
		os.Exit(1)
	}
	return policy
}

func parseShardingPolicyFromEnv(getenv func(string) string) (kb.ShardingPolicy, error) {
	policy := kb.DefaultShardingPolicy()

	// setInt parses a required-positive int env var into dest.
	setInt := func(key string, dest *int) error {
		v := strings.TrimSpace(getenv(key))
		if v == "" {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s must be a positive integer", key)
		}
		*dest = n
		return nil
	}

	// setInt64 parses a required-positive int64 env var into dest.
	setInt64 := func(key string, dest *int64) error {
		v := strings.TrimSpace(getenv(key))
		if v == "" {
			return nil
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s must be a positive integer", key)
		}
		*dest = n
		return nil
	}

	// setBool parses a boolean env var into dest.
	setBool := func(key string, dest *bool) error {
		v := strings.TrimSpace(getenv(key))
		if v == "" {
			return nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s must be a boolean", key)
		}
		*dest = b
		return nil
	}

	// setFloat64 parses a float64 env var into dest, validating it is within (lo, hi].
	setFloat64 := func(key string, dest *float64, lo, hi float64) error {
		v := strings.TrimSpace(getenv(key))
		if v == "" {
			return nil
		}
		n, err := strconv.ParseFloat(v, 64)
		if err != nil || n <= lo || n > hi {
			return fmt.Errorf("%s must be within (%g,%g]", key, lo, hi)
		}
		*dest = n
		return nil
	}

	for _, call := range []error{
		setInt64("KBCORE_SHARD_TRIGGER_BYTES", &policy.ShardTriggerBytes),
		setInt("KBCORE_SHARD_TRIGGER_VECTOR_ROWS", &policy.ShardTriggerVectorRows),
		setInt64("KBCORE_TARGET_SHARD_BYTES", &policy.TargetShardBytes),
		setInt("KBCORE_MAX_VECTOR_ROWS_PER_SHARD", &policy.MaxVectorRowsPerShard),
		setInt("KBCORE_QUERY_SHARD_FANOUT", &policy.QueryShardFanout),
		setInt("KBCORE_QUERY_SHARD_FANOUT_ADAPTIVE_MAX", &policy.QueryShardFanoutAdaptiveMax),
		setInt("KBCORE_QUERY_SHARD_PARALLELISM", &policy.QueryShardParallelism),
		setInt("KBCORE_SMALL_KB_MAX_SHARDS", &policy.SmallKBMaxShards),
		setInt("KBCORE_COMPACTION_MIN_SHARD_COUNT", &policy.CompactionMinShardCount),
		setBool("KBCORE_COMPACTION_ENABLED", &policy.CompactionEnabled),
		setFloat64("KBCORE_COMPACTION_TOMBSTONE_RATIO", &policy.CompactionTombstoneRatio, 0, 1),
	} {
		if call != nil {
			return kb.ShardingPolicy{}, call
		}
	}

	const maxSafeFanout = 64
	if policy.QueryShardFanout > maxSafeFanout {
		return kb.ShardingPolicy{}, fmt.Errorf("KBCORE_QUERY_SHARD_FANOUT must be <= %d", maxSafeFanout)
	}
	if policy.QueryShardFanoutAdaptiveMax > maxSafeFanout {
		return kb.ShardingPolicy{}, fmt.Errorf("KBCORE_QUERY_SHARD_FANOUT_ADAPTIVE_MAX must be <= %d", maxSafeFanout)
	}
	if policy.QueryShardFanoutAdaptiveMax < policy.QueryShardFanout {
		return kb.ShardingPolicy{}, fmt.Errorf("KBCORE_QUERY_SHARD_FANOUT_ADAPTIVE_MAX must be >= KBCORE_QUERY_SHARD_FANOUT")
	}

	return policy, nil
}

func getEmbedder(logger *slog.Logger, embedProvider, ollamaURL, ollamaModel string) kb.Embedder {
	var embedder kb.Embedder

	switch embedProvider {
	case "ollama":
		embedder = kb.NewOllamaEmbedder(ollamaURL, ollamaModel)
		logger.Info("configured ollama embedder", "url", ollamaURL, "model", ollamaModel)
	case "local":
		localDim := getenvIntDefault(logger, "KBCORE_LOCAL_EMBED_DIM", 384)
		le, err := kb.NewLocalEmbedder(localDim)
		if err != nil {
			logger.Error("failed to create local embedder", "error", err)
			os.Exit(1)
		}
		embedder = le
		logger.Info("configured local embedder", "dim", localDim)
	default:
		logger.Error("unknown embedder provider", "provider", embedProvider, "valid", "ollama, local")
		os.Exit(1)
	}

	return embedder
}
