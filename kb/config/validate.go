package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mikills/minnow/kb"
)

// Validate checks cross-field and per-field invariants that strict YAML
// decoding cannot express. It runs after applyDefaults and path resolution,
// so every field visible here has either an explicit user value or a schema
// default filled in.
//
// Errors are accumulated so operators see the full list of problems in one
// pass rather than fixing them one at a time.
func (c *Config) Validate() error {
	var errs []string

	errs = appendIf(errs, c.validateHTTP())
	errs = appendIf(errs, c.validateStorage())
	errs = appendIf(errs, c.validateFormat())
	errs = appendIf(errs, c.validateEmbedder())
	errs = appendIf(errs, c.validateGraph())
	errs = appendIf(errs, c.validateMongo())
	errs = appendIf(errs, c.validateScheduler())
	errs = appendIf(errs, c.validateWorkers())
	errs = appendIf(errs, c.validateMedia())
	errs = appendIf(errs, c.validateSharding())
	errs = appendIf(errs, c.validateCodeIndex())
	errs = appendIf(errs, c.validateMCP())

	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}

func (c *Config) validateMCP() error {
	if !c.MCP.Enabled {
		return nil
	}
	seen, err := validateMCPTransports(c.MCP.Transports)
	if err != nil {
		return err
	}
	return firstErr(
		validateMCPHTTPPath(seen["http"], c.MCP.HTTPPath),
		validateMCPToolGates(c.MCP),
		validateMCPSyncTimeouts(c.MCP),
		validateMCPSessionTimeout(c.MCP.HTTPSessionTimeout),
		requirePositiveInt("mcp.http_max_sessions", c.MCP.HTTPMaxSessions),
	)
}

func validateMCPSessionTimeout(value Duration) error {
	if value.AsDuration() < time.Second {
		return errors.New("mcp.http_session_timeout must be >= 1s")
	}
	return nil
}

func validateMCPTransports(transports []string) (map[string]bool, error) {
	if len(transports) == 0 {
		return nil, errors.New("mcp.transports must include at least one transport when mcp.enabled is true")
	}
	seen := map[string]bool{}
	for _, transport := range transports {
		name := strings.ToLower(strings.TrimSpace(transport))
		if name != "http" && name != "stdio" {
			return nil, fmt.Errorf("mcp.transports contains unsupported transport %q (allowed: http, stdio)", transport)
		}
		seen[name] = true
	}
	return seen, nil
}

func validateMCPHTTPPath(enabled bool, path string) error {
	if !enabled {
		return nil
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("mcp.http_path must start with / when http transport is enabled")
	}
	if strings.ContainsAny(path, " \t\n\r") {
		return fmt.Errorf("mcp.http_path must not contain whitespace")
	}
	return nil
}

func validateMCPToolGates(cfg MCPConfig) error {
	if cfg.ReadOnly && (cfg.AllowIndexing || cfg.AllowSyncIndexing || cfg.AllowDestructive || cfg.AllowAdmin) {
		return errors.New("mcp.read_only cannot be combined with indexing, destructive, or admin tools")
	}
	if cfg.AllowSyncIndexing && !cfg.AllowIndexing {
		return errors.New("mcp.allow_sync_indexing requires mcp.allow_indexing")
	}
	return nil
}

func validateMCPSyncTimeouts(cfg MCPConfig) error {
	if cfg.DefaultSyncTimeout <= 0 {
		return errors.New("mcp.default_sync_timeout must be > 0 when mcp.enabled is true")
	}
	if cfg.MaxSyncTimeout <= 0 {
		return errors.New("mcp.max_sync_timeout must be > 0 when mcp.enabled is true")
	}
	if cfg.DefaultSyncTimeout > cfg.MaxSyncTimeout {
		return errors.New("mcp.default_sync_timeout must be <= mcp.max_sync_timeout")
	}
	return nil
}

func appendIf(errs []string, err error) []string {
	if err != nil {
		errs = append(errs, err.Error())
	}
	return errs
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateHTTP() error {
	return firstErr(
		requireNonEmptyString("http.address", c.HTTP.Address),
		requirePositiveDuration("http.read_header_timeout", c.HTTP.ReadHeaderTimeout),
		requirePositiveDuration("http.shutdown_timeout", c.HTTP.ShutdownTimeout),
	)
}

func (c *Config) validateStorage() error {
	var blobErr error
	switch c.Storage.Blob.Kind {
	case "local":
		blobErr = requireNonEmptyString("storage.blob.root", c.Storage.Blob.Root)
	case "s3":
		blobErr = c.validateS3BlobConfig()
	case "tiered":
		blobErr = c.validateTieredBlobConfig()
	default:
		return fmt.Errorf("storage.blob.kind %q is not supported (want \"local\", \"s3\", or \"tiered\")", c.Storage.Blob.Kind)
	}
	return firstErr(
		blobErr,
		requireNonEmptyString("storage.cache.dir", c.Storage.Cache.Dir),
		requireNonNegativeInt64("storage.cache.max_bytes", c.Storage.Cache.MaxBytes, 0),
		requireNonNegativeDurationValue("storage.cache.entry_ttl", c.Storage.Cache.EntryTTL, "0 disables the TTL"),
		requirePositiveDuration("storage.cache.evict_interval", c.Storage.Cache.EvictInterval),
		validateCacheWatermarks(c.Storage.Cache),
	)
}

func validateCacheWatermarks(cache CacheConfig) error {
	if cache.HighWatermarkPercent == 1 || cache.HighWatermarkPercent < 0 || cache.HighWatermarkPercent >= 100 {
		return errors.New("storage.cache.high_watermark_percent must be between 2 and 99, or 0 to disable")
	}
	if cache.LowWatermarkPercent < 0 || cache.LowWatermarkPercent >= 100 {
		return errors.New("storage.cache.low_watermark_percent must be between 1 and 99, or 0 when high watermark is disabled")
	}
	if cache.HighWatermarkPercent == 0 && cache.LowWatermarkPercent != 0 {
		return errors.New("storage.cache.low_watermark_percent requires high_watermark_percent")
	}
	if cache.HighWatermarkPercent > 0 && cache.LowWatermarkPercent >= cache.HighWatermarkPercent {
		return errors.New("storage.cache.low_watermark_percent must be less than high_watermark_percent")
	}
	return requireNonNegativeInt64("storage.cache.min_free_bytes", cache.MinFreeBytes, 0)
}

func (c *Config) validateS3BlobConfig() error {
	s3 := c.Storage.Blob.S3
	if s3 == nil {
		return fmt.Errorf("storage.blob.s3 is required when kind is \"s3\" or \"tiered\"")
	}
	if (s3.AccessKeyID == "") != (s3.SecretAccessKey == "") {
		return fmt.Errorf("storage.blob.s3: access_key_id and secret_access_key must both be set or both be empty")
	}
	var endpointErr error
	if s3.Endpoint != "" {
		endpointErr = validateHTTPURL("storage.blob.s3.endpoint", s3.Endpoint)
	}
	return firstErr(
		requireNonEmptyString("storage.blob.s3.bucket", s3.Bucket),
		endpointErr,
	)
}

func (c *Config) validateTieredBlobConfig() error {
	if err := c.validateS3BlobConfig(); err != nil {
		return err
	}
	tiered := c.Storage.Blob.Tiered
	if tiered == nil {
		return errors.New("storage.blob.tiered is required when kind is \"tiered\"")
	}
	if tiered.Durability != "remote" && tiered.Durability != "local_journal" {
		return fmt.Errorf("storage.blob.tiered.durability %q is not supported (want \"remote\" or \"local_journal\")", tiered.Durability)
	}
	if tiered.Journal.Kind != "local" {
		return fmt.Errorf("storage.blob.tiered.journal.kind %q is not supported by the standalone runtime (want \"local\")", tiered.Journal.Kind)
	}
	if pathsOverlap(tiered.Journal.Dir, c.Storage.Cache.Dir) {
		return errors.New("storage.blob.tiered.journal.dir must not equal, contain, or be contained by storage.cache.dir because cache data is evictable")
	}
	return firstErr(
		requireNonEmptyString("storage.blob.tiered.journal.dir", tiered.Journal.Dir),
		requirePositiveInt("storage.blob.tiered.journal.max_pending_entries", tiered.Journal.MaxPendingEntries),
		requirePositiveInt64("storage.blob.tiered.journal.max_pending_bytes", tiered.Journal.MaxPendingBytes),
		requirePositiveInt64("storage.blob.tiered.journal.min_free_bytes", tiered.Journal.MinFreeBytes),
		requirePositiveDurationValue("storage.blob.tiered.replication.poll_interval", tiered.Replication.PollInterval),
		requirePositiveDurationValue("storage.blob.tiered.replication.retry_base", tiered.Replication.RetryBase),
		requirePositiveDurationValue("storage.blob.tiered.replication.retry_max", tiered.Replication.RetryMax),
		requirePositiveInt("storage.blob.tiered.replication.max_attempts", tiered.Replication.MaxAttempts),
		validateRetryRange(tiered.Replication),
	)
}

func pathsOverlap(first, second string) bool {
	if first == "" || second == "" {
		return false
	}
	first = canonicalPath(first)
	second = canonicalPath(second)
	if sameFileOrAncestor(first, second) || sameFileOrAncestor(second, first) {
		return true
	}
	for _, pair := range [][2]string{{first, second}, {second, first}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func sameFileOrAncestor(ancestor, path string) bool {
	ancestorInfo, err := os.Stat(ancestor)
	if err != nil {
		return false
	}
	for current := path; ; current = filepath.Dir(current) {
		if info, statErr := os.Stat(current); statErr == nil && os.SameFile(ancestorInfo, info) {
			return true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}

func canonicalPath(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	path = filepath.Clean(path)
	current := path
	var suffix []string
	for {
		if _, err := os.Lstat(current); err == nil {
			resolved, resolveErr := filepath.EvalSymlinks(current)
			if resolveErr == nil {
				for index := len(suffix) - 1; index >= 0; index-- {
					resolved = filepath.Join(resolved, suffix[index])
				}
				return filepath.Clean(resolved)
			}
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
	return path
}

func validateRetryRange(cfg TieredReplicationConfig) error {
	if cfg.RetryMax.AsDuration() < cfg.RetryBase.AsDuration() {
		return errors.New("storage.blob.tiered.replication.retry_max must be >= retry_base")
	}
	return nil
}

func (c *Config) validateGraph() error {
	if !c.Graph.Enabled {
		return nil
	}
	return firstErr(
		requireEnabledString("graph.enabled", "graph.url", c.Graph.URL),
		validateHTTPURL("graph.url", c.Graph.URL),
		requireEnabledString("graph.enabled", "graph.model", c.Graph.Model),
		requirePositiveIntPtr("graph.parallelism", c.Graph.Parallelism),
	)
}

func validateHTTPURL(name, raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s must be a valid URL: %w", name, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%s must include a scheme and host (got %q)", name, raw)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s scheme must be http or https (got %q)", name, parsed.Scheme)
	}
	return nil
}

func (c *Config) validateScheduler() error {
	if !c.SchedulerEnabled() || c.Scheduler.TickInterval == nil {
		return nil
	}
	return requirePositiveDuration("scheduler.tick_interval", c.Scheduler.TickInterval)
}

// requirePositiveDuration enforces "when present, must be > 0" on pointer
// duration fields. nil is allowed (caller falls back to a default via the
// helper accessors in build_inputs.go).
func requirePositiveDuration(name string, d *Duration) error {
	if d == nil {
		return nil
	}
	if d.AsDuration() <= 0 {
		return fmt.Errorf("%s must be > 0 when set, got %s", name, d.AsDuration())
	}
	return nil
}

func (c *Config) validateFormat() error {
	if c.Format.Kind != "duckdb" {
		return fmt.Errorf("format.kind %q is not supported (only \"duckdb\")", c.Format.Kind)
	}
	if c.Format.DuckDB.BuildThreads < 0 {
		return fmt.Errorf("format.duckdb.build_threads must be >= 0, got %d", c.Format.DuckDB.BuildThreads)
	}
	if c.Format.DuckDB.BuildThreads > maxConfiguredBuildThreads {
		return fmt.Errorf(
			"format.duckdb.build_threads must be <= %d, got %d",
			maxConfiguredBuildThreads, c.Format.DuckDB.BuildThreads,
		)
	}
	if c.Format.DuckDB.EmbedParallelism < 0 || c.Format.DuckDB.EmbedParallelism > maxConfiguredEmbedParallelism {
		return fmt.Errorf(
			"format.duckdb.embed_parallelism must be between 0 and %d, got %d",
			maxConfiguredEmbedParallelism, c.Format.DuckDB.EmbedParallelism,
		)
	}
	if err := validateMemoryLimit(c.Format.DuckDB.MemoryLimit); err != nil {
		return err
	}
	return nil
}

const maxConfiguredBuildThreads = 256

// Matches the ceiling the artifact format clamps to.
const maxConfiguredEmbedParallelism = 16

var memoryLimitSize = regexp.MustCompile(`(?i)^\s*(\d+)\s*(b|kb|mb|gb|tb)\s*$`)

// validateMemoryLimit rejects a bad size here rather than at the first query,
// and accepts "auto", which the runtime resolves from the host's ceiling.
func validateMemoryLimit(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return nil
	}
	if match := memoryLimitSize.FindStringSubmatch(trimmed); match != nil {
		if strings.Trim(match[1], "0") == "" {
			return fmt.Errorf("format.duckdb.memory_limit must be greater than zero, got %q", raw)
		}
		return nil
	}
	return fmt.Errorf(`format.duckdb.memory_limit must be a size such as 4GB or "auto", got %q`, raw)
}

func (c *Config) validateEmbedder() error {
	switch c.Embedder.Provider {
	case "ollama":
		return validateOllamaEmbedder(c.Embedder.Ollama)
	case "local":
		return validateLocalEmbedder(c.Embedder.Local)
	case "openai_compatible":
		return validateOpenAICompatibleEmbedder(c.Embedder.OpenAICompatible)
	default:
		return fmt.Errorf(
			"embedder.provider %q is not supported (expected \"ollama\", \"local\", or \"openai_compatible\")",
			c.Embedder.Provider,
		)
	}
}

func validateOllamaEmbedder(cfg *OllamaEmbedderConfig) error {
	if cfg == nil {
		return errors.New("embedder.ollama block is required when embedder.provider is \"ollama\"")
	}
	return firstErr(
		requireNonEmptyString("embedder.ollama.url", cfg.URL),
		requireNonEmptyString("embedder.ollama.model", cfg.Model),
	)
}

func validateLocalEmbedder(cfg *LocalEmbedderConfig) error {
	if cfg == nil {
		return errors.New("embedder.local block is required when embedder.provider is \"local\"")
	}
	return requirePositiveInt("embedder.local.dim", cfg.Dim)
}

func validateOpenAICompatibleEmbedder(cfg *OpenAICompatibleEmbedderConfig) error {
	if cfg == nil {
		return errors.New(
			"embedder.openai_compatible block is required when embedder.provider is \"openai_compatible\"",
		)
	}
	if err := firstErr(
		requireNonEmptyString("embedder.openai_compatible.base_url", cfg.BaseURL),
		validateHTTPURL("embedder.openai_compatible.base_url", cfg.BaseURL),
		requireNonEmptyString("embedder.openai_compatible.model", cfg.Model),
	); err != nil {
		return err
	}
	if cfg.Dimensions < 0 {
		return fmt.Errorf("embedder.openai_compatible.dimensions must be >= 0 when set, got %d", cfg.Dimensions)
	}
	return nil
}

func (c *Config) validateMongo() error {
	if c.Mongo == nil {
		return nil
	}
	if err := requireNonEmptyString("mongo.uri", c.Mongo.URI); err != nil {
		return errors.New("mongo.uri must be set when mongo block is present (reference a secret via ${VAR})")
	}
	if err := requireNonEmptyString("mongo.database", c.Mongo.Database); err != nil {
		return err
	}
	col := c.Mongo.Collections
	if col.Manifests == "" || col.Events == "" || col.Inbox == "" || col.Media == "" {
		return errors.New("mongo.collections must set all four names: manifests, events, inbox, media")
	}
	// Guard against all four names collapsing to the same collection: that
	// would silently corrupt state at runtime because every store would write
	// into the same Mongo collection.
	named := []struct {
		field string
		value string
	}{
		{"mongo.collections.manifests", col.Manifests},
		{"mongo.collections.events", col.Events},
		{"mongo.collections.inbox", col.Inbox},
		{"mongo.collections.media", col.Media},
	}
	seen := make(map[string]string, len(named))
	for _, n := range named {
		if prev, ok := seen[n.value]; ok {
			return fmt.Errorf("%s and %s must be distinct (both set to %q)", prev, n.field, n.value)
		}
		seen[n.value] = n.field
	}
	return nil
}

func (c *Config) validateWorkers() error {
	if err := validateWorkerDefaults(c.Workers.Defaults); err != nil {
		return err
	}
	pools := map[string]WorkerPool{
		"document_upsert":  c.Workers.DocumentUpsert,
		"document_chunked": c.Workers.DocumentChunked,
		"document_publish": c.Workers.DocumentPublish,
		"media_upload":     c.Workers.MediaUpload,
	}
	for name, p := range pools {
		if err := validateWorkerPool(name, p); err != nil {
			return err
		}
	}
	return nil
}

func validateWorkerDefaults(defaults WorkerDefaults) error {
	if defaults.MaxAttempts <= 0 {
		return errors.New("workers.defaults.max_attempts must be > 0")
	}
	if defaults.PollInterval <= 0 {
		return errors.New("workers.defaults.poll_interval must be > 0")
	}
	if defaults.VisibilityTimeout <= 0 {
		return errors.New("workers.defaults.visibility_timeout must be > 0")
	}
	return nil
}

func validateWorkerPool(name string, p WorkerPool) error {
	if p.Concurrency <= 0 {
		return fmt.Errorf("workers.%s.concurrency must be > 0", name)
	}
	return firstErr(
		requirePositiveOptionalInt(fmt.Sprintf("workers.%s.max_attempts", name), p.MaxAttempts),
		requirePositiveOptionalDuration(fmt.Sprintf("workers.%s.poll_interval", name), p.PollInterval),
		requirePositiveOptionalDuration(fmt.Sprintf("workers.%s.visibility_timeout", name), p.VisibilityTimeout),
	)
}

func requirePositiveOptionalInt(name string, value *int) error {
	if value != nil && *value <= 0 {
		return fmt.Errorf("%s must be > 0 when set", name)
	}
	return nil
}

func requirePositiveOptionalDuration(name string, value *Duration) error {
	if value != nil && *value <= 0 {
		return fmt.Errorf("%s must be > 0 when set", name)
	}
	return nil
}

func (c *Config) validateMedia() error {
	if !c.Media.Enabled {
		return nil
	}
	if err := requirePositiveInt64("media.max_bytes", c.Media.MaxBytes); err != nil {
		return err
	}
	if err := requirePositiveDurationValue("media.pending_ttl", c.Media.PendingTTL); err != nil {
		return err
	}
	if err := requirePositiveDurationValue("media.tombstone_grace", c.Media.TombstoneGrace); err != nil {
		return err
	}
	if err := requirePositiveDurationValue("media.upload_completion_ttl", c.Media.UploadCompletionTTL); err != nil {
		return err
	}
	// Tombstones must not outlive the pending window: otherwise a
	// tombstoned media row is still eligible for purge before an unreferenced
	// pending upload is GC'd, leading to confusing operator UX.
	if c.Media.TombstoneGrace > c.Media.PendingTTL {
		return fmt.Errorf("media.tombstone_grace (%s) must be <= media.pending_ttl (%s)",
			c.Media.TombstoneGrace.AsDuration(), c.Media.PendingTTL.AsDuration())
	}
	return nil
}

func (c *Config) validateCodeIndex() error {
	if err := firstErr(
		requireNonEmptyList("code_index.include", c.CodeIndex.Include),
		requireNoEmptyListItems("code_index.include", c.CodeIndex.Include),
		requireNoEmptyListItems("code_index.exclude", c.CodeIndex.Exclude),
		requirePositiveInt64Value("code_index.max_file_bytes", c.CodeIndex.MaxFileBytes),
		requirePositiveInt("code_index.chunk_size", c.CodeIndex.ChunkSize),
		requireNonNegativeInt("code_index.chunk_overlap", c.CodeIndex.ChunkOverlap),
		(kb.CodeIndexResourcePolicy{
			EmbedBatchSize: c.CodeIndex.EmbedBatchSize,
			MaxBatchBytes:  c.CodeIndex.MaxBatchBytes,
			Throttle:       c.CodeIndex.Throttle.AsDuration(),
			MaxHeapBytes:   c.CodeIndex.MaxHeapBytes,
			MaxRSSBytes:    c.CodeIndex.MaxRSSBytes,
			LargeRepoFiles: c.CodeIndex.LargeRepoFiles,
		}).Validate("code_index"),
	); err != nil {
		return err
	}
	if c.CodeIndex.ChunkOverlap >= c.CodeIndex.ChunkSize {
		return errors.New("code_index.chunk_overlap must be less than code_index.chunk_size")
	}
	return nil
}

// validateSharding enforces both per-field positive-int rules and the
// cross-field constraints from main.go's parseShardingPolicyFromEnv. Checks
// run on the user-supplied values where present and on the kb package
// defaults where absent, so a partially-specified block (e.g. user set
// query_shard_fanout but not adaptive max) is still validated end-to-end.
func (c *Config) validateSharding() error {
	resolved := c.Sharding.resolve()
	return firstErr(
		validateShardingPositiveFields(c.Sharding),
		validateCompactionTombstoneRatio(c.Sharding.CompactionTombstoneRatio),
		validateResolvedShardingFanout(c.Sharding, resolved),
	)
}

func validateShardingPositiveFields(cfg ShardingConfig) error {
	return firstErr(
		checkPositive64("shard_trigger_bytes", cfg.ShardTriggerBytes),
		checkPositive("shard_trigger_vector_rows", cfg.ShardTriggerVectorRows),
		checkPositive64("target_shard_bytes", cfg.TargetShardBytes),
		checkPositive("max_vector_rows_per_shard", cfg.MaxVectorRowsPerShard),
		checkPositive("query_shard_fanout", cfg.QueryShardFanout),
		checkPositive("query_shard_fanout_adaptive_max", cfg.QueryShardFanoutAdaptiveMax),
		checkPositive("query_shard_parallelism", cfg.QueryShardParallelism),
		checkPositive("query_shard_local_topk_multiplier", cfg.QueryShardLocalTopKMult),
		checkPositive("small_kb_max_shards", cfg.SmallKBMaxShards),
		checkPositive("compaction_min_shard_count", cfg.CompactionMinShardCount),
	)
}

func validateCompactionTombstoneRatio(ratio *float64) error {
	if ratio == nil {
		return nil
	}
	r := *ratio
	if r <= 0 || r > 1 {
		return fmt.Errorf("sharding.compaction_tombstone_ratio must be in (0, 1], got %g", r)
	}
	return nil
}

func validateResolvedShardingFanout(cfg ShardingConfig, resolved kb.ShardingPolicy) error {
	const maxSafeFanout = 64
	const maxTopKMult = 16
	if resolved.QueryShardFanout > maxSafeFanout {
		return fmt.Errorf("sharding.query_shard_fanout must be <= %d", maxSafeFanout)
	}
	if resolved.QueryShardFanoutAdaptiveMax > maxSafeFanout {
		return fmt.Errorf("sharding.query_shard_fanout_adaptive_max must be <= %d", maxSafeFanout)
	}
	if resolved.QueryShardFanoutAdaptiveMax < resolved.QueryShardFanout {
		return fmt.Errorf(
			"sharding: query_shard_fanout_adaptive_max (%d, %s) must be >= query_shard_fanout (%d, %s)",
			resolved.QueryShardFanoutAdaptiveMax,
			provenance(cfg.QueryShardFanoutAdaptiveMax != nil),
			resolved.QueryShardFanout,
			provenance(cfg.QueryShardFanout != nil),
		)
	}
	if resolved.QueryShardLocalTopKMult > maxTopKMult {
		return fmt.Errorf("sharding.query_shard_local_topk_multiplier must be <= %d", maxTopKMult)
	}
	return nil
}

func provenance(userSet bool) string {
	if userSet {
		return "user-set"
	}
	return "from default"
}

func checkPositive(name string, v *int) error {
	if v == nil {
		return nil
	}
	if *v <= 0 {
		return fmt.Errorf("sharding.%s must be > 0 when set, got %d", name, *v)
	}
	return nil
}

func checkPositive64(name string, v *int64) error {
	if v == nil {
		return nil
	}
	if *v <= 0 {
		return fmt.Errorf("sharding.%s must be > 0 when set, got %d", name, *v)
	}
	return nil
}

func requireNonEmptyString(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be set", name)
	}
	return nil
}

func requireEnabledString(flagName, valueName, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be set when %s is true", valueName, flagName)
	}
	return nil
}

func requirePositiveInt(name string, value int) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", name, value)
	}
	return nil
}

func requirePositiveInt64(name string, value int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", name, value)
	}
	return nil
}

func requirePositiveInt64Value(name string, value int64) error {
	return requirePositiveInt64(name, value)
}

func requireNonNegativeInt(name string, value int) error {
	if value < 0 {
		return fmt.Errorf("%s must be >= 0, got %d", name, value)
	}
	return nil
}

func requireNonNegativeInt64(name string, value int64, floor int64) error {
	if value < floor {
		return fmt.Errorf("%s must be >= %d", name, floor)
	}
	return nil
}

func requirePositiveIntPtr(name string, value *int) error {
	if value == nil {
		return nil
	}
	if *value <= 0 {
		return fmt.Errorf("%s must be > 0 when set, got %d", name, *value)
	}
	return nil
}

func requirePositiveDurationValue(name string, value Duration) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func requireNonEmptyList(name string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("%s must include at least one pattern", name)
	}
	return nil
}

func requireNoEmptyListItems(name string, values []string) error {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not contain empty patterns", name)
		}
	}
	return nil
}

func requireNonNegativeDurationValue(name string, value Duration, note string) error {
	if value < 0 {
		msg := fmt.Sprintf("%s must be >= 0", name)
		if strings.TrimSpace(note) != "" {
			msg += " (" + note + ")"
		}
		return errors.New(msg)
	}
	return nil
}

// resolve returns a kb.ShardingPolicy with user-supplied values overlaid on
// kb.DefaultShardingPolicy.
func (s ShardingConfig) resolve() kb.ShardingPolicy {
	p := kb.DefaultShardingPolicy()
	applyInt64Ptr(&p.ShardTriggerBytes, s.ShardTriggerBytes)
	applyIntPtr(&p.ShardTriggerVectorRows, s.ShardTriggerVectorRows)
	applyInt64Ptr(&p.TargetShardBytes, s.TargetShardBytes)
	applyIntPtr(&p.MaxVectorRowsPerShard, s.MaxVectorRowsPerShard)
	applyIntPtr(&p.QueryShardFanout, s.QueryShardFanout)
	applyIntPtr(&p.QueryShardFanoutAdaptiveMax, s.QueryShardFanoutAdaptiveMax)
	applyIntPtr(&p.QueryShardParallelism, s.QueryShardParallelism)
	applyIntPtr(&p.QueryShardLocalTopKMult, s.QueryShardLocalTopKMult)
	applyIntPtr(&p.SmallKBMaxShards, s.SmallKBMaxShards)
	applyIntPtr(&p.CompactionMinShardCount, s.CompactionMinShardCount)
	applyFloat64Ptr(&p.CompactionTombstoneRatio, s.CompactionTombstoneRatio)
	if s.CompactionEnabled != nil {
		p.CompactionEnabled = *s.CompactionEnabled
		p.CompactionEnabledSet = true
	}
	return p
}

func applyIntPtr(target *int, value *int) {
	if value != nil {
		*target = *value
	}
}

func applyInt64Ptr(target *int64, value *int64) {
	if value != nil {
		*target = *value
	}
}

func applyFloat64Ptr(target *float64, value *float64) {
	if value != nil {
		*target = *value
	}
}
