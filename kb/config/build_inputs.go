package config

import (
	"time"

	"github.com/mikills/minnow/kb"
)

const (
	defaultHTTPReadHeaderTimeout = 5 * time.Second
	defaultHTTPShutdownTimeout   = 5 * time.Second
	defaultSchedulerTick         = 1 * time.Minute
	defaultCacheEvictInterval    = 30 * time.Second
	defaultGraphParallelism      = 2
)

// ShardingPolicy returns the resolved kb.ShardingPolicy, overlaying any
// user-supplied values on top of kb.DefaultShardingPolicy. The returned
// policy is not normalized further. callers that pass it to NewKB via
// WithShardingPolicy get normalization there.
func (c *Config) ShardingPolicy() kb.ShardingPolicy {
	return c.Sharding.resolve()
}

// MediaGCConfig returns the kb.MediaGCConfig derived from media timings.
func (c *Config) MediaGCConfig() kb.MediaGCConfig {
	return kb.MediaGCConfig{
		PendingTTL:       c.Media.PendingTTL.AsDuration(),
		TombstoneGrace:   c.Media.TombstoneGrace.AsDuration(),
		UploadCompletion: c.Media.UploadCompletionTTL.AsDuration(),
	}
}

// PoolConfigFor resolves a per-pool WorkerPoolConfig, applying any per-pool
// overrides on top of Workers.Defaults.
func (c *Config) PoolConfigFor(p WorkerPool) kb.WorkerPoolConfig {
	d := c.Workers.Defaults
	cfg := kb.WorkerPoolConfig{
		Concurrency:       p.Concurrency,
		MaxAttempts:       d.MaxAttempts,
		PollInterval:      d.PollInterval.AsDuration(),
		VisibilityTimeout: d.VisibilityTimeout.AsDuration(),
	}
	if p.MaxAttempts != nil {
		cfg.MaxAttempts = *p.MaxAttempts
	}
	if p.PollInterval != nil {
		cfg.PollInterval = p.PollInterval.AsDuration()
	}
	if p.VisibilityTimeout != nil {
		cfg.VisibilityTimeout = p.VisibilityTimeout.AsDuration()
	}
	return cfg
}

// HTTPReadHeaderTimeout returns the resolved read-header timeout
// (user value if set, default otherwise).
func (c *Config) HTTPReadHeaderTimeout() time.Duration {
	if c.HTTP.ReadHeaderTimeout == nil {
		return defaultHTTPReadHeaderTimeout
	}
	return c.HTTP.ReadHeaderTimeout.AsDuration()
}

// HTTPShutdownTimeout returns the resolved graceful-shutdown timeout.
func (c *Config) HTTPShutdownTimeout() time.Duration {
	if c.HTTP.ShutdownTimeout == nil {
		return defaultHTTPShutdownTimeout
	}
	return c.HTTP.ShutdownTimeout.AsDuration()
}

// MCPHTTPStateless reports whether HTTP MCP requests avoid retained sessions.
func (c *Config) MCPHTTPStateless() bool { return c.MCP.HTTPStateless }

// SchedulerEnabled reports whether periodic maintenance jobs should run.
func (c *Config) SchedulerEnabled() bool { return c.Scheduler.Enabled }

// SchedulerTick returns the resolved scheduler tick interval.
func (c *Config) SchedulerTick() time.Duration {
	if c.Scheduler.TickInterval == nil {
		return defaultSchedulerTick
	}
	return c.Scheduler.TickInterval.AsDuration()
}

// CacheEvictInterval returns the resolved cache eviction interval.
func (c *Config) CacheEvictInterval() time.Duration {
	if c.Storage.Cache.EvictInterval == nil {
		return defaultCacheEvictInterval
	}
	return c.Storage.Cache.EvictInterval.AsDuration()
}

// GraphParallelism returns the resolved Ollama grapher parallelism.
// Only meaningful when c.Graph.Enabled is true.
func (c *Config) GraphParallelism() int {
	if c.Graph.Parallelism == nil {
		return defaultGraphParallelism
	}
	return *c.Graph.Parallelism
}
