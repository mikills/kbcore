package duckdb

import (
	"context"
	"time"

	kb "github.com/mikills/minnow/kb"
)

// DepOption configures DuckDB-specific fields on DuckDBArtifactDeps.
type DepOption func(*DuckDBArtifactDeps)

// WithMemoryLimit sets the DuckDB per-connection memory limit (e.g. "128MB").
func WithMemoryLimit(limit string) DepOption {
	return func(d *DuckDBArtifactDeps) { d.MemoryLimit = limit }
}

// WithExtensionDir sets the DuckDB extension directory path.
func WithExtensionDir(dir string) DepOption {
	return func(d *DuckDBArtifactDeps) { d.ExtensionDir = dir }
}

// WithOfflineExt controls whether DuckDB extensions are loaded offline.
func WithOfflineExt(offline bool) DepOption {
	return func(d *DuckDBArtifactDeps) { d.OfflineExt = offline }
}

// NewDepsFromKB constructs DuckDBArtifactDeps by wiring common fields from a
// *kb.KB instance. DuckDB-specific settings (memory limit, extension dir, etc.)
// are applied via functional options.
func NewDepsFromKB(k *kb.KB, opts ...DepOption) DuckDBArtifactDeps {
	deps := DuckDBArtifactDeps{
		BlobStore:                  k.BlobStore,
		ManifestStore:              k.ManifestStore,
		CacheDir:                   k.CacheDir,
		ExtensionDir:               ResolveExtensionDir(),
		OfflineExt:                 true,
		ShardingPolicy:             k.ShardingPolicy,
		Embed:                      k.Embed,
		GraphBuilder:               graphBuilderFromKB(k),
		EvictCacheIfNeeded:         k.EvictCacheIfNeeded,
		ReserveCache:               k.ReserveCache,
		LockFor:                    k.LockFor,
		AcquireWriteLease:          k.AcquireWriteLease,
		EnqueueReplacedShardsForGC: k.EnqueueReplacedShardsForGC,
		ReconcileShardBlobs: func(ctx context.Context, kbID string, active []kb.SnapshotShardMetadata) error {
			return k.EnqueueOrphanedShardBlobs(ctx, kbID, active, time.Time{})
		},
		Metrics: k,
	}
	for _, opt := range opts {
		opt(&deps)
	}
	return deps
}

func graphBuilderFromKB(k *kb.KB) func() *kb.GraphBuilder {
	return func() *kb.GraphBuilder { return k.GraphBuilder }
}
