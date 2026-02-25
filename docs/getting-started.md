# Getting Started

## Prerequisites

- Go toolchain matching module requirements (`go.mod`)
- CGO-capable environment for DuckDB driver
- Pre-downloaded DuckDB extensions in `.duckdb/extensions/` (shipped with the repo; no network access needed by default)

## Run Locally

From `kbcorego/`:

```bash
go test ./... -count=1
go run .
```

Default server address is `127.0.0.1:8080`.

## Quick Health Check

```bash
curl -s http://127.0.0.1:8080/healthz
```

Expected:

```json
{"status":"ok"}
```

## Minimal Runtime Env

- `KBCORE_BLOB_ROOT` (default `./.temp/fixtures`)
- `KBCORE_CACHE_DIR` (default `./.temp/cache`)
- `KBCORE_MEMORY_LIMIT` (default `128MB`)
- `KBCORE_HTTP_ADDR` (default `127.0.0.1:8080`)

## DuckDB Extension Env

- `KBCORE_DUCKDB_EXTENSION_DIR` (default `./.duckdb/extensions`) — root directory for pre-downloaded DuckDB extensions
- `KBCORE_DUCKDB_EXTENSION_OFFLINE` (default `true`) — when `true`, extensions are only `LOAD`ed from the local directory (no runtime `INSTALL`); set to `false` to allow downloading extensions on first use

## Sharding and Query Planner Env

These defaults are tuned for thresholded sharding in multi-tenant deployments:

- `KBCORE_SHARD_TRIGGER_BYTES` (default `67108864` / `64MB`)
- `KBCORE_SHARD_TRIGGER_VECTOR_ROWS` (default `150000`)
- `KBCORE_TARGET_SHARD_BYTES` (default `33554432` / `32MB`)
- `KBCORE_MAX_VECTOR_ROWS_PER_SHARD` (default `75000`)
- `KBCORE_QUERY_SHARD_FANOUT` (default `4`)
- `KBCORE_QUERY_SHARD_FANOUT_ADAPTIVE_MAX` (default `6`)
- `KBCORE_QUERY_SHARD_PARALLELISM` (default `4`)
- `KBCORE_SMALL_KB_MAX_SHARDS` (default `2`)
- `KBCORE_COMPACTION_ENABLED` (default `true`)
- `KBCORE_COMPACTION_MIN_SHARD_COUNT` (default `8`)
- `KBCORE_COMPACTION_TOMBSTONE_RATIO` (default `0.20`)

Sharding activation is based on snapshot bytes and vector row counts. It does not depend on source document count or chunk count.

Example:

```bash
KBCORE_BLOB_ROOT=./.temp/fixtures \
KBCORE_CACHE_DIR=./.temp/cache \
KBCORE_MEMORY_LIMIT=256MB \
KBCORE_HTTP_ADDR=127.0.0.1:8080 \
go run .
```

Sharding example:

```bash
KBCORE_SHARD_TRIGGER_BYTES=67108864 \
KBCORE_TARGET_SHARD_BYTES=33554432 \
KBCORE_QUERY_SHARD_FANOUT=4 \
go run .
```

## Manifest Store

By default, manifests are stored alongside shard data in the blob store. To use MongoDB instead, set:

- `KBCORE_MANIFEST_MONGO_URI` (default empty) — MongoDB connection string (e.g. `mongodb://localhost:27017`). When set, enables the Mongo-backed manifest store.
- `KBCORE_MANIFEST_MONGO_DB` (default `kbcore`) — database name.
- `KBCORE_MANIFEST_MONGO_COLLECTION` (default `manifests`) — collection name.

When `KBCORE_MANIFEST_MONGO_URI` is empty, the blob-backed default is used and no MongoDB connection is made.

## For Multi-Pod Deployments

- Use shared blob storage (for example S3) and keep `KBCORE_CACHE_DIR` pod-local (ephemeral or node-local PVC).
- For multi-pod writes and compaction, use `RedisWriteLeaseManager` so workers coordinate per KB ID.
- Keep CAS semantics enabled on manifest writes (`ManifestStore.UpsertIfMatch` path) as the final correctness guard during races.

# HNSW Maintenance

HNSW index refresh is shard-build driven.

## Current Behavior

- Every shard DuckDB file is built with a `docs` HNSW index.
- Compaction outputs replacement shard files with fresh HNSW indexes.
- There is no standalone HNSW scheduler, rebuild endpoint, or rebuild metrics surface.

## Operational Impact

- Index freshness follows normal mutation publish and compaction flow.
- Query performance tuning is controlled through sharding and compaction policy, not a separate HNSW maintenance subsystem.
