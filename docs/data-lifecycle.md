# Data Lifecycle

## Persisted Model (Shard-Only)

Each KB is persisted as:

- Manifest key: `<kb_id>.duckdb.manifest.json`
- Shard keys: `<kb_id>.duckdb.shards/<content-hash>/shard-xxxxx.duckdb`

Each shard key points to an independently queryable DuckDB file.
There is no monolithic `<kb_id>.duckdb` source of truth.

Shard keys are content-addressed by the source DB hash, so the same content
always produces the same key. Concurrent writers uploading identical shards
are idempotent.

## Shard Contents

Shard files are self-contained for retrieval:

- `docs` — document rows with HNSW-indexed embedding vectors
- `entities` — extracted named entities (graph)
- `edges` — entity relationships (graph)
- `doc_entities` — doc-to-entity link table (graph)

Graph rows are doc-scoped to the docs present in each shard.

## Write Path

`UpsertDocsAndUpload*` and `DeleteDocsAndUpload*` run:

1. Acquire per-KB write lease.
2. Materialize mutable state from current manifest shards.
3. Apply mutation transaction (tombstones for deletes, upsert for writes).
4. Checkpoint.
5. Split snapshot into fixed-size shard DuckDB files; each has its own HNSW index.
   - Row assignment uses `LIMIT/OFFSET` ordered by doc ID — deterministic for a given snapshot.
   - Tombstoned rows are excluded at shard build time, reclaiming storage without a compaction pass.
6. Upload shard files unconditionally (content-addressed; idempotent).
7. Publish new manifest via CAS (`ManifestStore.UpsertIfMatch(kbID, manifest, expectedVersion)`).
8. Best-effort delete legacy keys (`<kb_id>.duckdb`, `<kb_id>.snapshot.json`).
9. Run cache eviction sweep.

On CAS conflict (`ErrBlobVersionMismatch`) the operation aborts. Retry-capable callers
re-read the manifest version and re-run with quadratic backoff.

## Query Path

Vector and graph queries fetch the manifest, select shards, and execute against
shard DuckDB files only.

**Manifest fetch** uses `ManifestStore.Get(kbID)`. An absent manifest returns
`ErrManifestNotFound`, which callers map to `ErrKBUninitialized`.

**Shard selection:**

- `shards ≤ SmallKBMaxShards` (default 2): all shards queried; centroid ranking skipped.
- `shards > SmallKBMaxShards`: shards ranked by squared Euclidean distance from the
  query vector to each shard's centroid. Top `QueryShardFanout` (default 4) selected.
  Shards without centroids fall back to descending `VectorRows`.
  Scores are precomputed once (O(n)) before sorting — not inside the comparator.

**Shard download and verification** (cache miss path):

1. `os.Stat` → check `SizeBytes` (cheap).
2. `fileContentSHA256` → check `SHA256` (full read, only if size passes).
3. Atomic rename into cache on success.

**Shard execution:** selected shards queried in parallel (`QueryShardParallelism` goroutines).
Results merged globally by ascending distance, ties broken deterministically by ID, content,
shard index, local index.

## Graph Query Semantics

`POST /rag/query` supports:

- `search_mode=vector` (default)
- `search_mode=graph`
- `search_mode=adaptive`

`graph` mode is strict:

- If shard graph data is unavailable for the KB, request fails with
  `graph query requested but graph data is unavailable` (`400` at HTTP layer).

Query mode does not invoke the Ollama grapher at query time.

## Ingest API Contract

`POST /rag/ingest` requires `graph_enabled`.

- `true`: require graph extraction for this request; fail if graph builder is not configured.
- `false`: explicitly skip graph extraction.

## Rotation and Compaction

Sharding behavior is policy-driven:

- Publish/rotation thresholds (`ShardTriggerBytes`, `ShardTriggerVectorRows`)
- Target shard sizing (`TargetShardBytes`, `MaxVectorRowsPerShard`)
- Query fanout (`QueryShardFanout`, `QueryShardFanoutAdaptiveMax`, `QueryShardParallelism`)

**Compaction** merges shards using size-tiered selection (log₂ bucketing by size relative
to `TargetShardBytes`) or tombstone pressure (`TombstoneRatio ≥ CompactionTombstoneRatio`).
Up to `maxCompactionCandidates` (4) shards merged per pass. Replaced shards are enqueued
for delayed GC (grace window) to allow in-flight readers to finish before deletion.

## Snapshot Reconstruction

`DownloadSnapshotFromShards` reconstructs a single DuckDB file from the manifest:

1. Fetch manifest via `ManifestStore.Get`.
2. For each shard: download, verify size, verify SHA256 (skipped if no checksum in manifest).
3. ATTACH each shard read-only; INSERT docs and graph tables into a new combined DB.
4. Build HNSW index over the combined `docs` table.
5. Checkpoint and atomic rename into the destination path.
