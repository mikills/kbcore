# minnow Configuration Reference

minnow is configured from a single YAML file, discovered at:

1. `$MINNOW_CONFIG` if set, or
2. `./minnow.yaml` in the process working directory, or
3. the per-user config path (`~/Library/Application Support/minnow/minnow.yaml`
   on macOS, `~/.config/minnow/minnow.yaml` on Linux).

If none are present the process exits with a clear error. See
[`examples/minnow.min.yaml`](../examples/minnow.min.yaml) for the smallest valid
file, [`examples/minnow.dev.openai.yaml`](../examples/minnow.dev.openai.yaml)
for OpenAI-backed MCP development, and [`examples/minnow.yaml`](../examples/minnow.yaml)
for a full-field reference.

For globally installed MCP use, generate a starter OpenAI-backed config with:

```bash
minnow config init dev-openai
```

## Rules

- **Unknown keys are rejected.** A typo like `concurency` fails the load at
  startup with the line number and key path.
- **Durations are strings** in Go's `time.ParseDuration` form: `500ms`, `5s`,
  `5m`, `2h30m`.
- **Secrets stay in the environment.** Any value sourced from a secret (Mongo
  URI, API tokens, credentials) must be written as `${VAR}` and `VAR` must be
  set before startup. Unresolved `${VAR}` references aggregate into a single
  error listing every missing name.
- **Relative paths** (`storage.blob.root`, `storage.cache.dir`,
  `format.duckdb.extension_dir`) resolve against the YAML file's directory,
  not the process working directory. Paths that resolve *outside* the config's
  base directory (e.g. `root: ../../etc/foo`) are rejected. Explicit absolute
  paths are allowed and audit-logged.
- **Only one YAML document per file.** A second `---` block fails the load.

## Bootstrap environment variables

`MINNOW_CONFIG` and the logging vars are read before the config file. Other
bootstrap overrides are applied to the parsed config:

| Env var             | Purpose                                                                    |
| ------------------- | -------------------------------------------------------------------------- |
| `MINNOW_CONFIG`     | Path to the YAML config file. Overrides default discovery.                 |
| `MINNOW_LOG_FORMAT` | Logger format (`text` / `json`). Read before the YAML so parse errors log.|
| `MINNOW_LOG_LEVEL`  | Minimum log level (`debug` / `info` / `warn` / `error`). Defaults to `info`. |
| `MINNOW_DUCKDB_MEMORY_LIMIT` | Overrides `format.duckdb.memory_limit`, for deployments whose YAML is baked into an image. |
| `MINNOW_CACHE_MAX_BYTES` | Overrides `storage.cache.max_bytes`, for the same reason. Accepts bytes or a `KB` / `MB` / `GB` suffix. |
| `MINNOW_OPENAI_EMBEDDING_DIMENSIONS` | Overrides `embedder.openai_compatible.dimensions`. Existing knowledge bases must be rebuilt after changing it. |

All other deployment knobs are YAML fields.

## Schema

### `http`

| Field                 | Type     | Default           | Notes                            |
| --------------------- | -------- | ----------------- | -------------------------------- |
| `address`             | string   | `127.0.0.1:8080`  | Bind address.                    |
| `read_header_timeout` | duration | `5s`              |                                  |
| `shutdown_timeout`    | duration | `5s`              |                                  |

### `storage.blob`

| Field  | Type   | Default             | Notes                                          |
| ------ | ------ | ------------------- | ---------------------------------------------- |
| `kind` | enum   | `local`             | `local` \| `s3` \| `tiered`.                  |
| `root` | path   | `./.temp/fixtures`  | Relative to YAML file. Used when `kind: local`.|

When `kind: s3`, provide an `s3` block. Works with AWS S3, MinIO, Cloudflare R2,
and any S3-compatible store via `endpoint`.

| Field                    | Type   | Default     | Notes                                                              |
| ------------------------ | ------ | ----------- | ------------------------------------------------------------------ |
| `s3.bucket`              | string | —           | Required.                                                          |
| `s3.region`              | string | `us-east-1` |                                                                    |
| `s3.prefix`              | string | empty       | Key prefix for all objects.                                        |
| `s3.lease_prefix`        | string | empty       | Prefix-relative namespace for per-KB write leases and the tiered journal owner record. |
| `s3.endpoint`            | string | empty       | HTTP(S) URL for MinIO / R2 / other S3-compatible stores.           |
| `s3.access_key_id`       | string | empty       | Use `${VAR}`. Both keys must be set together, or both empty.       |
| `s3.secret_access_key`   | string | empty       | Use `${VAR}`. When both are empty, the AWS credential chain is used.|

When `kind: tiered`, the same `s3` block is the cold store and a persistent
local journal owns writes until S3 replication is confirmed. The endpoint must
preserve user metadata and ETags and correctly implement conditional
`PutObject` and `DeleteObject` (`If-Match` / `If-None-Match`); not every S3-compatible service
provides those semantics:

```yaml
storage:
  blob:
    kind: tiered
    s3:
      bucket: my-minnow-bucket
      region: us-east-1
      prefix: production/
      lease_prefix: leases/
    tiered:
      durability: remote # or local_journal
      journal:
        kind: local
        dir: /var/lib/minnow/journal
        max_pending_entries: 10000
        max_pending_bytes: 1073741824
        min_free_bytes: 268435456
      replication:
        poll_interval: 100ms
        retry_base: 250ms
        retry_max: 30s
        max_attempts: 20
```

`remote` acknowledges a mutation only after its ordered S3 replication has
completed and its journal transition is committed. Remote-mode reads use the
same visibility barrier; if an ambiguous replication result cannot yet be
reconciled, reads fail closed rather than expose an unacknowledged object.
`local_journal` acknowledges after the payload and catalog/outbox
transaction are durable on the local volume; losing that volume before the
backlog drains can therefore lose acknowledged writes. Replication always
starts immediately. Disk watermarks control warm-cache eviction, not when S3
receives its first copy.

The built-in journal uses bbolt plus content-addressed payload files, retains
failed entries, applies entry/byte backpressure, and recovers pending work after
a restart. `journal.min_free_bytes` rejects a payload before staging when the
journal filesystem would cross its emergency reserve; pending payloads are
never evicted. A restart requeues previously exhausted entries from attempt zero.
Startup claims a non-expiring S3 prefix-ownership record and
inventories the remote prefix, so S3 must be reachable when the process starts
even in `local_journal` mode. The claim is released on clean shutdown only when
the replication backlog is empty; after a crash, only the same persistent
journal identity can resume it. Every replicated put or delete also carries an
object-level S3 precondition and operation identity. Keys below
`s3.lease_prefix` are reserved for control records (`kb/` for write leases and
`journal/` for ownership) and excluded from the tiered
catalog. The claimed object prefix must not be mutated by other applications or
administrative jobs while Minnow is running. If the journal volume is
permanently lost, an operator must first prove the old process is stopped and
then manually remove `<prefix><lease_prefix>journal/owner.lock` before a new
journal can inventory and claim the S3 prefix. Any unreplicated tail is lost in
that disaster scenario.
Embedded users can supply another implementation of
`journal.Store` through `configruntime.BuildOptions.ReplicationJournal`; custom
implementations must provide the same atomic catalog/outbox and durable payload
ownership guarantees. See [`examples/minnow.tiered.yaml`](../examples/minnow.tiered.yaml)
for a complete configuration.

### `storage.cache`

| Field            | Type     | Default         | Notes                        |
| ---------------- | -------- | --------------- | ---------------------------- |
| `dir`            | path     | `./.temp/cache` | Relative to YAML file.       |
| `max_bytes`      | int      | `0`             | `0` = unbounded. `MINNOW_CACHE_MAX_BYTES` overrides it at startup. |
| `entry_ttl`      | duration | `0s`            | `0` = no TTL.                |
| `warm_shards`                  | int      | `0`             | Pre-download the N most-recently-sealed shards per KB into the cache at startup, in the background. `0` = off. |
| `evict_interval`               | duration | `30s`           |                              |
| `high_watermark_percent`       | int      | `0`             | Filesystem-used percentage that triggers eviction. `0` = disabled; otherwise 2–99. |
| `low_watermark_percent`        | int      | high minus 10   | Once triggered, evict toward this lower filesystem-used percentage. Must be below `high_watermark_percent`. |
| `min_free_bytes`               | int      | `0`             | Also trigger eviction when filesystem-available bytes fall below this reserve. |

Open ingest sessions are excluded from `max_bytes` because they cannot be
evicted safely. `minnow_cache_held_bytes` reports their size; use filesystem
watermarks or `min_free_bytes` to protect bounded volumes.

Queries run against local shard files, so a shard not yet in `dir` is fetched
from the blob store on first touch (a cold `GET` in S3 or tiered mode).
`warm_shards` pays that cost up front at startup instead of on the first query
per shard. Watermarks use filesystem capacity, include space consumed outside
the cache, and use high/low hysteresis to avoid repeated eviction at one
boundary. Sealed DuckDB query shards are ranked and evicted individually by
last access. Before a cold download, Minnow reserves its declared shard size,
evicts toward the configured disk target, and verifies actual filesystem free
space again after removal and installation. Minnow establishes an eviction
barrier, drains active and in-flight DuckDB opens, and prevents reopen until the
shard file is removed. Legacy cache
layouts without a `query-shards` directory remain whole-KB eviction entries.

For a small always-on server, set finite cache limits rather than accepting the
unbounded disk-cache defaults. A conservative 1 GB VPS profile is:

```yaml
storage:
  cache:
    max_bytes: 268435456 # 256 MiB
    entry_ttl: 30m
format:
  duckdb:
    memory_limit: 64MB
    build_threads: 1 # index builds hold memory too
sharding:
  query_shard_fanout: 2
  query_shard_fanout_adaptive_max: 2
  query_shard_parallelism: 1
```

`memory_limit: auto` derives all of this from the host instead. Set `GOMEMLIMIT`
yourself only if you want a specific Go-heap figure; `auto` then sizes DuckDB
around it rather than on top of it.

### `format`

| Field                  | Type   | Default         | Notes                              |
| ---------------------- | ------ | --------------- | ---------------------------------- |
| `kind`                 | string | `duckdb`        | Only `duckdb` is supported today.  |
| `duckdb.memory_limit`  | string | sized from the host | A size such as `4GB`, passed to DuckDB verbatim. Unset sizes from the host (see below), falling back to `128MB` where the ceiling cannot be read. `auto` is the same sizing but fails rather than falling back. `MINNOW_DUCKDB_MEMORY_LIMIT` overrides it at startup, for deployments whose config is baked into an image. |
| `duckdb.build_threads` | int    | `min(GOMAXPROCS, 4)` | DuckDB threads while sealing or compacting a shard, capped at 256. Queries stay at one thread per shard because several are probed at once. Halves index build time on a 75k row shard at 512 dim; the gain flattens past four. All concurrent builds share one budget sized to the machine's cores, and each build is also held to the current GOMAXPROCS, which Go re-reads from the cgroup CPU quota about once a second. Raising a container's CPU limit therefore widens builds without a restart. Each thread raises DuckDB's per-operator memory reservation, so lower it alongside a small `memory_limit`. |
| `duckdb.embed_parallelism` | int | `4` | Embedding batches in flight during one upsert, capped at 16. A remote embedder spends each request waiting, so running several at once cuts the wall clock. All upserts share a process budget of 64, so raising this does not multiply across concurrent ingests. |
| `duckdb.temp_directory` | path  | unset           | Where DuckDB spills once a query exceeds `memory_limit`. Created if missing. Unset spills to a `.tmp` directory beside each shard, so the spill lands on whichever volume holds `storage.cache.dir`. Set it when that volume is smaller than the working set. |
| `duckdb.extension_dir` | path   | `./extensions`  | Relative to YAML file.             |
| `duckdb.offline`       | bool   | `false`         | If true, disables extension fetch. |

#### Sizing memory from the host

`memory_limit: auto` reads the ceiling this process runs under and divides it.
In a cgroup that is the tightest limit in the ancestry, from `memory.max`,
`memory.high`, or v1's `memory.limit_in_bytes`. Otherwise it is physical memory.

```
budget    = 90% of ceiling
Go heap   = 30% of budget, or your GOMEMLIMIT if you set one
DuckDB    = budget - Go heap
databases = min(16, DuckDB / what indexing max_shard_bytes needs)
```

`memory_limit` bounds only DuckDB's buffer manager. Documents and embeddings in
flight are Go allocations it never sees, so the Go heap needs its own share.
When the computed Go share falls below 256MiB the floor applies instead; a
`GOMEMLIMIT` you set is used as-is.

The divisor is the part to understand. `memory_limit` binds one database, not
the process, and minnow holds up to 16 shard readers open at once. A fanout
query across six shards runs six buffer managers, each entitled to the full
setting. So the budget is divided before it becomes a setting. On a 16 GiB host
that is a 14.4 GiB budget, a 4.3 GiB Go heap, and `645MB` per database.

Sixteen is a ceiling on that divisor, not a fixed one. A host that cannot give
sixteen databases enough to finish an index build keeps fewer instead, and the
reader cache is bounded by the same number, so the cache and the budget cannot
drift apart. A 2 GiB box with a 768MiB `GOMEMLIMIT` runs nine databases of
`119MB` rather than sixteen of `62MB`, none of which could seal a shard.

This costs something. A tight host caches fewer shard readers, so a fanout
query across more shards than the cache holds reopens databases it just closed.
Raising `sharding.max_shard_bytes` makes it worse: at 128MB the same 2 GiB box
affords four readers, under its own `query_shard_fanout_adaptive_max`. If the
`databases` count logged at startup is below your fanout, lower
`sharding.max_shard_bytes` or add memory. A bigger cache would only overcommit.

What one index build needs is measured, not assumed. `TestIndexBuildMemoryFloor`
walks `memory_limit` down until an HNSW build over a full shard stops finishing.

| rows x dimensions | raw vectors | floor | ratio |
| --- | --- | --- | --- |
| 18,750 x 512 | 36 MiB | 56MB | 1.53 |
| 37,500 x 512 | 73 MiB | 96MB | 1.31 |
| 75,000 x 256 | 73 MiB | 96MB | 1.31 |
| 37,500 x 768 | 109 MiB | 144MB | 1.31 |
| 56,250 x 640 | 137 MiB | 176MB | 1.28 |
| 75,000 x 512 | 146 MiB | 192MB | 1.31 |
| 100,000 x 384 | 146 MiB | 192MB | 1.31 |

Those are darwin/arm64. The `index-floor` CI job remeasures on linux/amd64,
where every shape came in at the same ratio or lower.

The floor tracks raw vector bytes, not row count. 75k rows at 256 dimensions and
37.5k at 512 measure identically, as do 75k x 512 and 100k x 384. The ratio
falls as a shard grows, so the planner uses 1.6 times raw, above the worst
measured, and never less than 64MiB.

Neither the row count nor the embedder's width is an input, because both cancel.
DuckDB stores a float vector at 0.87 times its raw size, measured at 256 through
1024 dimensions, so a shard file of a given size holds the same raw vector bytes
whatever the embedder:

```
rows per shard   = max_shard_bytes / (0.87 x dim x 4)
raw vector bytes = rows x dim x 4 = max_shard_bytes / 0.87
```

At the 64MB default that is 73 MiB of vectors and a 117MB floor. Documents
with text hold fewer vectors in the same bytes, so vectors-only is the worst
case and the one the planner sizes for.

Getting this wrong does not always fail cleanly. An allocation past
`memory_limit` can throw where nothing catches it and abort the process.

### Why shards have a maximum

`max_shard_bytes` is what makes the floor computable. Compaction merges the
densest size tier and never re-splits, so without a cap each round multiplies
the largest shard by the fan-in. Driving the real planner with 32MB seals used
to reach 128 MiB after 8 shards, 1.5 GiB after 67, and 63 GiB after 4000, at
which point the whole corpus sits in one file that no host can index and
sharding has stopped doing anything.

The cap defaults to 64MB, twice `target_shard_bytes`. Raising it raises the
memory every cached reader needs and lowers how many fit, and 64MB is the
largest cap that still leaves the 2 GiB deployment in `deploy/fly` caching more
readers than its own `query_shard_fanout_adaptive_max`. Lowering it leaves more
shards behind instead. A shard at the cap also stops merging, so tombstones
inside it are only reclaimed if it falls back under.

### `embedder`

| Field                            | Type   | Default                    | Notes                                                   |
| -------------------------------- | ------ | -------------------------- | ------------------------------------------------------- |
| `provider`                       | enum   | `ollama`                   | `ollama` \| `local` \| `openai_compatible`.             |
| `ollama.url`                     | string | `http://localhost:11434`   | Required when `provider = ollama`; uses `/api/embed`.   |
| `ollama.model`                   | string | `all-minilm`               |                                                         |
| `local.dim`                      | int    | `384`                      | Required (> 0) when `provider = local`.                 |
| `openai_compatible.base_url`     | string | `https://api.openai.com/v1` | Required when `provider = openai_compatible`.           |
| `openai_compatible.model`        | string | none                       | Required.                                               |
| `openai_compatible.token`        | string | none                       | Optional bearer token; omit for unauthenticated locals. |
| `openai_compatible.dimensions`   | int    | `0`                        | Optional; `0` omits the request field.                  |

The provider-specific block must be present when its provider is selected; the
loader does not auto-create an empty block. `openai_compatible` calls
`POST {base_url}/embeddings`, so use `base_url: http://localhost:11434/v1` for
Ollama's OpenAI-compatible API.

### `code_index`

Defaults used by Minnow's MCP code-indexing tools. The separate `codeindex` CLI
does not read this deployment file; it has a user-level client config described
below.

| Field                 | Type     | Default                                                                  | Notes                                      |
| --------------------- | -------- | ------------------------------------------------------------------------ | ------------------------------------------ |
| `include`             | []string | common source/config/docs extensions                                      | Glob-like relative path include patterns.  |
| `exclude`             | []string | common generated/vendor/build directories, `*.lock`, `.gitignore`        | Applied after includes.                    |
| `max_file_bytes`      | int      | `1048576`                                                                | Files larger than this are skipped.        |
| `chunk_size`          | int      | `1200`                                                                   | Target character size for code chunks.     |
| `chunk_overlap`       | int      | `120`                                                                    | Must be less than `chunk_size`.            |
| `include_untracked`   | bool     | `false`                                                                  | Include untracked non-ignored Git files.   |
| `embed_batch_size`    | int      | `32`                                                                     | Maximum chunks per embedding batch.        |
| `max_batch_bytes`     | int      | `262144`                                                                 | Maximum text bytes per embedding batch.    |
| `throttle`            | duration | `100ms`                                                                  | Delay between embedding batches.           |
| `max_heap_bytes`      | int      | `1073741824`                                                             | Abort indexing if the Go memory footprint (total obtained minus released to the OS) exceeds this guard. |
| `max_rss_bytes`       | int      | `1073741824`                                                             | Abort indexing if peak process resident memory exceeds this guard. Peak is measured since process start and never falls. |
| `large_repo_files`    | int      | `1000`                                                                   | CLI confirmation threshold for scanned files. |
| `require_confirm`     | bool     | `false`                                                                  | Require explicit confirmation for large repositories when set by callers/config. |

Code indexing respects `.gitignore` when Git is available, excludes likely
secret paths such as `.env*`, `*.pem`, `*.key`, credentials, and secret files,
and stores path/language/symbol/line metadata in a separate code-index manifest.
The default include set is intentionally source-focused after real-repo testing:
Go, JS/TS, Python, Rust, JVM/Ruby/PHP/C/C++/C#/Swift/Kotlin, shell, Markdown,
YAML, JSON, TOML, XML, and Dockerfiles. Use `include: ["**/*"]` only when you
also maintain explicit excludes for generated and binary artifacts.

For interactive developer machines, the code indexer is resource-guarded: it
batches by chunk count and text bytes, throttles between batches, and aborts
before exceeding configured Go heap or process RSS guards. The CLI also requires
`--yes` for repositories larger than `large_repo_files` by default; use
`--low-resource` to halve batch size and increase throttling for background
indexing.

The repo-local codebase index registry lives at `.minnow/codebase-indexes.json`.
It is created or updated by `codeindex codebase` and maps stable keys to KBs:

```json
{
  "schema_version": "minnow.codebase_indexes/v1",
  "codebase_indexes": {
    "default": {
      "kb_id": "my-project",
      "scope_id": "opaque-scope-id",
      "root": ".",
      "description": "Default codebase index",
      "include_untracked": false
    }
  }
}
```

CLI and MCP code tools accept `index_key`; when `kb_id` is omitted, Minnow reads
this registry and resolves the key to the right KB. If no entry exists,
`default` maps to KB `default`, and other keys map to `code-<key>`.
For a remote Minnow server without the client repository, pass both `kb_id` and
`scope_id` to `minnow_code_search`. Coding agents should normally use the local
read-only `codeindex mcp` server, which resolves both values from the current
checkout and calls Minnow over HTTP.

### Codeindex client config

Run `codeindex setup --minnow-url http://127.0.0.1:8080` to write the client
config under the operating system's user config directory. Use
`CODEINDEX_CONFIG` or `--config` to select another path.

```yaml
minnow:
  url: http://127.0.0.1:8080
  token: ${MINNOW_TOKEN}

code_index:
  include: ["**/*.go", "**/*.ts", "**/*.md"]
  exclude: [".git/**", "node_modules/**", "vendor/**"]
  max_file_bytes: 1048576
  chunk_size: 1200
  chunk_overlap: 120
  request_batch_size: 32
  max_batch_bytes: 262144
  throttle: 100ms
  max_heap_bytes: 1073741824
  max_rss_bytes: 1073741824
  large_repo_files: 1000
  require_confirm: true
  poll_interval: 500ms
  operation_timeout: 10m
```

`minnow.url` is the only required connection setting. `token` is sent as a
Bearer token and should reference an environment variable rather than storing a
secret directly. `CODEINDEX_MINNOW_URL` and `CODEINDEX_TOKEN` override the file.
Codeindex sends prepared chunks to `POST /rag/ingest`, polls operation status,
and deletes stale IDs through the vector API.

### `graph` (optional, RAG graph extraction)

| Field         | Type   | Default                  | Notes |
| ------------- | ------ | ------------------------ | ----- |
| `enabled`     | bool   | `false`                  |       |
| `url`         | string | `http://localhost:11434` |       |
| `model`       | string | `llama3`                 |       |
| `parallelism` | int    | `2`                      |       |

### `mongo` (optional - omit for local/dev mode)

Mongo controls **persistence**, not whether the event pipeline exists. The event
store, inbox, and worker pools always run; Mongo makes their state durable
across restarts.

- **Omit the `mongo` block** - local/dev mode:
  - **Manifests**: blob-backed under `storage.blob.root`. Survive restarts as
    long as the blob root persists.
  - **Event store + inbox**: in-memory. `/rag/ingest` returns 202 and workers
    process events, but the event log resets on restart.
  - **Media store** (when `media.enabled: true`): in-memory metadata index;
    uploaded bytes still land in the blob store and survive restarts.
- **Include the `mongo` block** - manifests, events, inbox, and (when enabled)
  media metadata move to Mongo collections. `uri` is required.

If `media.enabled: false`, no media store is wired regardless of Mongo, and
`/rag/media/*` routes return 503.

| Field                    | Type   | Default          | Notes                                |
| ------------------------ | ------ | ---------------- | ------------------------------------ |
| `uri`                    | string | -                | Required. Use `${VAR}` for secrets.  |
| `database`               | string | `minnow`         |                                      |
| `collections.manifests`  | string | `manifests`      |                                      |
| `collections.events`     | string | `kb_events`      |                                      |
| `collections.inbox`      | string | `kb_event_inbox` |                                      |
| `collections.media`      | string | `media`          |                                      |

The four `collections.*` names must be distinct - duplicates are rejected at
startup to prevent silent cross-store corruption.

### `scheduler`

| Field             | Type         | Default | Notes                                   |
| ----------------- | ------------ | ------- | --------------------------------------- |
| `enabled`         | bool         | `true`  | Runs retention, reaper, shard-GC, and media maintenance jobs. Disable only for short-lived tests or specialized deployments. |
| `tick_interval`   | duration     | `1m`    | Scheduler wake-up cadence.              |
| `disabled_jobs`   | list[string] | `[]`    | Job IDs to skip registration for.       |

Keeping the scheduler enabled is important for long-running local deployments:
it bounds completed event and inbox retention, requeues interrupted work, and
deletes documents that remain outside every scope after the one-hour grace.

### `workers`

| Field                                    | Type     | Default | Notes                        |
| ---------------------------------------- | -------- | ------- | ---------------------------- |
| `defaults.max_attempts`                  | int      | `5`     |                              |
| `defaults.poll_interval`                 | duration | `500ms` |                              |
| `defaults.visibility_timeout`            | duration | `5m`    |                              |
| `document_upsert.concurrency`            | int      | `4`     |                              |
| `document_chunked.concurrency`           | int      | `4`     |                              |
| `document_publish.concurrency`           | int      | `2`     |                              |
| `media_upload.concurrency`               | int      | `2`     |                              |

Any pool may set `max_attempts`, `poll_interval`, or `visibility_timeout` to
override the `defaults` block for that one pool.

### `media`

When `enabled` is `false`, no media store is wired and every `/rag/media/*`
route returns `503 Service Unavailable`. The other defaults in this block only
take effect when `enabled` is `true`.

| Field                     | Type         | Default    | Notes                                    |
| ------------------------- | ------------ | ---------- | ---------------------------------------- |
| `enabled`                 | bool         | `false`    | Set `true` to accept media uploads.      |
| `max_bytes`               | int          | `10485760` | Max upload size (10 MiB). Must be > 0 when `enabled: true`. |
| `content_type_allowlist`  | list[string] | `[]`       | Empty = accept any MIME type.            |
| `pending_ttl`             | duration     | `24h`      | Unreferenced uploads past this are GC'd. |
| `tombstone_grace`         | duration     | `1h`       | Grace before hard-delete. Must be `<= pending_ttl`. |
| `upload_completion_ttl`   | duration     | `15m`      | Upload completion window.                |

Constraints when `media.enabled: true`:

- `max_bytes` must be > 0. A zero is replaced with the default (10 MiB) at load
  time; a negative value is rejected.
- `tombstone_grace` must be `<=` `pending_ttl`.

### `sharding`

Presence of a key means "explicit"; omit a key to accept the default.

| Field                               | Type    | Default    | Constraint                                             |
| ----------------------------------- | ------- | ---------- | ------------------------------------------------------ |
| `shard_trigger_bytes`               | int     | `67108864` | > 0 when set.                                          |
| `shard_trigger_vector_rows`         | int     | `150000`   | > 0 when set.                                          |
| `target_shard_bytes`                | int     | `33554432` | > 0 when set. What a fresh seal aims for.              |
| `max_shard_bytes`                   | int     | `67108864` | > 0 when set. Caps compaction. See above.              |
| `max_vector_rows_per_shard`         | int     | `75000`    | > 0 when set. Accepted but not enforced.               |
| `query_shard_fanout`                | int     | `4`        | > 0 and ≤ 64.                                          |
| `query_shard_fanout_adaptive_max`   | int     | `6`        | ≤ 64 and ≥ `query_shard_fanout`.                       |
| `query_shard_parallelism`           | int     | `4`        | > 0 when set.                                          |
| `query_shard_local_topk_multiplier` | int     | `2`        | > 0 and ≤ 16.                                          |
| `small_kb_max_shards`               | int     | `2`        | > 0 when set.                                          |
| `compaction_enabled`                | bool    | `true`     |                                                        |
| `compaction_min_shard_count`        | int     | `8`        | > 0 when set.                                          |
| `compaction_tombstone_ratio`        | float   | `0.20`     | `(0, 1]`.                                              |

### `ingest`

| Field              | Type | Default                | Notes                                                                |
| ------------------ | ---- | ---------------------- | -------------------------------------------------------------------- |
| `deferred_publish` | bool | decided by blob store  | Lets one client upload many batches and publish them with one commit. |

Publishing per batch rewrites the whole knowledge base every time, which makes a
large ingest quadratic. A deferred session uploads every batch first and
publishes once at the end.

Until the commit, the rows sit in one instance's local shard, so every request in
the session has to reach that instance. Omit the key and Minnow decides from
`storage.blob.kind`. Local storage is a single instance by construction, so it is
enabled. Shared storage may be serving several instances, so it stays off until
an operator declares one writer.

Set it to `true` only where a single Minnow process owns the data directory. In a
load-balanced deployment it splits a session across instances, and every batch
that misses the holder is refused with `409`.

A commit only publishes for the client that still holds the session, so a client
has to renew it by carrying its handle on every batch. It also has to wait for
each batch's operation to finish before committing. A commit that overtakes a
batch still in the pipeline publishes everything before it and leaves that batch
for the reaper.

When it is on, `/healthz` advertises the `ingest_sessions` capability, `POST
/rag/commit` publishes a session, and the scheduler's `session-reap` job picks up
sessions whose client never came back. When it is off, both a write asking to
defer and `/rag/commit` are rejected with `400`, and clients such as `codeindex`
fall back to publishing per batch. The reaper stays registered either way, so
turning the key off does not strand a session that was already open.

### `mcp`

MCP exposes Minnow for LLM agents. The schema default is disabled, 
but the repository's local developer configs
(`minnow.yaml`, `examples/minnow.min.yaml`, `examples/minnow.dev.openai.yaml`)
ship with MCP enabled and retrieval + indexing tools allowed. Destructive and
admin tools remain opt-in.

**Schema vs shipped defaults**: the table below documents schema defaults that
apply when `mcp.enabled` is `true` but a field is omitted. The shipped local
configs override `enabled` to `true` and `allow_indexing`/`allow_sync_indexing`
to `true` for developer ergonomics; production configs should set every field
explicitly.

The MCP `tools/list` reflects only the tools the gates allow: tools whose
required gate (e.g. `allow_destructive`, `allow_admin`) is off are not
registered. Agents therefore see exactly the surface they may call instead of
discovering tools that always error.

| Field                  | Type         | Default             | Notes |
| ---------------------- | ------------ | ------------------- | ----- |
| `enabled`              | bool         | `false`             | Enables MCP config and validation. Local examples set this to `true`. |
| `transports`           | list[string] | `[stdio, http]`     | Allowed values: `stdio`, `http`. Applies when enabled. |
| `http_path`            | string       | `/mcp`              | Streamable HTTP endpoint path. Must start with `/`. |
| `read_only`            | bool         | `false`             | Blocks indexing, destructive, and admin tools. |
| `allow_indexing`       | bool         | `false`             | Enables async document indexing tools. |
| `allow_sync_indexing`  | bool         | `false`             | Enables bounded wait indexing. Requires `allow_indexing`. |
| `allow_destructive`    | bool         | `false`             | Enables delete/tombstone tools. Keep off unless explicitly needed. |
| `allow_admin`          | bool         | `false`             | Enables maintenance tools such as cache sweep and compaction. |
| `default_sync_timeout` | duration     | `30s`               | Default wait for sync indexing. |
| `max_sync_timeout`     | duration     | `2m`                | Upper bound for caller-requested sync indexing waits. |
| `http_json_response`   | bool         | `false`             | Prefer JSON responses over SSE where the SDK supports it. |
| `http_stateless`       | bool         | `true`              | Runs HTTP MCP requests without retained sessions. Set `false` only when session state is required. |
| `http_session_timeout` | duration     | `30m`               | Closes inactive stateful HTTP sessions so crashed/disconnected clients cannot accumulate memory and goroutines. |
| `http_max_sessions`    | int          | `128`               | Hard active-session admission limit when `http_stateless: false`. |

Stdio mode is launched with:

```bash
go run . mcp stdio
```

In stdio mode, Minnow writes logs to stderr so stdout is reserved for MCP
messages.

Example local agent config:

```yaml
mcp:
  enabled: true
  transports: [stdio, http]
  http_path: /mcp
  allow_indexing: true
  allow_sync_indexing: true
  allow_admin: true
  allow_destructive: false
  http_session_timeout: 30m
  http_max_sessions: 128
```

Destructive tools (`minnow_delete_knowledge_base`, `minnow_delete_media`,
`minnow_clear_cache`) require `allow_destructive: true`; admin maintenance tools
require `allow_admin: true`.

For an agent running on the Minnow host, stdio launches the server directly:

```json
{
  "mcpServers": {
    "minnow": {
      "command": "minnow",
      "args": ["mcp", "stdio"],
      "env": {
        "MINNOW_CONFIG": "/path/to/minnow.yaml"
      }
    }
  }
}
```

Agents on another computer should use the streamable HTTP endpoint instead.
Current Codex CLI, Claude Code, and OpenCode registration examples are in
[getting-started.md](getting-started.md#mcp-endpoints). Keep credentials in
environment variables rather than client config. Bearer-token examples require
an authenticating HTTPS reverse proxy or API gateway until Minnow has native
authentication.

## Secret policy

- Do not commit inline secret values to the repo.
- Fields that carry credentials (notably `mongo.uri`) may exist in YAML, but
  production configs must reference them via `${VAR}`.
