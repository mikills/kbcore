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

These two env vars are read before the config file:

| Env var             | Purpose                                                                    |
| ------------------- | -------------------------------------------------------------------------- |
| `MINNOW_CONFIG`     | Path to the YAML config file. Overrides default discovery.                 |
| `MINNOW_LOG_FORMAT` | Logger format (`text` / `json`). Read before the YAML so parse errors log.|

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
| `kind` | enum   | `local`             | `local` \| `s3`.                               |
| `root` | path   | `./.temp/fixtures`  | Relative to YAML file. Used when `kind: local`.|

When `kind: s3`, provide an `s3` block. Works with AWS S3, MinIO, Cloudflare R2,
and any S3-compatible store via `endpoint`.

| Field                    | Type   | Default     | Notes                                                              |
| ------------------------ | ------ | ----------- | ------------------------------------------------------------------ |
| `s3.bucket`              | string | —           | Required.                                                          |
| `s3.region`              | string | `us-east-1` |                                                                    |
| `s3.prefix`              | string | empty       | Key prefix for all objects.                                        |
| `s3.lease_prefix`        | string | empty       | Namespaces the S3 write lease across deployments sharing a bucket. |
| `s3.endpoint`            | string | empty       | HTTP(S) URL for MinIO / R2 / other S3-compatible stores.           |
| `s3.access_key_id`       | string | empty       | Use `${VAR}`. Both keys must be set together, or both empty.       |
| `s3.secret_access_key`   | string | empty       | Use `${VAR}`. When both are empty, the AWS credential chain is used.|

### `storage.cache`

| Field            | Type     | Default         | Notes                        |
| ---------------- | -------- | --------------- | ---------------------------- |
| `dir`            | path     | `./.temp/cache` | Relative to YAML file.       |
| `max_bytes`      | int      | `0`             | `0` = unbounded.             |
| `entry_ttl`      | duration | `0s`            | `0` = no TTL.                |
| `warm_shards`    | int      | `0`             | Pre-download the N most-recently-sealed shards per KB into the cache at startup, in the background. `0` = off. |
| `evict_interval` | duration | `30s`           |                              |

Queries run against local shard files, so a shard not yet in `dir` is fetched
from the blob store on first touch (a cold `GET` in S3 mode). `warm_shards` pays
that cost up front at startup instead of on the first query per shard.

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
sharding:
  query_shard_fanout: 2
  query_shard_fanout_adaptive_max: 2
  query_shard_parallelism: 1
```

Set `GOMEMLIMIT` in the service environment as an additional Go-heap guard.
It does not include DuckDB native memory, so retain an OS/cgroup memory limit.

### `format`

| Field                  | Type   | Default         | Notes                              |
| ---------------------- | ------ | --------------- | ---------------------------------- |
| `kind`                 | string | `duckdb`        | Only `duckdb` is supported today.  |
| `duckdb.memory_limit`  | string | `128MB`         | Passed to DuckDB verbatim.         |
| `duckdb.extension_dir` | path   | `./extensions`  | Relative to YAML file.             |
| `duckdb.offline`       | bool   | `false`         | If true, disables extension fetch. |

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
| `max_heap_bytes`      | int      | `1073741824`                                                             | Abort indexing if Go heap/system memory exceeds this guard. |
| `max_rss_bytes`       | int      | `1073741824`                                                             | Abort indexing if process resident memory exceeds this guard. |
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
it bounds completed event and inbox retention and requeues interrupted work.

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
| `target_shard_bytes`                | int     | `33554432` | > 0 when set.                                          |
| `max_vector_rows_per_shard`         | int     | `75000`    | > 0 when set.                                          |
| `query_shard_fanout`                | int     | `4`        | > 0 and ≤ 64.                                          |
| `query_shard_fanout_adaptive_max`   | int     | `6`        | ≤ 64 and ≥ `query_shard_fanout`.                       |
| `query_shard_parallelism`           | int     | `4`        | > 0 when set.                                          |
| `query_shard_local_topk_multiplier` | int     | `2`        | > 0 and ≤ 16.                                          |
| `small_kb_max_shards`               | int     | `2`        | > 0 when set.                                          |
| `compaction_enabled`                | bool    | `true`     |                                                        |
| `compaction_min_shard_count`        | int     | `8`        | > 0 when set.                                          |
| `compaction_tombstone_ratio`        | float   | `0.20`     | `(0, 1]`.                                              |

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
