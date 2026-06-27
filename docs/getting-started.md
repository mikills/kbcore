# Getting Started

## Prerequisites

- Go toolchain matching the module's version.
- CGO-capable environment for the DuckDB driver.
- DuckDB extensions in `extensions/` (shipped with the repo; no network access needed by default).

## Configuration

minnow reads a single YAML file (discovered via `$MINNOW_CONFIG`, `./minnow.yaml`,
or the per-user config path). Copy
[`examples/minnow.min.yaml`](../examples/minnow.min.yaml) to start. The full
schema, discovery rules, env vars, and secret handling live in
[configuration.md](configuration.md).

## Run locally

```bash
cp examples/minnow.min.yaml minnow.yaml
go run .
```

## Install for MCP

Install the binary globally:

```bash
go install github.com/mikills/minnow@latest
$(go env GOPATH)/bin/minnow setup
```

`go install` writes the binary to `$(go env GOBIN)`, or to
`$(go env GOPATH)/bin` when `GOBIN` is unset. `minnow setup` is an interactive
terminal setup that checks whether that directory is on `PATH` and can append
the right export line to your shell profile. If `minnow` is already on `PATH`,
run `minnow setup` directly. After restarting the shell, verify with:

```bash
minnow --version
```

Create an OpenAI-backed developer config in the per-user config path:

```bash
minnow config init dev-openai
export OPENAI_API_KEY=sk-...
minnow config validate
```

That config enables both HTTP and stdio MCP, uses `text-embedding-3-small`, and
stores local blobs/cache under the same user config directory. The first ingest
fixes each KB's embedding dimension to the model output dimension.

## Index a codebase

Index the current repository for code-aware retrieval. From inside the repo, no
flags are required:

```bash
minnow index codebase     # indexes . , index-key "default", kb_id auto-derived
minnow index status
```

Optional overrides:

| Flag | Default | Purpose |
|---|---|---|
| `--root` | `.` | Repository root to index. |
| `--index-key` | `default` | Stable agent-facing key for this index. |
| `--kb` | derived from index-key | Backing knowledge base id. |
| `--description` | empty | Human label stored in the registry. |
| `--include-untracked` | off | Index files not tracked by Git. |

The first run writes `.minnow/codebase-indexes.json`, a repo-local registry
mapping each `index_key` to its backing `kb_id`, so MCP clients pass only
`index_key: "default"`. One repo can hold several indexes (`default`, `backend`,
`docs`), each with its own KB.

Optionally install Git hooks to refresh on commit / checkout / merge / rebase:

```bash
minnow index hooks install
minnow index hooks status
```

Refreshes are state-based: Minnow hashes tracked files (`git ls-files`) against
the previous manifest and only re-embeds what changed (deleted files' chunks are
removed). Without hooks, run `minnow index refresh` after code changes.

## MCP endpoints

The minimal config enables MCP for local coding-agent workflows:

- Streamable HTTP: `http://127.0.0.1:8080/mcp` (override `http.address` in the YAML)
- Stdio (for editor registration): `go run . mcp stdio`

For editor registration JSON and the full MCP tool/gate reference, see the `mcp`
section of [configuration.md](configuration.md).

## Validate a config

Before rolling out a config, run the built-in validator. It loads, interpolates
`${VAR}` references, applies defaults, and dry-runs the runtime builder - no
Mongo connection, no port bind.

```bash
go run . config validate ./minnow.yaml
# => config OK
```

The validator exits 1 on any error; wire it into CI to gate merges.

## Send a first request

Health check:

```bash
curl -s http://127.0.0.1:8080/healthz
# => {"status":"ok"}
```

`POST /rag/ingest` and `POST /rag/media/upload` are asynchronous and return an
operation handle. Poll `GET /rag/operations/:id` for terminal status.

## Optional: MongoDB for durable event state

Without a `mongo` block, minnow runs in local/dev mode: manifests are blob-backed
(and survive restarts as long as the blob root does), while the event store and
inbox are in-memory (and reset on restart). To make event and inbox state durable,
add a `mongo` block:

```yaml
mongo:
  uri: ${MINNOW_MONGO_URI}
  database: minnow
  collections:
    manifests: manifests
    events: kb_events
    inbox: kb_event_inbox
    media: media
```

Media wiring follows `media.enabled` independently of Mongo: with media
disabled, `/rag/media/*` routes return `503`.

See [configuration.md](configuration.md) for the full set of fields.
