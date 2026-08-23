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

Codeindex is an HTTP client of Minnow; it does not load `minnow.yaml` or embed
content itself. Start Minnow's HTTP service and configure the connection once:

```bash
go install github.com/mikills/minnow/codeindex@latest
minnow
codeindex setup --minnow-url http://127.0.0.1:8080
```

Then index from inside a repository or ordinary directory:

```bash
codeindex codebase
codeindex status
```

Optional overrides:

| Flag | Default | Purpose |
|---|---|---|
| `--root` | `.` | Repository root to index. |
| `--index-key` | repository branch or directory identity | Stable agent-facing key. |
| `--kb` | repository derived | Backing knowledge base id. |
| `--description` | empty | Human label stored in the registry. |
| `--include-untracked` | off | Index files not tracked by Git. |

For Git repositories, branches share repository vectors and receive separate
opaque scopes. Unchanged chunks are reused and searches remain branch-aware.
Without Git, the absolute directory provides stable identity. The command output
and `codeindex status` show the selected `kb_id` for HTTP or MCP searches.

Optionally install Git hooks to refresh on commit / checkout / merge / rebase:

```bash
codeindex hooks install
codeindex hooks status
```

Refreshes compare tracked files (`git ls-files`) with branch state under
`.minnow/codeindex/`, upload only missing chunks, and replace the branch scope.
Acknowledged batches survive interruption in the local journal. Codeindex adds
`/.minnow/` to `.git/info/exclude` so generated state is not staged. Without
hooks, run `codeindex refresh` after code changes. Run `codeindex remove` on a
branch before deleting it to remove its remote scope and local state.

## MCP endpoints

The minimal config enables MCP for local coding-agent workflows:

- Streamable HTTP: `http://127.0.0.1:8080/mcp` (override `http.address` in the YAML)
- Stdio (for editor registration): `go run . mcp stdio`

For branch-aware code search, register the local read-only codeindex MCP. It
resolves the current Git branch and calls hosted Minnow over HTTP:

```bash
# Codex CLI
codex mcp add codeindex -- codeindex mcp --root /path/to/repository

# Claude Code
claude mcp add --scope project codeindex -- \
  codeindex mcp --root /path/to/repository
```

If indexing used `--kb` or `--index-key`, pass the same flags to `codeindex mcp`.

For direct access to non-code Minnow tools, register the hosted `/mcp` endpoint
separately. Codeindex exposes only `codeindex_search` and `codeindex_status`.

For OpenCode code search, add this to `opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "codeindex": {
      "type": "local",
      "command": ["codeindex", "mcp", "--root", "/path/to/repository"],
      "enabled": true
    }
  }
}
```

Check registration with `codex mcp get codeindex --json`,
`claude mcp get codeindex`, or `opencode mcp list`. The local process reads the
hosted URL and token from the normal codeindex configuration.

For the full MCP tool and gate reference, see the `mcp` section of
[configuration.md](configuration.md).

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
