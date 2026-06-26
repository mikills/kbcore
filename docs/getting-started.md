# Getting Started

## Prerequisites

- Go toolchain matching the module's version.
- CGO-capable environment for the DuckDB driver.
- DuckDB extensions in `extensions/` (shipped with the repo; no network access needed by default).

## Configuration

minnow is configured from a single YAML file, discovered at:

1. `$MINNOW_CONFIG` if set, or
2. `./minnow.yaml` in the process working directory, or
3. the per-user config path (`~/Library/Application Support/minnow/minnow.yaml`
   on macOS, `~/.config/minnow/minnow.yaml` on Linux).

See [configuration.md](configuration.md) for the full schema and
[`examples/minnow.min.yaml`](../examples/minnow.min.yaml) /
[`examples/minnow.yaml`](../examples/minnow.yaml) for ready-to-copy starting
points.

Only two environment variables are read directly by the binary; every other
deployment knob lives in the YAML.

| Env var             | Purpose                                                  |
| ------------------- | -------------------------------------------------------- |
| `MINNOW_CONFIG`     | Path to the YAML config. Overrides default discovery.     |
| `MINNOW_LOG_FORMAT` | Logger format (`text` / `json`). Read before the config. |

Secret values (Mongo URI, tokens) are referenced from YAML via `${VAR}`
interpolation and set as regular environment variables.

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

The first run creates `.minnow/codebase-indexes.json` in the repository:

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

MCP clients can then use the stable `index_key` instead of remembering the
backing `kb_id`. This also lets an agent create multiple codebase indexes, such
as `default`, `backend`, or `docs`, each with its own description and KB.

Optional Git hooks can keep the index warm after commits, checkouts, merges, and
rebases:

```bash
minnow index hooks install
minnow index hooks status
```

Code index refreshes are state-based, not commit-based. On each refresh Minnow
scans the current tree, uses Git tracked files by default (`git ls-files`), hashes
eligible files, and compares them with the previous code-index manifest. Unchanged
files are skipped, changed files are re-chunked and re-embedded, new files are
added, and chunks for deleted files are hard-deleted from the backing KB. Pass the
include-untracked option when creating or refreshing an index if untracked files
should be indexed too. Without installed hooks, run `minnow index refresh`
manually after code changes.

The default bind address is `127.0.0.1:8080` (override `http.address` in the YAML).

The minimal config enables MCP for local coding-agent workflows:

- Streamable HTTP MCP endpoint: `http://127.0.0.1:8080/mcp`
- Stdio MCP command: `go run . mcp stdio`

For editors that register MCP servers as a command, use the stdio mode. If your
config is not at `./minnow.yaml`, pass it through `MINNOW_CONFIG` in the editor's
MCP server environment.

Example MCP registration shape:

```json
{
  "mcpServers": {
    "minnow": {
      "command": "go",
      "args": ["run", ".", "mcp", "stdio"],
      "env": {
        "MINNOW_CONFIG": "./minnow.yaml"
      }
    }
  }
}
```

If you install Minnow as a binary, replace the command and args with the binary:

```json
{
  "mcpServers": {
    "minnow": {
      "command": "minnow",
      "args": ["mcp", "stdio"],
      "env": {
        "OPENAI_API_KEY": "sk-..."
      }
    }
  }
}
```

If you keep the config outside the default user config path, add `MINNOW_CONFIG`
to the `env` block as well. GUI editors may not inherit shell environment
variables on macOS, so pass `OPENAI_API_KEY` explicitly unless your editor has a
known environment loading mechanism.

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
