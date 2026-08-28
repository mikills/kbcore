# Getting Started

## Install script

The fastest path needs only Docker and, for keyless embeddings,
[Ollama](https://ollama.com):

```bash
ollama pull all-minilm
curl -fsSL https://raw.githubusercontent.com/mikills/minnow/main/install.sh | sh
```

The script downloads a prebuilt `codeindex` binary, verifies its checksum,
starts Minnow through `docker compose`, waits for `/healthz`, and writes the
codeindex connection config. It reads these variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `CODEINDEX_VERSION` | latest `codeindex/v*` | release tag to install |
| `CODEINDEX_INSTALL_DIR` | `~/.local/bin` | where the binary goes |
| `MINNOW_EMBEDDER` | `ollama` | `ollama` or `openai` |
| `MINNOW_PORT` | `8080` | host port for Minnow |
| `SKIP_SERVER` | unset | set to `1` to install the CLI only |

The two embedders produce different vector widths, so an index built with one
cannot be queried with the other. Delete the `minnow-data` volume if you switch.

## Manual install

The script is a convenience. These are the same steps by hand.

### 1. Start Minnow

The published image carries two configs. `MINNOW_EMBEDDER` selects between them:

```bash
curl -fsSLO https://raw.githubusercontent.com/mikills/minnow/main/compose.yaml
docker compose up -d                                   # ollama, no API key
MINNOW_EMBEDDER=openai OPENAI_API_KEY=sk-... docker compose up -d
curl -s http://127.0.0.1:8080/healthz
```

Without compose, name the config yourself:

```bash
docker run -d --name minnow \
  -e MINNOW_CONFIG=/etc/minnow/minnow.ollama.yaml \
  --add-host host.docker.internal:host-gateway \
  -p 127.0.0.1:8080:8080 -v minnow-data:/var/lib/minnow \
  ghcr.io/mikills/minnow:latest
```

The image ships `/etc/minnow/minnow.ollama.yaml` and
`/etc/minnow/minnow.openai.yaml`. `/etc/minnow/minnow.yaml` stays the OpenAI one
and is the default when `MINNOW_CONFIG` is unset. The ollama config points at
`host.docker.internal:11434`, so Ollama runs on the host, not in the container.
On Linux that hostname needs `--add-host host.docker.internal:host-gateway`,
which compose already sets.

Compose passes `OPENAI_API_KEY` through only when your shell exports it. Leave
it unset with `MINNOW_EMBEDDER=openai` and Minnow refuses to start with
`unresolved env vars: OPENAI_API_KEY` instead of failing later on a 401.

To mount your own config instead of using a baked one:

```bash
docker run -d -v "$PWD/minnow.yaml:/etc/minnow/custom.yaml:ro" \
  -e MINNOW_CONFIG=/etc/minnow/custom.yaml ... ghcr.io/mikills/minnow:latest
```

### 2. Install codeindex

Download the release binary and check it before running it. Pick your platform
from `darwin_arm64`, `darwin_amd64`, `linux_amd64`, `linux_arm64`:

```bash
version=v0.8.1
platform=darwin_arm64
base="https://github.com/mikills/minnow/releases/download/codeindex/$version"
curl -fsSLO "$base/codeindex_${version}_${platform}.tar.gz"
curl -fsSLO "$base/checksums.txt"
shasum -a 256 -c checksums.txt --ignore-missing   # sha256sum -c on Linux
tar -xzf "codeindex_${version}_${platform}.tar.gz"
install -m 755 codeindex ~/.local/bin/codeindex
```

Or build it, which needs a Go toolchain but no release assets:

```bash
go install github.com/mikills/minnow/codeindex@latest
```

### 3. Point codeindex at Minnow

```bash
codeindex setup --minnow-url http://127.0.0.1:8080
```

Add `--force` to overwrite an existing config, and `--token-env MINNOW_TOKEN`
when the server needs a bearer token. That writes a `${MINNOW_TOKEN}` reference
rather than the secret itself.

Then index and register the MCP server as described under
[Index a codebase](#index-a-codebase) and [MCP endpoints](#mcp-endpoints).

## Prerequisites

These apply to running Minnow from a checkout. The published image needs none of
them.

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
hosted URL from the normal codeindex configuration, but an agent starts it as
its own process, so a `${MINNOW_TOKEN}` reference in that config resolves
against the server entry's environment and not your shell. Forward the variable
there. [MCP](mcp.md) shows the syntax for each client.

For the full setup, tool permissions, and troubleshooting steps, see
[MCP](mcp.md). The configuration fields are documented in the [`mcp` section
of the configuration reference](configuration.md#mcp).

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
