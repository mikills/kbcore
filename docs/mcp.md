# MCP

Minnow has two MCP servers. They solve different problems.

| Server | Use it for | Connection |
| --- | --- | --- |
| `codeindex mcp` | Read-only search of the current repository and Git branch | A local stdio process that calls Minnow over HTTP |
| Minnow MCP | Querying a known knowledge base, ingesting documents, checking operations, and permitted maintenance | Local stdio or hosted streamable HTTP |

Use `codeindex mcp` for coding agents. It reads the repository and branch on
each call, so the model does not need a knowledge base ID or scope ID. It does
not call Minnow MCP. It calls the hosted Minnow HTTP API directly.

## Search a codebase

Install and configure `codeindex` once:

```bash
go install github.com/mikills/minnow/codeindex@latest
export MINNOW_TOKEN='your-token'
codeindex setup \
  --minnow-url https://minnow.example.com \
  --token-env MINNOW_TOKEN
```

Index the repository and install hooks:

```bash
cd /path/to/repository
codeindex codebase
codeindex hooks install
codeindex status
```

The hooks refresh the index after commits, checkouts, merges, and rebases. An
interrupted run records acknowledged batches and continues from them when the
command runs again.

Register the read-only MCP server with the same repository root. A client starts
the server as its own process, which does not inherit the environment of the
shell you register from, so a config using `${MINNOW_TOKEN}` needs that variable
forwarded explicitly:

```bash
# Codex
codex mcp add codeindex -- codeindex mcp --root /path/to/repository

# Claude Code
claude mcp add --scope local codeindex -- \
  codeindex mcp --root /path/to/repository
```

Then name the variable in the server entry. Codex forwards by name through
`env_vars` in `~/.codex/config.toml`, which keeps the token out of the file:

```toml
[mcp_servers.codeindex]
command = "codeindex"
args = ["mcp", "--root", "/path/to/repository"]
env_vars = ["MINNOW_TOKEN"]
```

Claude Code takes `KEY=VALUE` pairs, so pass the variable through the shell
rather than pasting the token:

```bash
claude mcp add --scope local -e MINNOW_TOKEN="$MINNOW_TOKEN" codeindex -- \
  codeindex mcp --root /path/to/repository
```

For OpenCode, add this to `opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "codeindex": {
      "type": "local",
      "command": ["codeindex", "mcp", "--root", "/path/to/repository"],
      "environment": { "MINNOW_TOKEN": "{env:MINNOW_TOKEN}" },
      "enabled": true
    }
  }
}
```

Codex and OpenCode forward by name, so export `MINNOW_TOKEN` in the environment
the client itself starts from. Claude Code's `-e` stores the value at add time,
so export it in the shell you run `claude mcp add` from and re-run that command
after rotating the token. A name forwarded but never exported reaches the server
set and empty, which is reported the same way as absent.

Restart the client after registration. The server provides two tools:

- `codeindex_search` searches the current branch. It accepts `query`, `k`,
  `path`, and `language`.
- `codeindex_status` reports whether the current branch has a usable index or
  a recoverable run.

Both tools are read-only. Initial indexing and refreshes run through the CLI or
the installed Git hooks.

If the index used `--kb` or `--index-key`, pass the same flag to
`codeindex mcp`. Register a separate MCP server name for each checkout that an
agent needs to search.

A server that cannot read its config still completes the handshake and reports
the reason from the first tool call, because clients do not show a server's
standard error. If `codeindex_search` answers with a missing environment
variable, the token is not reaching the server and the registration needs the
forwarding above.

## Connect to hosted Minnow

Use the hosted Minnow MCP when the agent already knows which knowledge base to
query or needs the general ingest and operation tools. A hosted Minnow process
cannot read files from your workstation. Use `codeindex mcp` for local source
code.

The Fly deployment puts Caddy in front of `/mcp` and requires its bearer token.
Keep that token in an environment variable.

Codex reads the token at runtime:

```bash
export MINNOW_TOKEN='your-token'
codex mcp add minnow \
  --url https://minnow.example.com/mcp \
  --bearer-token-env-var MINNOW_TOKEN
```

Claude Code expands environment variables in `.mcp.json`. Add this entry at the
repository root:

```json
{
  "mcpServers": {
    "minnow": {
      "type": "http",
      "url": "https://minnow.example.com/mcp",
      "headers": {
        "Authorization": "Bearer ${MINNOW_TOKEN}"
      }
    }
  }
}
```

Set `MINNOW_TOKEN` before starting Claude Code.

For OpenCode, add a remote server to `opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "minnow": {
      "type": "remote",
      "url": "https://minnow.example.com/mcp",
      "oauth": false,
      "headers": {
        "Authorization": "Bearer {env:MINNOW_TOKEN}"
      }
    }
  }
}
```

Check the connection with `codex mcp get minnow --json`,
`claude mcp get minnow`, or `opencode mcp list`.

## Tool permissions

Minnow only advertises tools allowed by its `mcp` configuration.

- Query and status tools are available whenever MCP is enabled. These include
  `minnow_query`, `minnow_query_vectors`, `minnow_operation_status`, and the
  media read tools.
- `allow_indexing` adds asynchronous document ingest and vector upsert.
  `allow_sync_indexing` also adds bounded synchronous ingest.
- `allow_destructive` adds knowledge base and media deletion.
- `allow_admin` adds cache, compaction, and hook maintenance tools.
- `read_only` removes indexing, destructive tools, and hook changes. Cache
  sweep and compaction still require `allow_admin`.

Keep destructive and admin tools disabled on a shared hosted service unless an
agent has a specific operational job. See the [`mcp` configuration
reference](configuration.md#mcp) for every gate and default.

## Run Minnow over stdio

An agent on the same machine can launch Minnow directly:

```bash
MINNOW_CONFIG=/path/to/minnow.yaml minnow mcp stdio
```

The config must enable the `stdio` transport. Minnow writes logs to stderr so
stdout remains reserved for MCP messages.

## Troubleshooting

- A `401` from hosted `/mcp` means the proxy rejected the bearer token.
- An empty or shorter tool list usually means an MCP permission gate is off.
- `codeindex_search` reporting an unindexed branch means the CLI or hook has not
  finished that branch. Run `codeindex status`, then `codeindex codebase`.
- Changing branches does not require MCP registration again. `codeindex mcp`
  resolves the current branch for each call.
