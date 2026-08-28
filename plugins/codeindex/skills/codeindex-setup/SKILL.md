---
name: codeindex-setup
description: Install the codeindex CLI and point it at a Minnow service. Use when the user wants to install, set up, or reconfigure codeindex, move it to a different Minnow URL, or when codeindex reports a missing config or missing environment variables.
---

# codeindex setup

`codeindex` chunks a local repository and sends the chunks to a running Minnow
service. Minnow owns embeddings, storage, and search. This skill installs the
CLI and writes its connection config.

## 1. Install

Requires Go. Install the published module, not a local checkout:

```bash
go install github.com/mikills/minnow/codeindex@latest
codeindex --version
```

`go install` puts the binary in `$(go env GOBIN)`, or `$(go env GOPATH)/bin`
when `GOBIN` is unset. If `codeindex` is not found afterwards, that directory
is not on `PATH`.

Versions before `v0.3.1` hang on a repository containing a file with a long
line followed by short ones, growing memory until the process is killed. If
`codeindex --version` reports anything earlier, reinstall before indexing.

## 2. Decide the Minnow endpoint

Ask the user which Minnow they are indexing into if it is not already clear:

- A local service, normally `http://127.0.0.1:8080`, needs no token.
- A deployed service normally sits behind a proxy that checks a bearer token.

## 3. Write the config

For a local Minnow:

```bash
codeindex setup --minnow-url http://127.0.0.1:8080
```

For a token-protected Minnow, name the environment variable holding the token
rather than passing the token itself. The config stores the reference
`${MINNOW_TOKEN}`, so the secret never lands in a file:

```bash
codeindex setup --minnow-url https://minnow.example.com --token-env MINNOW_TOKEN
```

The token must then be exported in the shell that runs `codeindex`. Persist it
in the user's shell profile or a secret store. Never echo a token to stdout and
never write one into the config file.

`setup` refuses to overwrite an existing config. Pass `--force` to replace one,
which matters in scripts using `set -e`.

`setup` takes only `--minnow-url`, `--token-env`, `--config`, and `--force`.
The indexing flags belong to other subcommands and are rejected here.

### Where the config lands

`codeindex` uses the operating system's user config directory:

| OS | Path |
| --- | --- |
| macOS | `~/Library/Application Support/codeindex/config.yaml` |
| Linux | `${XDG_CONFIG_HOME:-~/.config}/codeindex/config.yaml` |
| Windows | `%AppData%\codeindex\config.yaml` |

Do not assume `~/.config` on macOS. Override the location with `--config` or
`CODEINDEX_CONFIG`; override the default URL with `CODEINDEX_MINNOW_URL`.

## 4. Verify

```bash
codeindex status
```

`status` reads repository-local state only, so it answers even when the service
is unreachable. To prove the connection works, run a real index from a small
repository and confirm it reports chunks uploaded. See the `codeindex-index` skill.

Note that `codeindex`'s connection check calls `/healthz`, which deployments
commonly leave unauthenticated. A wrong token can therefore pass the check and
fail later during upload. See the `codeindex-troubleshoot` skill.

## Searching the index

Register codeindex's read-only local MCP so the agent automatically uses the
current branch scope:

```bash
codex mcp add codeindex -- codeindex mcp --root /path/to/repository
claude mcp add --scope project codeindex -- \
  codeindex mcp --root /path/to/repository
```

The client starts the server as its own process, which does not inherit the
shell it was registered from. A config referencing `${MINNOW_TOKEN}` therefore
needs that variable named in the server entry. Codex forwards by name, which
keeps the token out of the file:

```toml
[mcp_servers.codeindex]
command = "codeindex"
args = ["mcp", "--root", "/path/to/repository"]
env_vars = ["MINNOW_TOKEN"]
```

Claude Code takes `KEY=VALUE`, so pass it through the shell rather than pasting
the token, and re-run after rotating it:

```bash
claude mcp add --scope project -e MINNOW_TOKEN="$MINNOW_TOKEN" codeindex -- \
  codeindex mcp --root /path/to/repository
```

OpenCode takes an `environment` map in `opencode.jsonc`, using
`"MINNOW_TOKEN": "{env:MINNOW_TOKEN}"`.

Without forwarding, every tool call answers with the variable the server could
not read. A name that is forwarded but never exported arrives set and empty and
is reported the same way.

If indexing used `--kb` or `--index-key`, pass the same flags to `codeindex mcp`.

The MCP process reads the hosted Minnow URL from the normal codeindex
configuration and the token from its own environment. It exposes search and
status only; `codeindex hooks install` keeps the index current after Git
changes.

Once registered, see the `codeindex-search` skill for querying the index.
