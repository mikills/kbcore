# codeindex

`codeindex` scans and chunks a local codebase, then sends prepared chunks to a
running Minnow HTTP service. Minnow owns embeddings, storage, and search;
codeindex owns Git, branch identity, incremental state, and hooks.

## Install

```bash
go install github.com/mikills/minnow/codeindex@latest
```

From a Minnow checkout, use `go install ./codeindex`.

## Configure

Start Minnow, then create codeindex's user config:

```bash
minnow
codeindex setup --minnow-url http://127.0.0.1:8080
```

The config is stored under the operating system's user config directory, for
example `~/.config/codeindex/config.yaml` on Linux. Override it with
`CODEINDEX_CONFIG` or `--config`.

For a Minnow endpoint protected by a bearer token, keep the token in an
environment variable:

```bash
codeindex setup --minnow-url https://minnow.example.com --token-env MINNOW_TOKEN
export MINNOW_TOKEN=...
```

Minimal configuration:

```yaml
minnow:
  url: http://127.0.0.1:8080
```

## Index

```bash
cd /path/to/repository
codeindex codebase
codeindex status
codeindex hooks install
```

For Git repositories, the repository identity and current branch derive the
default index key and `kb_id`. Switching branches selects a separate index. In a
non-Git directory, codeindex derives identity from the absolute directory.

The first run writes branch-specific state under `.minnow/codeindex/`. A refresh
hashes the current files, uploads changed chunks, waits for Minnow to publish
them, deletes stale chunk IDs, and then atomically saves the new state.
`codeindex status` reads only this repository-local state, so it remains usable
without a valid connection config or a running Minnow service.

Useful overrides:

```bash
codeindex codebase --include-untracked
codeindex codebase --low-resource --yes
codeindex codebase --kb explicit-kb --index-key explicit-key
```

Inside Git, explicit `--index-key` and `--kb` values are stable prefixes: the
current branch and indexed subdirectory are appended so switching branches
cannot reuse incompatible local state or mix branch data in one KB. Hooks keep
the prefix in their command and resolve the current branch each time they run.
