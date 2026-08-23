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

For Git repositories, one `kb_id` holds the repository vectors and each branch
has an opaque document scope. Switching branches reuses unchanged chunks while
keeping searches branch-aware. Git worktrees share the same repository identity.
In a non-Git directory, codeindex derives identity from the absolute directory.

The first run writes branch-specific state under `.minnow/codeindex/` and adds
`/.minnow/` to Git's repository-local exclude file (`.git/info/exclude`). If an
older setup already committed that directory, untrack it first with
`git rm -r --cached .minnow`. A refresh
hashes the current files, uploads missing chunks, publishes the branch scope,
and then atomically saves the new state. A journal records acknowledged batches
so an interrupted run resumes without uploading them again.
Refreshes do not delete unscoped vectors inline; this avoids racing another
clone that is publishing a scope. `codeindex remove` deletes the current branch
scope, schedules unused chunks for delayed cleanup, and deletes its local state.
Normal refreshes use the same delayed cleanup. Minnow reclaims candidates after
one hour when its scheduler is enabled.
`codeindex status` reads only this repository-local state, so it remains usable
without a valid connection config or a running Minnow service.

Repositories upgraded from v0.4 reuse legacy data found by the original clone
or its worktrees. A clean clone cannot discover that branch-specific legacy KB;
use the same explicit `--kb` when clones must share during migration.

Useful overrides:

```bash
codeindex codebase --include-untracked
codeindex codebase --low-resource --yes
codeindex codebase --kb explicit-kb --index-key explicit-key
```

Inside Git, explicit `--index-key` and `--kb` values are stable prefixes. The
repository and indexed subdirectory select the KB; the branch selects its scope.
Hooks resolve the invoking worktree and repository-local settings each time they
run. Hook failures do not block Git operations; diagnostics are appended to
Git's `minnow-codeindex-hook.log` path.
