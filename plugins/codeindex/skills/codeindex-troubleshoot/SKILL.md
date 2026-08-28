---
name: codeindex-troubleshoot
description: Diagnose a codeindex run that failed, hung, or was killed. Use when codeindex produces no output, is killed by the system, reports a missing token or a refresh already running, or when indexing fails partway through upload.
---

# Diagnosing a codeindex run

Start by recording the version, since several known failures are version
specific:

```bash
codeindex --version
```

## The process was killed

`zsh: killed codeindex codebase` is the kernel reclaiming memory, not a crash
in Minnow. The upload never reached the service.

On versions before `v0.3.1`, a file with a long line followed by short ones
made the chunker repeat one chunk forever, growing memory until it was killed.
Markdown with long YAML front matter is the usual trigger. Upgrade:

```bash
go install github.com/mikills/minnow/codeindex@latest
```

If the version is already `v0.3.1` or later, find where it stopped. The last
progress line names how many files were chunked, and the file after that one in
scan order is the suspect.

## No output after the scan line

Only `scanned` appears and nothing follows. Two different causes, and they are
told apart by whether the process is burning CPU:

- **Using CPU.** It is chunking, and one file is slow or stuck. Progress is
  only reported once a file finishes, so a single slow file produces silence.
- **Idle.** It is waiting on the network. Between `scanned` and the first
  `chunked` line, the run health-checks Minnow — five attempts at a flat poll
  interval, retried only for transport errors, 429, and 5xx — and recovers any
  journal left by an earlier interrupted run, which issues delete requests in
  batches. An unreachable Minnow or a large recovery both stall here. An
  authentication rejection does not; it fails immediately.

Check with `top -pid <pid>` or `ps -o %cpu= -p <pid>` before guessing.

Also confirm `--quiet` is not set, since it suppresses progress and the JSON
result together.

## Missing environment variables

```
missing environment variables: MINNOW_TOKEN
```

The config references `${MINNOW_TOKEN}` but the variable is not exported in the
current shell. Export it, or persist it in the shell profile. Sourcing a
profile that only defines the variable inside a conditional block will not fix
it. A variable that is set but empty is reported the same way.

Under an MCP client the same message comes back from every tool call rather
than the terminal, because the client starts the server without the shell it
was registered from. Name the variable in the server entry: `env_vars` for
Codex, `-e NAME=value` for Claude Code, an `environment` map for OpenCode. See
the `codeindex-setup` skill.

## A token that is accepted, then rejected

`codeindex`'s connection check calls `/healthz`, and deployments commonly leave
that route unauthenticated while protecting everything else. A wrong token
therefore passes the check and fails later during upload, against `/rag/ingest`
or `/v1/vectors`. Treat "it connected" as no evidence about the token.

Probe an authenticated route instead. Never print the token to inspect it:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H "Authorization: Bearer ${MINNOW_TOKEN}" https://minnow.example.com/mcp
```

Only `401` means the token was rejected. **Any other code means it was
accepted.** A bare GET of `/mcp` that gets past the proxy is answered by the
MCP handler, not the proxy, so expect `405` or `400` depending on version. Do
not read those as failure.

`401` also occurs when `MINNOW_TOKEN` is unset, since the header is then sent
as a bare `Bearer `. Confirm the variable is non-empty before concluding the
value is wrong.

## A refresh is already running

```
index refresh already running for <index-key> (lock <path>)
```

Usually nothing needs deleting. A lock is treated as stale automatically once
its file has not been touched for the operation timeout plus one minute, which
is 11 minutes by default, **and** the recorded process is gone. Waiting out that
window is the safe fix.

To inspect it, note that state lives under the Git top level, not the current
directory:

```bash
ls -la "$(git rev-parse --show-toplevel)/.minnow/codeindex/"
cat "$(git rev-parse --show-toplevel)"/.minnow/codeindex/*.lock   # holds the pid
kill -0 <pid> || echo "dead"
```

Remove a lock by hand only after confirming the process is dead and the wait is
not acceptable.

A `.journal` file next to it records completed files and acknowledged chunks.
A `.pending` file records a completed upload awaiting hosted finalization. The
next ordinary indexing invocation resumes both automatically. Do not delete
either file by hand; doing so discards the information needed to skip confirmed
work or reconcile a hosted finalization.

During finalization, the CLI reports `publishing and finalizing branch scope`
with elapsed-time heartbeats. The hosted operation continues if the CLI exits.
Use `codeindex status` to inspect its local phase.

## The service runs out of memory

If ingest fails on the Minnow side rather than in the CLI, the DuckDB memory
limit is the usual cause on a small machine. It is set in the service config
and overridden without rebuilding an image:

```
MINNOW_DUCKDB_MEMORY_LIMIT=512MB
```

Check the current value before changing it. The Fly, AWS, and root Compose
examples already set this environment variable to `512MB`, so setting it again
changes nothing and the cause lies elsewhere.

The `128MB` config value survives in `deploy/docker/minnow.yaml`, which the
image bakes in, and in `deploy/fly/minnow.yaml`. Editing either is usually
pointless, because the environment variable overrides the file and every
Compose path already sets it. The case that genuinely runs at `128MB` is a bare
`docker run` of the image with no environment variable, so set it on the
container rather than editing a committed template.

`codeindex codebase --low-resource` reduces request size from the client side
and is worth trying regardless.

## The index looks empty or wrong

`codeindex` scopes an index to the directory it runs in. From a subdirectory it
indexes only that subtree under a different `kb_id`, and `codeindex status`
there reports `"indexed": false` even though the repository is indexed. Run
from the output of `git rev-parse --show-toplevel` before concluding anything.

## Files are missing from the index

Most often the file type is not indexed. Indexing uses an extension allowlist;
`.sql`, `.html`, `.css`, `.vue`, `.tf`, `.proto`, `.txt`, and `Makefile` are
among the types absent from it, so re-running alone changes nothing. Files whose
name contains `secret` or `credentials` are dropped too.

The allowlist lives in `code_index.include` in the config file and is editable.
Add the pattern, then re-index. The full list is in the `codeindex-index` skill.

Untracked files are excluded by default. They do **not** appear in the
`skipped` count, which only counts candidates that were considered and then
rejected, so `(0 skipped)` is no evidence that untracked files were included.
Re-run with `--include-untracked`.
