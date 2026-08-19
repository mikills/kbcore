---
name: codeindex-index
description: Index a repository into Minnow with codeindex and keep it fresh. Use when the user asks to index or re-index a codebase, refresh an index, install git hooks for it, or asks which files get indexed and how branches and worktrees are handled.
---

# Indexing a repository with codeindex

Run these from the top of the repository. `codeindex setup` must have run
first; see the `codeindex-setup` skill.

Running from a subdirectory is not a no-op: the subdirectory becomes the
indexed scope, which produces a *different* index key and `kb_id`, so it
quietly builds a second index holding only that subtree. `codeindex status`
then reports `"indexed": false` there. `--root` has the same effect
deliberately.

## Index the current branch

```bash
cd /path/to/repository
codeindex codebase
```

Repositories over 1000 scanned files need confirmation, which fails with
`code index requires confirmation for large repository`. Confirm with `--yes`
or `--force`:

```bash
codeindex codebase --yes
```

The threshold is `large_repo_files` in the config, or `--large-repo-files`, and
`require_confirm: false` turns the prompt off entirely.

`codebase` and `refresh` are the same command. A run hashes the current files,
uploads changed chunks, waits for Minnow to publish them, deletes stale chunk
IDs, and saves the new state atomically. Re-running after edits uploads only
what changed.

The JSON result on stdout carries the `kb_id`, which is what searching needs.

## Watch it work

Progress goes to stderr, the JSON result to stdout, so the result can be piped
without the progress mixing in:

```
codeindex: scanned 1629 files (76 skipped)
codeindex: chunked 412/1629 files, 8104 chunks, 6200 uploaded
```

Progress is reported as files finish chunking and as batches upload, throttled
to one line every two seconds, so the counters keep moving during upload even
when no new file has been chunked. `--quiet` suppresses both the progress and the JSON result, despite
its flag description mentioning only JSON.

Work happens between the `scanned` line and the first `chunked` line: a health
check against Minnow, retried up to five times at a flat poll interval, and
recovery of any journal left by an interrupted run, which deletes orphaned
chunks in batches. A pause there is normal. A long one is diagnosed in the
`codeindex-troubleshoot` skill.

## Check state

```bash
codeindex status
```

`status` reads repository-local state under `.minnow/codeindex/` and never
contacts Minnow, so it works with the service down or the config missing.

## Keep it fresh

```bash
codeindex hooks install
codeindex hooks status
codeindex hooks uninstall
```

Hooks re-index after commits and branch switches. They resolve the invoking
worktree each run. A failing hook does not block the Git operation; it appends
diagnostics to Git's `minnow-codeindex-hook.log`.

## Branches and worktrees

The repository identity and current branch derive the index key and `kb_id`, so
each branch gets a separate index and switching branches selects a different
one. Worktrees work the same way, each resolving its own branch. A new local
state generation gets a unique KB suffix, so a fresh clone or a deleted state
cannot reuse stale remote chunks from an older index.

Outside Git, identity comes from the absolute directory path.

Indexing a subdirectory with `--root` produces a different index key and
`kb_id` than indexing the repository root.

## What gets indexed

Indexing is an **extension allowlist**, not "any text file". Only these are
ever considered:

```
.go .js .jsx .ts .tsx .mjs .cjs .py .rs .java .rb .php
.c .cc .cpp .h .hpp .cs .swift .kt .kts .sh .bash .zsh
.md .mdx .yaml .yml .json .toml .xml  and files named Dockerfile
```

Anything else is not indexed by default, including `.sql`, `.html`, `.css`,
`.scss`, `.vue`, `.svelte`, `.proto`, `.tf`, `.gradle`, `.graphql`, `.txt`,
`.rst`, `.cxx`, and `Makefile`. If a user searches such a repository and finds
nothing, this is why, and re-indexing alone will not help.

**This list is configuration, not a limitation.** `codeindex setup` writes all
32 patterns into `code_index.include` in the config file, and that list is
honoured as written. To index `.sql`, add `'**/*.sql'` there and re-index;
`code_index.exclude` works the same way for re-including a directory such as
`fixtures/`. Changing either causes every file to be re-chunked.

Also dropped, silently:

- Files whose name contains `secret` or `credentials`, ends in `.pem` or
  `.key`, or is `.env` or `.env.*`. The check is case insensitive, so
  `Credentials.java` goes too. In a Go repository this removes real source such
  as `secret_store.go`.
- Files larger than `max_file_bytes`, which defaults to 1 MiB, plus empty files
  and symlinks.
- Files detected as binary by content, and default excludes including `vendor/`, `node_modules/`, `dist/`, `build/`,
  `target/`, `coverage/`, `fixtures/`, `data/`, `.next/`, `.turbo/`,
  `*.min.js`, `*.min.css`, `*.map`, `*.lock`, and `.gitignore`.

By default only files Git tracks are indexed. Add untracked files with
`--include-untracked`. Untracked files are not counted in `skipped`; they are
never considered at all.

Chunking is symbol-aware for Go, JavaScript, TypeScript, Python, and Rust.
Every other language, C and C++ included, is chunked by lines — as is any file
over 2000 lines, whatever its language.

## Useful overrides

```bash
codeindex codebase --low-resource     # smaller requests on a constrained machine
codeindex codebase --root subdir      # index one subdirectory
codeindex codebase --throttle 200ms   # slow the request rate
```

First run writes state to `.minnow/codeindex/` under the Git top level and adds
`/.minnow/` to `.git/info/exclude`. If an older setup committed that directory,
untrack it with `git rm -r --cached .minnow`.
