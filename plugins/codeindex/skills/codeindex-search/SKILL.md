---
name: codeindex-search
description: Search an indexed codebase through Minnow's MCP tools. Use when looking for where something is implemented, which files touch a concept, or examples of a pattern in a repository that codeindex has indexed, instead of reading files or grepping blindly.
---

# Searching an indexed codebase

`codeindex` writes the index; Minnow reads it. Searching goes through Minnow's
MCP server, never through the `codeindex` CLI. Register the server first; see
the `codeindex-setup` skill.

Reach for this before grepping when the question is conceptual, such as "where
is retry handled" or "how do we validate tokens". Grep is better when you
already know the exact symbol or string.

## Find the knowledge base first

Every search targets one knowledge base. Each repository, and each branch
inside it, has its own.

**Pass `kb_id` explicitly on every call whenever Minnow runs anywhere but this
machine.** The `root` and `index_key` arguments are resolved against the
*server's* filesystem, where a deployed Minnow has no copy of the repository.
Passing a local path to a remote service fails like this:

```
lstat /Users: no such file or directory
```

Omitting all three does not fail either: selection falls back to a
`code-default` knowledge base that almost never exists. For `minnow_code_search`
that surfaces as `select vector query path: kb is not initialized` — the same
error a mistyped `kb_id` gives, so the message never tells you which mistake you
made. For `minnow_code_index_status` it is worse: the call succeeds and reports
an empty index. Always pass `kb_id` rather than relying on the error to tell
you.

Get the id from the repository:

```bash
codeindex status
```

```json
{
  "kb_id": "code-myrepo-main-0d6e4079-2baf8531-cdb4ee2a-3d56463468aeae32",
  "index_key": "main-0d6e4079",
  "indexed": true,
  "chunk_count": 35348
}
```

For a Git repository indexed at its root, the `kb_id` contains the `index_key`
verbatim. That does not hold for a non-Git directory or when `--kb` was passed,
so never assemble one by hand; copy it.

`"indexed": false` or an empty `kb_id` means nothing has been indexed yet. Run
`codeindex codebase` first; see the `codeindex-index` skill.

Run `codeindex status` from the top of the repository. From a subdirectory it
resolves a different, usually non-existent, index and reports
`"indexed": false`.

Other sources for the same id, in order of convenience: the JSON result printed
by `codeindex codebase`, its final stderr line, and
`<gitroot>/.minnow/codebase-indexes.json`, which maps every index key to its
`kb_id`. Run `codeindex status` from the same root that was indexed — a
subdirectory index made with `--root` has a different id.

**There is no MCP tool that lists knowledge bases.** With only a remote MCP
server and no checkout of the repository, the id cannot be discovered; ask the
user to run `codeindex status` in it.

## Search

Call `minnow_code_search` with the query and that `kb_id`:

| Argument | Meaning |
| --- | --- |
| `query` | Natural language. Required. |
| `kb_id` | Which index to search. Use the one from `codeindex status`. |
| `k` | Results to return. Defaults to 10, maximum 200. |
| `path` | Keep results whose path **contains** this string. Not a glob, and case sensitive. |
| `language` | Keep one language. Exact match, case insensitive. |
| `root`, `index_key` | Server-side resolution. See the warning above. |

`language` takes the detected name, not the extension or a common alias:

```
go  javascript  typescript  python  rust  java  ruby  php
c   cpp  csharp  swift  kotlin  shell  markdown  yaml  json  dockerfile
```

`.h` is `c`, `.cc`/`.cpp`/`.cxx`/`.hpp` are `cpp`, `.md`/`.mdx` are `markdown`,
`.sh`/`.bash`/`.zsh` are `shell`. Anything unmapped, such as `.toml` or `.xml`,
uses the bare extension. `js`, `ts`, and `c++` match nothing and return no
error, which is indistinguishable from an empty result.

The reply is an object, `{"kb_id": ..., "results": [...]}`, not a bare array.
Each result carries `id`, `path`, `start_line`, `end_line`, `language`,
`symbol`, `kind`, `content`, and `distance`. **Lower `distance` is a closer
match.**

Use `path` and `start_line` to open the real file. Treat `content` as a pointer
to code, not as the current source, since the index is only as fresh as the
last run.

## Reading the results

Chunks are symbol-aware for Go, JavaScript, TypeScript, Python, and Rust, so
`symbol` and `kind` usually name the enclosing function or type. Other
languages, and any file over 2000 lines whatever its language, are chunked by
lines and often have neither.

Filters apply after retrieval and then truncate to `k`, so a narrow `path`
filter can return fewer than `k` results even when more exist. Widen the filter
or raise `k` rather than concluding nothing matched.

Empty results usually mean the file type is not indexed at all rather than a
stale index. Check the allowlist in the `codeindex-index` skill before
re-indexing; it is editable, so an unindexed file type is a fixable
configuration problem rather than a limitation.

To confirm an index really is populated, call `minnow_code_index_status` — and
pass the same `kb_id`, for the same reason as above. It defaults to
`code-default` and will report an empty index otherwise.

## Searching non-code knowledge bases

`minnow_code_search` only reads code indexes. For other content in Minnow, use
`minnow_query`, which takes `query` and `k` — **both required, `k` has no
default here** — plus an optional `kb_id`, a `filter` as a JSON FilterExpr, and
a `search_mode` of `vector`, `bm25`, `hybrid`, `graph`, or `adaptive`.
