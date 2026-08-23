---
name: codeindex-search
description: Search the current repository and Git branch through codeindex's read-only MCP tools. Use for conceptual code questions before broad filesystem searches.
---

# Searching indexed code

Use `codeindex_search` for conceptual questions such as where retries are
handled or how authentication is validated. Use exact filesystem search when
the symbol or string is already known.

Codeindex resolves the repository and current Git branch locally on every call.
Do not construct or pass Minnow knowledge-base or scope identifiers.

## Search

Call `codeindex_search` with:

| Argument | Meaning |
| --- | --- |
| `query` | Natural-language query. Required. |
| `k` | Results to return. Defaults to 10, maximum 200. |
| `path` | Optional case-sensitive path substring. |
| `language` | Optional exact language name. |

Each result includes its chunk ID, path, line range, language, symbol, kind,
content, and distance. Lower distance is a closer match. Open the real file at
the returned path and line before making changes.

Call `codeindex_status` when search reports that the branch is not indexed or
is still finalizing. The status tool is local and read-only. Indexing runs from
the CLI or installed Git hooks; the MCP server cannot refresh or remove indexes.

Language filters use detected names such as `go`, `javascript`, `typescript`,
`python`, `rust`, `java`, `cpp`, `csharp`, `swift`, `shell`, and `markdown`.
