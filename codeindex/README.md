# codeindex

`codeindex` is the developer-facing codebase indexing CLI. It owns command
parsing, repository selection, Git hooks, and resource flags while using
Minnow's runtime and knowledge-base APIs for storage and embeddings.


```bash
go install github.com/mikills/minnow/codeindex@latest
codeindex codebase
```

When working from the Minnow checkout before a release is published, use
`go install ./codeindex` instead.

The GitHub install requires matching published versions: Minnow is tagged
`v0.1.0`, and this nested module is tagged `codeindex/v0.1.0`.
