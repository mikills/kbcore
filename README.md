# Minnow

> [!WARNING]
> **Breaking changes expected.** The on-disk format, event schemas, and public API are still moving. Do not pin a production system to a commit SHA without pinning its data too. DuckDB VSS (HNSW) is an experimental upstream dependency - tail latency at 1M+ docs / 768 dim is a known soft spot (see [`BENCHMARK.md`](BENCHMARK.md)).

Embedded vector search for Go. DuckDB-backed, HNSW-indexed, multi-tenant via per-knowledge-base isolation. A small self-hosted alternative to managed vector databases.

## Quickstart

```bash
go run .
curl -s http://127.0.0.1:8080/healthz
```

For globally installed MCP use with OpenAI embeddings:

```bash
go install github.com/mikills/minnow@latest
$(go env GOPATH)/bin/minnow setup
minnow config init dev-openai
OPENAI_API_KEY=sk-... minnow mcp stdio
```

`minnow setup` checks whether Go's install directory is on your shell `PATH` and
can update your shell profile for you. If `minnow` is already on `PATH`, just run
`minnow setup` directly.

Index a codebase for MCP/code-agent retrieval. From inside the repo, no flags
needed — it indexes `.`, auto-derives the `kb_id`, and uses the `default` index key:

```bash
minnow index codebase          # symbol-aware index of the current repo
minnow index hooks install     # auto-refresh on commit / checkout / merge
```

Then search it via `POST /rag/query` or the `minnow_code_search` MCP tool.
Refreshes are state-based (only changed files re-embed), so re-running is cheap.

The first run writes `.minnow/codebase-indexes.json`, a small repo-local registry
mapping stable agent-facing keys like `default` to the backing `kb_id`, root, and
settings — so later MCP calls pass only `index_key: "default"`. Override any
default with `--kb`, `--index-key`, `--root`, or `--include-untracked`.

The default `minnow.yaml` also exposes MCP for coding agents:

- Streamable HTTP: `http://127.0.0.1:8080/mcp`
- Stdio: `go run . mcp stdio`

The default config at `./minnow.yaml` is sufficient for local development (embedder-only, no external services). See [`docs/getting-started.md`](docs/getting-started.md) for a deployment-grade setup.

## What it does

- Vector and graph RAG over your corpus, exposed at `/rag/query` and `/rag/ingest`.
- Code indexing for coding agents: `minnow index codebase` indexes the current repo (30+ file types; symbol-aware chunking for Go, JS/TS, Python, and Rust), with optional git hooks to keep it fresh.
- Per-tenant isolation through knowledge bases. Each one has its own manifest, shards, and HNSW indexes.
- Two storage modes: local disk for always-hot workloads, S3-backed for SaaS with a long-tail distribution of cold tenants.
- Event-driven ingest pipeline with at-least-once delivery, durable operation lineage, and retry semantics.

## Documentation

- [Getting started](docs/getting-started.md) - install, configure, first request.
- [Architecture](docs/architecture.md) - components, concurrency, graph extraction.
- [Data and pipeline](docs/data-lifecycle.md) - storage model, write pipeline, event model, query path.
- [Configuration reference](docs/configuration.md) - every YAML knob.
- [Benchmark](BENCHMARK.md) - query latency, ingest throughput, sizing estimates, S3 vs local cost comparison.

## Status

Production-ready for small-to-medium corpora (up to ~1M docs per knowledge base). See [`BENCHMARK.md`](BENCHMARK.md) for latency numbers and the known-slow scenarios.

## License

Apache 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). Vendored code under `kb/internal/cron/` remains under its original MIT license (PocketBase); the MIT header stays on those files and the attribution is captured in `NOTICE`.
