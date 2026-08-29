# Benchmark

End-to-end vector query latency over synthetic random vectors, top-10. Single-threaded client, warm state.

## Setup

- Hardware: Apple M4, local SSD.
- Build: embedded library, DuckDB backend with HNSW index.
- Vectors: L2-normalized.
- Shards: default policy (trigger at 150k vector rows).
- DuckDB memory limit: 16 GB. One DuckDB thread per shard; query fanout runs 4 shards in parallel.
- 100 warmup queries, 1000 measured.

## Results

### 384 dim

| Corpus | Seed time | Ingest rate | Query qps | p50 | p90 | p99 | max |
|--------|-----------|-------------|-----------|-----|-----|-----|-----|
| 10k    | 11.9 s    | 842 docs/s  | 340       | 2.7 ms | 3.5 ms | 7.9 ms  | 18.5 ms |
| 100k   | 2m 31s    | 662 docs/s  | 189       | 5.2 ms | 6.1 ms | 9.8 ms  | 20.2 ms |
| 1M     | 25m 52s   | 645 docs/s  | 164       | 5.7 ms | 7.0 ms | 13.3 ms | 32.8 ms |

### 768 dim

| Corpus | Seed time | Ingest rate | Query qps | p50 | p90 | p99 | max |
|--------|-----------|-------------|-----------|-----|-----|-----|-----|
| 10k    | 19.6 s    | 509 docs/s  | 246       | 3.9 ms  | 4.3 ms  | 5.7 ms   | 12.4 ms  |
| 100k   | 3m 40s    | 455 docs/s  | 156       | 6.2 ms  | 6.6 ms  | 11.3 ms  | 29.2 ms  |
| 1M     | 39m 30s   | 422 docs/s  | 36        | 20.1 ms | 45.6 ms | 134.7 ms | 975.3 ms |

### Real corpus with real embedders (20 SEC 10-K filings, 10k chunks)

10k chunks from actual 10-K filings of large US companies (Apple, Microsoft, JPMorgan, etc.) embedded with real models; queries are also embeddings of held-out filing text.

| Case                  | Embedder              | p50    | p90    | p99    | max     | p99/p50 |
|-----------------------|-----------------------|--------|--------|--------|---------|---------|
| 10k / 384             | LocalEmbedder (subword-ngram) | 4.2 ms | 4.5 ms | 5.2 ms | 9.4 ms  | 1.26x |
| 10k / 768             | Ollama nomic-embed-text       | 5.6 ms | 6.2 ms | 8.6 ms | 17.5 ms | 1.55x |

**Synthetic (uniform random vectors) at the same scale for contrast:**

| Case                  | p50    | p90    | p99    | max     | p99/p50 |
|-----------------------|--------|--------|--------|---------|---------|
| 10k / 384 synthetic   | 2.7 ms | 3.5 ms | 7.9 ms | 18.5 ms | 2.93x   |
| 10k / 768 synthetic   | 3.9 ms | 4.3 ms | 5.7 ms | 12.4 ms | 1.46x   |

The p50 is lower on synthetic (simpler query plan on uniform vectors), but the p99/p50 ratio is significantly wider (2.9x vs 1.26x at 384 dim). HNSW on clustered real embeddings has much more predictable probe behaviour because the graph structure mirrors the actual data distribution. The long-tail queries that hurt synthetic workloads mostly disappear.

Ingest throughput with a real embedder is dominated by embedding cost: 38 docs/s with Ollama `nomic-embed-text`, 921 docs/s with the in-repo `LocalEmbedder`.

To regenerate the corpus (runs once, ~18s):

```bash
go run ./scripts/fetch_corpus/
```

The 768-dim real case requires Ollama running with `nomic-embed-text` pulled (`ollama pull nomic-embed-text`). The bench skips cleanly if Ollama is unavailable.

## Real-world runs (code indexing)

Indexing real repositories with `codeindex codebase`, OpenAI
`text-embedding-3-small` via `openai_compatible`, Apple M4 / local SSD.

**Minnow source tree** — 110 files, 22,601 lines, 1,360 chunks (DuckDB limit 2 GB):

| Case | Wall time | Files/s | Chunks/s | Notes |
|------|-----------|---------|----------|-------|
| First index | 5m 56s | 0.31 | 3.82 | Embeds and writes chunks. |
| No-op refresh | 0.26s | 423 | n/a | Hash check only; 0 files re-indexed. |

**Django (`django__django-11333`)** — 2,091 files, 33,861 chunks (DuckDB limit 4 GB):

| Case | Wall time | Files/s | Chunks/s | Notes |
|------|-----------|---------|----------|-------|
| First index | 8m 26s | 4.1 | 66.9 | Streams embedding batches into DuckDB. |
| No-op refresh | 2.41s | 868 | n/a | Hash check only; 0 files re-indexed. |

First-index cost is dominated by the embedder; refreshes are state-based, so an
unchanged tree costs only a hash scan. Keep `include` source-focused — a broad
`["**/*"]` without strong excludes pulls in binaries, fixtures, and generated
files. Minnow's defaults already skip those (see `code_index` in
[configuration.md](docs/configuration.md)).

## vs. Turbopuffer

Turbopuffer's published headline: 1M / 768 dim, p50 8 ms, p90 10 ms, p99 35 ms.

| Metric | minnow 1M / 384 dim | minnow 1M / 768 dim | Turbopuffer 1M / 768 dim |
|--------|---------------------|---------------------|--------------------------|
| p50    | 5.7 ms              | 20.1 ms             | 8 ms                     |
| p90    | 7.0 ms              | 45.6 ms             | 10 ms                    |
| p99    | 13.3 ms             | 134.7 ms            | 35 ms                    |

At Turbopuffer's exact 1M / 768 corpus, minnow trails p50 by ~2.5x and p99 by ~4x. At 384 dim with the same corpus size, minnow leads p99 by ~2.5x.

## Wall-time summary (full suite)

| Case         | Wall time       |
|--------------|-----------------|
| 10k / 384    | 15.2 s          |
| 100k / 384   | 2m 37s          |
| 1M / 384     | 26m 3s          |
| 10k / 768    | 24.2 s          |
| 100k / 768   | 3m 47s          |
| 1M / 768     | 40m 17s         |
| Suite total  | 1h 13m 24s      |

## Reproduce

The default suite is 10k, 100k, and 1M at 384, 512, and 768 dimensions, plus
three real-corpus cases that skip when the corpus is absent (~110 min):

```bash
go test ./kb/duckdb/ -bench=BenchmarkVectorQuery -run=^$ -benchtime=1x -v -timeout=180m
```

Subset by regex:

```bash
go test ./kb/duckdb/ -bench=BenchmarkVectorQuery/1M -run=^$ -benchtime=1x -v -timeout=120m
```

Or name the cases, which is the only way to reach a size the suite does not list:

| Variable | Default | Effect |
| --- | --- | --- |
| `MINNOW_BENCH_CASES` | the suite above | Comma-separated `<count>[k\|M][_real]_dim<n>`. |
| `MINNOW_BENCH_MEMORY_LIMIT` | `16GB` | DuckDB `memory_limit` for the run. |
| `MINNOW_BENCH_TEMP_DIR` | unset | Where DuckDB spills. Unset spills beside the shard. |
| `MINNOW_BENCH_JSON` | unset | Appends one JSON line per case. |

```bash
MINNOW_BENCH_CASES=2M_dim512,5M_dim512 \
MINNOW_BENCH_MEMORY_LIMIT=32GB \
MINNOW_BENCH_JSON=results.jsonl \
  go test ./kb/duckdb/ -bench=BenchmarkVectorQuery -run=^$ -benchtime=1x -v -timeout=360m
```

### On CI

The `Benchmark` workflow runs the same cases on a runner instead of a
workstation. Dispatch it from the Actions tab, or from any branch without
merging first:

```bash
gh workflow run Benchmark --ref my-branch -f cases=1M_dim512,2M_dim512 \
  -f runner=ubuntu-latest -f memory_limit=10GB -f timeout_minutes=350
```

Each case gets its own job, and one summary table reports them in corpus order
with peak RSS. A case that runs out of memory or disk keeps its row with the
reason, so the table shows where the limit is rather than omitting it.

A hosted runner has 15 GB of RAM and caps a job at 6 hours, so memory binds
before disk does. The workflow points the corpus and the DuckDB spill at
whichever of `/mnt` and `RUNNER_TEMP` has more room and reports both the choice
and the free space, so a run records its own headroom rather than assuming it.
Corpora past roughly 2M spill hard enough to want a self-hosted runner, which
caps at 5 days.

A push to a `bench/**` branch, or the `benchmark` label on a pull request, runs
`100k_dim512` alone. The four-case sweep is dispatch-only, so a label cannot
start a multi-hour job.

## Tuning notes

- **DuckDB memory limit matters.** An 8 GB limit at 1M / 768 showed p99 ≈ 488 ms; bumping to 16 GB dropped it to 135 ms. The working set exceeds what an 8 GB buffer pool can keep hot.
- **Threads per shard = 1 is correct.** Raising `PRAGMA threads` to 4 made 1M / 768 worse across all percentiles (16 DuckDB threads across 4 concurrent shards on a 10-core machine = contention).
- **Fanout amplifies tail.** Every query probes 4 shards in parallel; the slowest shard decides query latency. Reducing fanout trades recall for lower tail.

## Caveats

- Not apples-to-apples. minnow is an embedded library on localhost; Turbopuffer is a managed cloud service whose numbers include a network hop.
- Single-threaded client; Turbopuffer's per-namespace 1k+ qps figure reflects parallel clients.
- Synthetic random vectors; real embeddings cluster, which shifts HNSW cost either way.
- Warm state. Cold-cache first query after restart is slower and not measured.
- DuckDB VSS HNSW is experimental; tail variance at 1M / 768 is a characteristic of this implementation.

## Sizing estimates

Rough footprint per document at ~500 chars of text, including the HNSW index:

| Dim  | Per doc | 10k docs | 100k docs | 1M docs |
|------|---------|----------|-----------|---------|
| 384  | ~2.5 KB | ~25 MB   | ~250 MB   | ~2.5 GB |
| 768  | ~4 KB   | ~40 MB   | ~400 MB   | ~4 GB   |
| 1024 | ~5 KB   | ~50 MB   | ~500 MB   | ~5 GB   |

> Throughout this section, **knowledge base** refers to one isolated corpus (minnow's unit of isolation - one manifest, its own shards and HNSW indexes). Within the project this is informally called a "KB", but in this document `KB` is reserved for kilobyte to avoid ambiguity in the sizing tables.

Peak memory (DuckDB memory limit + buffer pool headroom) to keep a single knowledge base hot:

| Largest hot knowledge base | Recommended memory |
|----------------------------|--------------------|
| 10k / any dim              | ~1 GB              |
| 100k / 384                 | ~2 GB              |
| 100k / 768                 | ~4 GB              |
| 1M / 384                   | ~8 GB              |
| 1M / 768                   | ~16 GB             |

Across multiple knowledge bases (the isolation unit; one tenant may own several):

- **Disk** scales linearly with total documents across all knowledge bases. Zero fixed overhead per knowledge base beyond a small manifest blob.
- **Memory** is driven by the working set of *concurrently hot* knowledge bases, not the total count. Cold knowledge bases live on disk and pay no RAM cost. 1000 cold + 1 hot uses the same memory as 1 hot alone.
- **CPU** is per-query. Idle knowledge bases consume nothing.

Concrete scenarios:

- 100 knowledge bases, 5k docs each at 768 dim, one or two hot at a time: ~2 GB disk, 2 GB RAM, any VM.
- 10 knowledge bases, 100k docs each at 384 dim, a handful hot: ~2.5 GB disk, 4 GB RAM.
- One large knowledge base at 1M / 768: ~4 GB disk, 16 GB RAM.

The main bottleneck as knowledge-base count grows is shard cache eviction pressure - if working-set RAM is tight, queries against one that just got evicted pay a disk read to re-warm. Tune `MaxCacheBytes` above the expected hot working set.

### Backed by S3 instead of local disk

Swapping the blob store to S3 (`storage.blob.kind: s3`) changes the cost curve significantly:

- **Local disk collapses to a cache.** Everything authoritative lives in S3; only recently-queried shards are resident. Total node disk = working-set cache, not total corpus.
- **Memory is unchanged.** Still driven by DuckDB buffer pool for hot shards.
- **Cold queries pay one S3 `GET`** for the shard file (typically ~50-200 MB). Expect a few hundred milliseconds of extra latency on first touch. Warm queries behave the same as the local-disk numbers above.
- **Namespace count becomes essentially free.** A cold knowledge base costs only S3 storage. Active node resources scale with *concurrently hot* knowledge bases, not total count.

S3 is durable storage; the node running minnow is where HNSW probes actually happen. Real TCO is S3 + the EC2 instance (RAM + CPU) + the EBS volume holding the local warm cache.

AWS us-east-1 on-demand pricing (as of 2026):

| Component                  | Rate                                  |
|----------------------------|---------------------------------------|
| S3 Standard storage        | $0.023/GB-mo                          |
| S3 `GET`                   | $0.0004/1000                          |
| S3 `PUT`/LIST              | $0.005/1000                           |
| EBS gp3 (local cache)      | $0.08/GB-mo                           |
| Same-region S3 -> EC2      | $0                                    |
| EC2 t4g.small (2 vCPU / 2 GB)   | ~$16/mo                          |
| EC2 t4g.large (2 vCPU / 8 GB)   | ~$49/mo                          |
| EC2 t4g.xlarge (4 vCPU / 16 GB) | ~$98/mo                          |

Concrete scenarios (Graviton on-demand, no Savings Plan). The unit is knowledge bases, not tenants; a tenant often owns several:

| Scenario                                                 | Instance     | S3    | EBS   | Total / mo | Per knowledge base |
|----------------------------------------------------------|--------------|-------|-------|------------|--------------------|
| 10 knowledge bases x 10k docs @ 768, mostly cold         | t4g.small    | $0.01 | $0.40 | ~$17       | ~$1.70             |
| 100 knowledge bases x 10k docs, ~5 hot at once           | t4g.large    | $0.10 | $1.60 | ~$51       | ~$0.51             |
| 1 big knowledge base at 1M / 768                          | t4g.xlarge   | $0.09 | $1.60 | ~$101      | n/a                |

Per-tenant cost is `per-knowledge-base cost x average knowledge bases per tenant`. A tenant with product-docs + support-tickets + internal-wiki (3 knowledge bases) in the 100-knowledge-base scenario costs ~$1.50/mo, not $0.51.

**The real saving vs Turbopuffer is amortisation.** One single-digit-hundred-dollar EC2 instance can host hundreds of knowledge bases, while Turbopuffer typically bills per namespace or per query-volume. A single big 1M/768 corpus on minnow costs roughly the same as Turbopuffer; a SaaS with 100+ small knowledge bases on minnow costs a fraction.

Cost levers:

- Reserved Instances / Savings Plans cut EC2 by 30-60%.
- Spot instances cut it further if the workload tolerates interruption.
- Instance families with local NVMe (`im4gn`, `i4g`) trade a bit more per-hour cost for "free" cache storage - often cheaper than EBS at scale.
- A cold query path that constantly pages shards pays `qps * fanout * 30 * 86400 / 1000 * $0.0004` in `GET` charges on top of the flat S3 storage line; size `MaxCacheBytes` to fit the working set.

S3 is the closest architectural match to Turbopuffer's model. It's the right choice for SaaS with long-tail tenants where most namespaces are cold. Local disk wins for always-hot, latency-critical workloads where the cold-`GET` penalty is unacceptable.
