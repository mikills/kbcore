# KB Core Docs Overview

Please use this README as an entry point, then jump to the detailed docs:

- Architecture details: `docs/architecture.md`
- Data model and lifecycle: `docs/data-lifecycle.md`
- Local setup and runtime config: `docs/getting-started.md`

## TL;DR

- Storage is shard-only: manifest + immutable shard DuckDB files.
- Writes use per-KB lease + manifest CAS (`UploadIfMatch`) to prevent lost updates.
- Queries read manifest, choose small-KB full scan or centroid-ranked fanout, then merge deterministically.
- Cache is pod-local and protected by TTL + size budget eviction.
- Graph query mode is strict: if knowledge graph data is unavailable, request fails.

## End-to-End Flow

```mermaid
flowchart LR
    Client([Client]) --> App[KB App]
    App --> Ingest["/rag/ingest"]
    App --> Query["/rag/query"]

    Ingest --> Mutate[Mutable tx + checkpoint]
    Mutate --> Publish[Upload shards + CAS manifest]
    Publish --> Blob[(Blob store)]

    Query --> Planner[Path selection + shard ranking]
    Planner --> Cache[(Local shard cache)]
    Planner --> Blob
    Planner --> Merge[Deterministic top-K merge]
```

## Write Path (Ingest)

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as App
    participant K as KB Runtime
    participant L as Write Lease
    participant B as Blob Store

    C->>A: POST /rag/ingest
    A->>K: UpsertDocsAndUpload*
    K->>L: Acquire(kb_id)
    K->>K: apply mutation + checkpoint
    K->>B: upload shard files
    K->>B: UploadIfMatch(manifest)
    alt CAS success
        K-->>A: success
        A-->>C: 200
    else CAS conflict
        K-->>A: conflict
        A-->>C: retry-capable error
    end
```

## Query Path

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as App
    participant K as KB Runtime
    participant B as Blob Store

    C->>A: POST /rag/query
    A->>K: TopK/SearchByDistance
    K->>B: download manifest
    alt missing manifest
        K-->>A: ErrKBUninitialized
        A-->>C: 400
    else manifest exists
        K->>K: select small-KB or fanout path
        K->>K: query selected shards in parallel
        K->>K: deterministic global merge
        K-->>A: results
        A-->>C: 200
    end
```

## Quick Run

```bash
go test ./... -count=1
go run .
curl -s http://127.0.0.1:8080/healthz
```
