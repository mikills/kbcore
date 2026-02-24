// graph_pipeline.go defines the graph extraction pipeline and the types that
// flow through it.
//
// Pipeline stages:
//
//  1. Chunk   — Chunker splits each Document into fixed-size Chunk values.
//  2. Extract — Grapher receives a batch of Chunks and returns raw
//               EntityCandidates and EdgeCandidates.
//  3. Canonicalize — Canonicalizer maps each raw entity name to a stable ID
//               (e.g. lowercase, deduplicated). If no Canonicalizer is set,
//               the raw name is used as-is.
//
// Type progression:
//
//   EntityCandidate / EdgeCandidate  →  GraphEntity / GraphEdge  →  GraphBuildResult
//   (raw, string names)                 (canonicalized IDs)         (persisted)
//
// Batching:
//
//   Chunks are accumulated up to GraphBuilder.BatchSize before each Grapher
//   call, reducing the number of LLM API round-trips for large document sets.
//
// Incremental persistence:
//
//   BuildAndInsert passes a sink to buildGraph that writes each completed batch
//   to DuckDB immediately, keeping peak memory proportional to batch size rather
//   than total document count.

package kb

import (
	"context"
	"database/sql"
	"fmt"
)

// EntityCandidate is a raw entity name extracted by a Grapher.
type EntityCandidate struct {
	Name    string
	ChunkID string
}

// EdgeCandidate is a raw edge between entity names.
type EdgeCandidate struct {
	Src     string
	Dst     string
	RelType string
	Weight  float64
	ChunkID string
}

// GraphExtraction is the output of a Grapher before canonicalization.
type GraphExtraction struct {
	Entities []EntityCandidate
	Edges    []EdgeCandidate
}

// Grapher extracts entities and edges from chunks.
type Grapher interface {
	Extract(ctx context.Context, chunks []Chunk) (*GraphExtraction, error)
}

// Canonicalizer maps entity names to canonical IDs.
type Canonicalizer interface {
	Canonicalize(ctx context.Context, name string) (string, error)
}

// GraphEntity is a canonicalized entity.
type GraphEntity struct {
	ID   string
	Name string
}

// GraphEdge is a canonicalized edge between entities.
type GraphEdge struct {
	SrcID   string
	DstID   string
	RelType string
	Weight  float64
	ChunkID string
}

// EntityChunkMapping links a standalone entity to its source chunk,
// enabling doc_entities rows for entities that have no edges.
type EntityChunkMapping struct {
	EntityID string
	ChunkID  string
}

// GraphBuildResult contains the intermediate chunks and canonicalized graph output.
type GraphBuildResult struct {
	Chunks              []Chunk
	Entities            []GraphEntity
	Edges               []GraphEdge
	EntityChunkMappings []EntityChunkMapping
}

// GraphBuilder coordinates chunking, graph extraction, and canonicalization.
// BatchSize controls how many chunks are sent to Grapher.Extract per call;
// it defaults to 500 when <= 0. Canonicalizer is optional — when nil, raw
// entity names are used as IDs directly.
type GraphBuilder struct {
	Chunker       Chunker
	Grapher       Grapher
	Canonicalizer Canonicalizer
	BatchSize     int
}

// buildGraph runs the chunk → extract → canonicalize pipeline over docs.
//
// For each batch of chunks it calls Grapher.Extract, canonicalizes entity names
// and edge endpoints, deduplicates entities by ID across batches via entityMap,
// and records an EntityChunkMapping for every entity so doc_entities rows can
// be built even for entities that have no edges.
//
// If sink is non-nil it is called after each completed batch with the
// incremental result so the caller can persist without accumulating the full
// result in memory. pendingEntities is reset after each sink call to avoid
// re-inserting entities that were already flushed.
func (b *GraphBuilder) buildGraph(ctx context.Context, docs []Document, sink func(context.Context, *GraphBuildResult) error) (*GraphBuildResult, error) {
	if b.Chunker == nil {
		return nil, fmt.Errorf("chunker is required")
	}
	if b.Grapher == nil {
		return nil, fmt.Errorf("grapher is required")
	}

	canonicalize := func(name string) (string, error) {
		if b.Canonicalizer == nil {
			return name, nil
		}
		return b.Canonicalizer.Canonicalize(ctx, name)
	}

	batchSize := b.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	entityMap := make(map[string]GraphEntity)
	allEdges := make([]GraphEdge, 0)
	allChunks := make([]Chunk, 0)
	entityChunkMappings := make([]EntityChunkMapping, 0)
	pendingEntities := make([]GraphEntity, 0)

	flush := func(chunks []Chunk, extraction *GraphExtraction) error {
		if extraction == nil {
			return nil
		}

		batchEdges := make([]GraphEdge, 0, len(extraction.Edges))

		for _, ent := range extraction.Entities {
			if ent.Name == "" {
				continue
			}
			id, err := canonicalize(ent.Name)
			if err != nil {
				return err
			}
			if id == "" {
				continue
			}
			if _, ok := entityMap[id]; !ok {
				entity := GraphEntity{ID: id, Name: ent.Name}
				entityMap[id] = entity
				pendingEntities = append(pendingEntities, entity)
			}
			if ent.ChunkID != "" {
				entityChunkMappings = append(entityChunkMappings, EntityChunkMapping{
					EntityID: id,
					ChunkID:  ent.ChunkID,
				})
			}
		}

		for _, edge := range extraction.Edges {
			if edge.Src == "" || edge.Dst == "" {
				continue
			}
			srcID, err := canonicalize(edge.Src)
			if err != nil {
				return err
			}
			dstID, err := canonicalize(edge.Dst)
			if err != nil {
				return err
			}
			if srcID == "" || dstID == "" {
				continue
			}
			weight := edge.Weight
			if weight <= 0 {
				weight = 1.0
			}
			batchEdges = append(batchEdges, GraphEdge{
				SrcID:   srcID,
				DstID:   dstID,
				RelType: edge.RelType,
				Weight:  weight,
				ChunkID: edge.ChunkID,
			})
		}

		allChunks = append(allChunks, chunks...)
		allEdges = append(allEdges, batchEdges...)

		if sink != nil {
			batchResult := &GraphBuildResult{
				Chunks:   chunks,
				Entities: pendingEntities,
				Edges:    batchEdges,
			}
			if err := sink(ctx, batchResult); err != nil {
				return err
			}
		}
		pendingEntities = pendingEntities[:0]

		return nil
	}

	chunkBatch := make([]Chunk, 0, batchSize)
	for _, doc := range docs {
		chunks, err := b.Chunker.Chunk(ctx, doc.ID, doc.Text)
		if err != nil {
			return nil, err
		}
		for _, chunk := range chunks {
			chunkBatch = append(chunkBatch, chunk)
			if len(chunkBatch) >= batchSize {
				extraction, err := b.Grapher.Extract(ctx, chunkBatch)
				if err != nil {
					return nil, err
				}
				if err := flush(chunkBatch, extraction); err != nil {
					return nil, err
				}
				chunkBatch = chunkBatch[:0]
			}
		}
	}

	if len(chunkBatch) > 0 {
		extraction, err := b.Grapher.Extract(ctx, chunkBatch)
		if err != nil {
			return nil, err
		}
		if err := flush(chunkBatch, extraction); err != nil {
			return nil, err
		}
	}

	entities := make([]GraphEntity, 0, len(entityMap))
	for _, ent := range entityMap {
		entities = append(entities, ent)
	}

	return &GraphBuildResult{
		Chunks:              allChunks,
		Entities:            entities,
		Edges:               allEdges,
		EntityChunkMappings: entityChunkMappings,
	}, nil
}

// Build runs the full pipeline and returns the accumulated GraphBuildResult.
// The entire result is held in memory; use BuildAndInsert for large document
// sets where incremental persistence is preferred.
func (b *GraphBuilder) Build(ctx context.Context, docs []Document) (*GraphBuildResult, error) {
	return b.buildGraph(ctx, docs, nil)
}

// BuildAndInsert runs the pipeline and persists each batch to DuckDB as it
// completes, keeping peak memory proportional to BatchSize. Graph tables are
// created if they do not already exist before the first batch is written.
func (b *GraphBuilder) BuildAndInsert(ctx context.Context, db *sql.DB, docs []Document) (*GraphBuildResult, error) {
	if db == nil {
		return nil, fmt.Errorf("db is required")
	}
	if err := EnsureGraphTables(ctx, db); err != nil {
		return nil, err
	}
	return b.buildGraph(ctx, docs, func(ctx context.Context, batch *GraphBuildResult) error {
		return InsertGraphBuildResult(ctx, db, batch)
	})
}
