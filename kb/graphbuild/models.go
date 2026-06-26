package graphbuild

import (
	"context"

	"github.com/mikills/minnow/kb/media"
)

type Chunk struct {
	DocID     string
	ChunkID   string
	Text      string
	Start     int
	End       int
	MediaRefs []media.ChunkMediaRef
}

type EntityCandidate struct {
	Name    string
	ChunkID string
}

type EdgeCandidate struct {
	Src     string
	Dst     string
	RelType string
	Weight  float64
	ChunkID string
}

type Extraction struct {
	Entities []EntityCandidate
	Edges    []EdgeCandidate
}

type Grapher interface {
	Extract(ctx context.Context, chunks []Chunk) (*Extraction, error)
}

type Canonicalizer interface {
	Canonicalize(ctx context.Context, name string) (string, error)
}

type Entity struct {
	ID   string
	Name string
}

type Edge struct {
	SrcID   string
	DstID   string
	RelType string
	Weight  float64
	ChunkID string
}

type EntityChunkMapping struct {
	EntityID string
	ChunkID  string
}

type BuildResult struct {
	Chunks              []Chunk
	Entities            []Entity
	Edges               []Edge
	EntityChunkMappings []EntityChunkMapping
}
