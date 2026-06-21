package kb

import (
	"context"
	"fmt"

	"github.com/mikills/minnow/kb/search"
)

type RagQueryOptions struct {
	TopK        int
	MaxDistance *float64
	Filter      *search.FilterExpr
}

type RagQueryRequest struct {
	KBID     string
	QueryVec []float32
	Options  RagQueryOptions
}

type GraphQueryOptions struct {
	TopK        int
	MaxDistance *float64
	Filter      *search.FilterExpr
	Expansion   *ExpansionOptions
}

type GraphQueryRequest struct {
	KBID     string
	QueryVec []float32
	Options  GraphQueryOptions
}

type IngestUpsertRequest struct {
	KBID    string
	Docs    []Document
	Upload  bool
	Options UpsertDocsOptions
}

type IngestDeleteRequest struct {
	KBID    string
	DocIDs  []string
	Upload  bool
	Options DeleteDocsOptions
}

type IngestResult struct {
	MutatedCount int
}

type EmbeddedDocument struct {
	ID        string          `json:"id"`
	Text      string          `json:"text"`
	MediaIDs  []string        `json:"media_ids,omitempty"`
	MediaRefs []ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata  map[string]any  `json:"metadata,omitempty"`
	Embedding []float32       `json:"embedding"`
}

type PreparedPublishRequest struct {
	KBID        string
	Docs        []EmbeddedDocument
	GraphResult *GraphBuildResult
	Upload      bool
	Options     UpsertDocsOptions
}

type PreparedStreamRequest struct {
	KBID    string
	Next    func(context.Context) ([]EmbeddedDocument, error)
	Options UpsertDocsOptions
}

type BM25QueryRequest struct {
	KBID      string
	QueryText string
	Options   RagQueryOptions
}

type ArtifactFormat interface {
	Kind() string
	Version() int
	FileExt() string
	BuildArtifacts(ctx context.Context, kbID, srcPath string, targetBytes int64) ([]SnapshotShardMetadata, error)
	QueryRag(ctx context.Context, req RagQueryRequest) ([]ExpandedResult, error)
	QueryBM25(ctx context.Context, req BM25QueryRequest) ([]ExpandedResult, error)
	QueryGraph(ctx context.Context, req GraphQueryRequest) ([]ExpandedResult, error)
	Ingest(ctx context.Context, req IngestUpsertRequest) (IngestResult, error)
	Delete(ctx context.Context, req IngestDeleteRequest) (IngestResult, error)
}

type PreparedArtifactPublisher interface {
	PublishPrepared(ctx context.Context, req PreparedPublishRequest) (IngestResult, error)
}

type PreparedArtifactCommitter interface {
	CommitPrepared(ctx context.Context, kbID string) error
}

type PreparedArtifactStreamer interface {
	PublishPreparedStream(ctx context.Context, req PreparedStreamRequest) (IngestResult, error)
}

func ValidateRagQueryRequest(req RagQueryRequest) error {
	if req.Options.TopK <= 0 {
		return fmt.Errorf("%w: top_k must be > 0", ErrInvalidQueryRequest)
	}
	if len(req.QueryVec) == 0 {
		return fmt.Errorf("%w: query vector cannot be empty", ErrInvalidQueryRequest)
	}
	if req.Options.MaxDistance != nil && *req.Options.MaxDistance <= 0 {
		return fmt.Errorf("%w: max_distance must be > 0", ErrInvalidQueryRequest)
	}
	return nil
}

func ValidateGraphQueryRequest(req GraphQueryRequest) error {
	if req.Options.TopK <= 0 {
		return fmt.Errorf("%w: top_k must be > 0", ErrInvalidQueryRequest)
	}
	if len(req.QueryVec) == 0 {
		return fmt.Errorf("%w: query vector cannot be empty", ErrInvalidQueryRequest)
	}
	if req.Options.MaxDistance != nil && *req.Options.MaxDistance <= 0 {
		return fmt.Errorf("%w: max_distance must be > 0", ErrInvalidQueryRequest)
	}
	return nil
}
