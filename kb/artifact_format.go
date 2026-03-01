package kb

import (
	"context"
	"database/sql"
	"fmt"
)

type RagQueryOptions struct {
	TopK int
	// MaxDistance, if set, filters results after top-K retrieval. Results with
	// distance > MaxDistance are excluded. Because filtering is post-retrieval,
	// a small TopK may cause in-threshold results to be missed if they fall
	// outside the top-K window.
	MaxDistance *float64
}

type RagQueryRequest struct {
	KBID     string
	QueryVec []float32
	Options  RagQueryOptions
}

type GraphQueryOptions struct {
	TopK int
	// MaxDistance, if set, filters results after top-K retrieval (same semantics
	// as RagQueryOptions.MaxDistance).
	MaxDistance *float64
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

type ArtifactFormat interface {
	Kind() string
	Version() int
	FileExt() string
	BuildArtifacts(ctx context.Context, kbID, srcPath string, targetBytes int64) ([]SnapshotShardMetadata, error)
	QueryRag(ctx context.Context, req RagQueryRequest) ([]ExpandedResult, error)
	QueryGraph(ctx context.Context, req GraphQueryRequest) ([]ExpandedResult, error)
	Upsert(ctx context.Context, req IngestUpsertRequest) (IngestResult, error)
	Delete(ctx context.Context, req IngestDeleteRequest) (IngestResult, error)
	PrepareAndOpenDB(ctx context.Context, kbID string) (*sql.DB, error)
	BuildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []SnapshotShardMetadata) (SnapshotShardMetadata, error)
	DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*SnapshotShardManifest, error)
}

func validateRagQueryRequest(req RagQueryRequest) error {
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

func validateGraphQueryRequest(req GraphQueryRequest) error {
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
