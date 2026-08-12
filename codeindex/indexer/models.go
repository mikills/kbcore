package codeindex

import (
	"errors"
	"time"
)

type Options struct {
	KBID             string
	IndexKey         string
	Description      string
	Root             string
	Include          []string
	Exclude          []string
	MaxFileBytes     int64
	ChunkSize        int
	ChunkOverlap     int
	IncludeUntracked bool
	EmbedBatchSize   int
	MaxBatchBytes    int
	Throttle         time.Duration
	MaxHeapBytes     uint64
	MaxRSSBytes      uint64
	LargeRepoFiles   int
	RequireConfirm   bool
	ConfirmedLarge   bool
}

type Result struct {
	KBID           string `json:"kb_id"`
	IndexKey       string `json:"index_key"`
	Description    string `json:"description,omitempty"`
	Root           string `json:"root"`
	ScannedFiles   int    `json:"scanned_files"`
	SkippedFiles   int    `json:"skipped_files"`
	IndexedFiles   int    `json:"indexed_files"`
	DeletedFiles   int    `json:"deleted_files"`
	UnchangedFiles int    `json:"unchanged_files"`
	ChunksIndexed  int    `json:"chunks_indexed"`
	ChunksDeleted  int    `json:"chunks_deleted"`
	ManifestKey    string `json:"manifest_key"`
}

type Status struct {
	KBID        string     `json:"kb_id"`
	IndexKey    string     `json:"index_key,omitempty"`
	Description string     `json:"description,omitempty"`
	Indexed     bool       `json:"indexed"`
	Root        string     `json:"root,omitempty"`
	RepoID      string     `json:"repo_id,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	FileCount   int        `json:"file_count"`
	ChunkCount  int        `json:"chunk_count"`
	ManifestKey string     `json:"manifest_key"`
}

type SearchOptions struct {
	TopK     int
	Path     string
	Language string
}

type SearchResult struct {
	ID        string  `json:"id"`
	Content   string  `json:"content"`
	Distance  float64 `json:"distance"`
	Path      string  `json:"path,omitempty"`
	Language  string  `json:"language,omitempty"`
	Symbol    string  `json:"symbol,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	StartLine int     `json:"start_line,omitempty"`
	EndLine   int     `json:"end_line,omitempty"`
}

type Registry struct {
	SchemaVersion string                   `json:"schema_version"`
	Indexes       map[string]RegistryEntry `json:"codebase_indexes"`
}

type RegistryEntry struct {
	KBID             string `json:"kb_id"`
	Root             string `json:"root"`
	Description      string `json:"description,omitempty"`
	IncludeUntracked bool   `json:"include_untracked"`
}

var ErrRequiresConfirmation = errors.New("code index requires confirmation for large repository")
