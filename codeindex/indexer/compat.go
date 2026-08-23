// Package codeindex preserves the former standalone indexer import path.
// Deprecated: use github.com/mikills/minnow/kb/codeindex.
package codeindex

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"strings"

	shared "github.com/mikills/minnow/kb/codeindex"
)

const (
	DefaultEmbedBatchSize = shared.DefaultEmbedBatchSize
	DefaultMaxBatchBytes  = shared.DefaultMaxBatchBytes
	DefaultMaxHeapBytes   = shared.DefaultMaxHeapBytes
	DefaultMaxRSSBytes    = shared.DefaultMaxRSSBytes
	DefaultThrottle       = shared.DefaultThrottle
	DefaultLargeRepoFiles = shared.DefaultLargeRepoFiles
	DefaultChunkSize      = shared.DefaultChunkSize
	DefaultChunkOverlap   = shared.DefaultChunkOverlap
	DefaultMaxFileBytes   = shared.DefaultMaxFileBytes
)

var (
	CodeHookNames           = append([]string(nil), shared.CodeHookNames...)
	DefaultExcludePatterns  = append([]string(nil), shared.DefaultExcludePatterns...)
	DefaultIncludePatterns  = append([]string(nil), shared.DefaultIncludePatterns...)
	ErrFileChanged          = shared.ErrFileChanged
	ErrRequiresConfirmation = shared.ErrRequiresConfirmation
	LanguageByExt           = maps.Clone(shared.LanguageByExt)

	BuildDocumentsFromBytes   = shared.BuildDocumentsFromBytes
	CodeRepoID                = shared.CodeRepoID
	DefaultDescription        = shared.DefaultDescription
	DefaultKBIDForIndexKey    = shared.DefaultKBIDForIndexKey
	EnsureLocalStateIgnored   = shared.EnsureLocalStateIgnored
	FileSHA256                = shared.FileSHA256
	FormatChunkText           = shared.FormatChunkText
	IsEligibleRelPath         = shared.IsEligibleRelPath
	IsLikelyBinaryBytes       = shared.IsLikelyBinaryBytes
	IsLikelySecretPath        = shared.IsLikelySecretPath
	MatchesAnyPattern         = shared.MatchesAnyPattern
	RelativeRoot              = shared.RelativeRoot
	ResolveRequestedRoot      = shared.ResolveRequestedRoot
	ResolveRoot               = shared.ResolveRoot
	RootFromEntry             = shared.RootFromEntry
	SanitizeIDToken           = shared.SanitizeIDToken
	SanitizeKey               = shared.SanitizeKey
	SaveRegistry              = shared.SaveRegistry
	SplitLines                = shared.SplitLines
	SplitNullDelimited        = shared.SplitNullDelimited
	StableChunkID             = shared.StableChunkID
	ChunkText                 = shared.ChunkText
	ResolveSelection          = shared.ResolveSelection
	LoadRegistry              = shared.LoadRegistry
	ResourcePolicyFromOptions = shared.ResourcePolicyFromOptions
)

type (
	Chunk           = shared.Chunk
	ChunkIDInput    = shared.ChunkIDInput
	ChunkMetadata   = shared.ChunkMetadata
	CodeHookOptions = shared.CodeHookOptions
	CodeHookStatus  = shared.CodeHookStatus
	Document        = shared.Document
	Options         = shared.Options
	Registry        = shared.Registry
	RegistryEntry   = shared.RegistryEntry
	ResourcePolicy  = shared.ResourcePolicy
	Result          = shared.Result
	ScannedFile     = shared.ScannedFile
	SearchOptions   = shared.SearchOptions
	SearchResult    = shared.SearchResult
	Status          = shared.Status
	Target          = shared.Target
)

func NormalizeOptions(opts Options) Options {
	if strings.TrimSpace(opts.IndexKey) == "" {
		opts.IndexKey = "default"
	} else {
		opts.IndexKey = shared.SanitizeKey(opts.IndexKey)
	}
	if opts.MaxFileBytes <= 0 {
		opts.MaxFileBytes = DefaultMaxFileBytes
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = DefaultChunkSize
	}
	if opts.ChunkOverlap < 0 {
		opts.ChunkOverlap = 0
	}
	if opts.ChunkOverlap == 0 {
		opts.ChunkOverlap = DefaultChunkOverlap
	}
	if opts.ChunkOverlap >= opts.ChunkSize {
		opts.ChunkOverlap = opts.ChunkSize / 10
	}
	if len(opts.Include) == 0 {
		opts.Include = append([]string(nil), DefaultIncludePatterns...)
	}
	if len(opts.Exclude) == 0 {
		opts.Exclude = append([]string(nil), DefaultExcludePatterns...)
	}
	return shared.ResourcePolicyFromOptions(opts).ApplyToOptions(opts)
}

func ResolveTarget(opts Options) (Target, error) {
	return shared.ResolveTarget(NormalizeOptions(opts))
}

func DetectLanguage(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if language, ok := LanguageByExt[ext]; ok {
		return language
	}
	base := strings.ToLower(filepath.Base(path))
	if base == "dockerfile" || strings.HasPrefix(base, "dockerfile.") {
		return "dockerfile"
	}
	return strings.TrimPrefix(ext, ".")
}

func Scan(ctx context.Context, root string, opts Options, excludes []string) ([]ScannedFile, int, error) {
	files, skipped, err := shared.Scan(ctx, root, opts, excludes)
	for i := range files {
		files[i].Language = DetectLanguage(files[i].RelPath)
	}
	return files, skipped, err
}

func InstallCodeIndexHooks(ctx context.Context, opts CodeHookOptions) (CodeHookStatus, error) {
	return shared.InstallCodeIndexHooks(ctx, opts)
}

func UninstallCodeIndexHooks(ctx context.Context, root string) (CodeHookStatus, error) {
	return shared.UninstallCodeIndexHooks(ctx, root)
}

func CodeIndexHookStatus(ctx context.Context, root string) (CodeHookStatus, error) {
	return shared.CodeIndexHookStatus(ctx, root)
}

func BuildDocuments(
	ctx context.Context,
	root, repoID string,
	file ScannedFile,
	opts Options,
) ([]Document, []ChunkMetadata, error) {
	docs, metadata, err := shared.BuildDocuments(ctx, root, repoID, file, opts)
	if errors.Is(err, shared.ErrFileChanged) {
		return nil, nil, fmt.Errorf("%w: %s", ErrFileChanged, file.RelPath)
	}
	return docs, metadata, err
}

func ValidateConfirmation(opts Options, scanned int) error {
	if opts.RequireConfirm && !opts.ConfirmedLarge && scanned > opts.LargeRepoFiles {
		return fmt.Errorf(
			"%w: scanned %d files exceeds threshold %d; rerun with confirmation or lower the threshold",
			ErrRequiresConfirmation,
			scanned,
			opts.LargeRepoFiles,
		)
	}
	return nil
}
