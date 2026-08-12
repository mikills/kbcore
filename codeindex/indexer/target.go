package codeindex

import (
	"fmt"
	"strings"
)

const (
	DefaultChunkSize    = 1200
	DefaultChunkOverlap = 120
	DefaultMaxFileBytes = 1024 * 1024
)

var DefaultExcludePatterns = []string{
	".git/**", ".minnow/**", "node_modules/**", "vendor/**", "**/vendor/**", "dist/**", "build/**",
	"coverage/**", "fixtures/**", "**/fixtures/**", "data/**", "**/data/**", ".next/**", ".turbo/**",
	"target/**", "extensions/**", "examples/extensions/**", "**/*.duckdb", "**/*.duckdb_extension", "**/*.parquet",
	"**/*.min.js", "**/*.min.css", "**/*.map", "*.lock", ".gitignore",
}

var DefaultIncludePatterns = []string{
	"**/*.go", "**/*.js", "**/*.jsx", "**/*.ts", "**/*.tsx", "**/*.mjs", "**/*.cjs", "**/*.py",
	"**/*.rs", "**/*.java", "**/*.rb", "**/*.php", "**/*.c", "**/*.cc", "**/*.cpp", "**/*.h",
	"**/*.hpp", "**/*.cs", "**/*.swift", "**/*.kt", "**/*.kts", "**/*.sh", "**/*.bash", "**/*.zsh",
	"**/*.md", "**/*.mdx", "**/*.yaml", "**/*.yml", "**/*.json", "**/*.toml", "**/*.xml", "**/Dockerfile",
}

type Target struct {
	Root         string
	RepoRoot     string
	RegistryRoot string
	RepoID       string
	KBID         string
	IndexKey     string
	Description  string
	Registry     Registry
	Options      Options
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

func ResolveTarget(opts Options) (Target, error) {
	root, err := ResolveRequestedRoot(opts.Root)
	if err != nil {
		return Target{}, err
	}
	repoRoot, err := ResolveRoot(root)
	if err != nil {
		return Target{}, err
	}
	opts = NormalizeOptions(opts)
	registry, err := LoadRegistry(repoRoot)
	if err != nil {
		return Target{}, err
	}
	entry, hasEntry := registry.Indexes[opts.IndexKey]
	return Target{
		Root:         resolvedRoot(root, repoRoot, entry, hasEntry),
		RepoRoot:     repoRoot,
		RegistryRoot: repoRoot,
		RepoID:       CodeRepoID(repoRoot),
		KBID:         resolvedKBID(opts, entry, hasEntry),
		IndexKey:     opts.IndexKey,
		Description:  resolvedDescription(opts, entry, hasEntry, root),
		Registry:     registry,
		Options:      resolveIncludeUntracked(opts, entry, hasEntry),
	}, nil
}

func NormalizeOptions(opts Options) Options {
	if strings.TrimSpace(opts.IndexKey) == "" {
		opts.IndexKey = "default"
	} else {
		opts.IndexKey = SanitizeKey(opts.IndexKey)
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
	return ResourcePolicyFromOptions(opts).ApplyToOptions(opts)
}

func resolvedRoot(root string, repoRoot string, entry RegistryEntry, hasEntry bool) string {
	if hasEntry {
		return RootFromEntry(repoRoot, entry)
	}
	return root
}

func resolvedKBID(opts Options, entry RegistryEntry, hasEntry bool) string {
	kbID := strings.TrimSpace(opts.KBID)
	if kbID == "" && hasEntry {
		kbID = entry.KBID
	}
	if kbID == "" {
		return DefaultKBIDForIndexKey(opts.IndexKey)
	}
	return kbID
}

func resolvedDescription(opts Options, entry RegistryEntry, hasEntry bool, root string) string {
	description := strings.TrimSpace(opts.Description)
	if description == "" && hasEntry {
		description = entry.Description
	}
	if description == "" {
		return DefaultDescription(root, opts.IndexKey)
	}
	return description
}

func resolveIncludeUntracked(opts Options, entry RegistryEntry, hasEntry bool) Options {
	opts.IncludeUntracked = opts.IncludeUntracked || (hasEntry && entry.IncludeUntracked)
	return opts
}
