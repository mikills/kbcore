package codeindex

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var testExcludePatterns = []string{".git/**", "node_modules/**", "vendor/**", "**/vendor/**", "data/**", "**/data/**"}

func testOptions(opts Options) Options {
	if len(opts.Include) == 0 {
		opts.Include = []string{"**/*.go", "**/*.md"}
	}
	if opts.MaxFileBytes <= 0 {
		opts.MaxFileBytes = 1024 * 1024
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1200
	}
	if opts.ChunkOverlap <= 0 {
		opts.ChunkOverlap = 120
	}
	return ResourcePolicyFromOptions(opts).ApplyToOptions(opts)
}

func TestBehavior(t *testing.T) {
	t.Run("chunks symbols", func(t *testing.T) {
		src := `package demo

func Alpha() string { return "alpha" }
type Beta struct{}
func Gamma() string { return "gamma" }
`
		chunks := ChunkText(src, "go", 80, 10)
		require.NotEmpty(t, chunks)
		require.Equal(t, "Alpha", chunks[0].Symbol)
		require.Equal(t, "function", chunks[0].Kind)
		foundBeta := false
		for _, chunk := range chunks {
			foundBeta = foundBeta || chunk.Symbol == "Beta" && chunk.Kind == "type"
		}
		require.True(t, foundBeta)
	})

	t.Run("scan respects ignores", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "main.go"), "package main\nfunc main() {}\n")
		writeTestFile(t, filepath.Join(root, "node_modules", "dep.js"), "module.exports = {}\n")
		writeTestFile(t, filepath.Join(root, ".env"), "SECRET=1\n")
		writeTestFile(t, filepath.Join(root, "ignored.go"), "package ignored\n")
		writeTestFile(t, filepath.Join(root, ".gitignore"), "ignored.go\n")
		runGit(t, root, "init")
		runGit(t, root, "add", ".")
		files, skipped, err := Scan(ctx, root, testOptions(Options{IncludeUntracked: true}), testExcludePatterns)
		require.NoError(t, err)
		require.Greater(t, skipped, 0)
		require.Len(t, files, 1)
		require.Equal(t, "main.go", files[0].RelPath)
	})

	t.Run("wildcard includes text and skips binary", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "text.custom"), "custom text\n")
		require.NoError(t, os.WriteFile(filepath.Join(root, "image.custom"), []byte{0x89, 'P', 'N', 'G', 0}, 0o644))
		runGit(t, root, "init")
		runGit(t, root, "add", ".")
		files, skipped, err := Scan(
			ctx,
			root,
			testOptions(Options{Include: []string{"**/*"}, IncludeUntracked: true}),
			testExcludePatterns,
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, skipped, 1)
		require.Len(t, files, 1)
		require.Equal(t, "text.custom", files[0].RelPath)
	})

	t.Run("scan skips tracked symlinks", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		outside := filepath.Join(t.TempDir(), "outside.go")
		writeTestFile(t, outside, "package outside\n")
		require.NoError(t, os.Symlink(outside, filepath.Join(root, "linked.go")))
		runGit(t, root, "init")
		runGit(t, root, "add", "linked.go")
		files, skipped, err := Scan(ctx, root, testOptions(Options{}), testExcludePatterns)
		require.NoError(t, err)
		require.Empty(t, files)
		require.Equal(t, 1, skipped)
	})

	t.Run("git paths preserve newlines", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		name := "line\nbreak.go"
		writeTestFile(t, filepath.Join(root, name), "package newline\n")
		runGit(t, root, "init")
		runGit(t, root, "add", name)
		files, _, err := Scan(ctx, root, testOptions(Options{}), testExcludePatterns)
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.Equal(t, name, files[0].RelPath)
	})

	t.Run("recursive globs", func(t *testing.T) {
		require.True(t, MatchesAnyPattern("src/main.go", []string{"src/**/*.go"}))
		require.True(t, MatchesAnyPattern("generated/api/types.go", []string{"**/generated/**"}))
		require.False(t, MatchesAnyPattern("packages/app/test/main.ts", []string{"packages/*/src/**/*.ts"}))
	})

	t.Run("splits long lines", func(t *testing.T) {
		chunks := ChunkText(strings.Repeat("a", 3500), "json", 1000, 100)
		require.Len(t, chunks, 4)
		for _, chunk := range chunks {
			require.LessOrEqual(t, len(chunk.Text), 1000)
		}
	})
}

func TestResourcePolicy(t *testing.T) {
	policy := ResourcePolicyFromOptions(Options{})
	require.Equal(t, DefaultEmbedBatchSize, policy.EmbedBatchSize)
	require.Equal(t, DefaultMaxBatchBytes, policy.MaxBatchBytes)
	require.Equal(t, DefaultThrottle, policy.Throttle)
	require.Error(t, ResourcePolicy{MaxHeapBytes: 1}.Check(context.Background()))
	require.Equal(t, 2, (ResourcePolicy{EmbedBatchSize: 3, MaxBatchBytes: 10}).BatchEndByTextBytes([]int{4, 4, 4}))
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	require.NoError(t, cmd.Run())
}
