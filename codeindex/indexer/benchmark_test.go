package codeindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkChunkText(b *testing.B) {
	languages := map[string]func(int) string{
		"go":         benchGoSource,
		"javascript": benchJavaScriptSource,
		"typescript": benchTypeScriptSource,
		"python":     benchPythonSource,
		"rust":       benchRustSource,
	}
	for _, lines := range []int{200, 2000, 8000} {
		for language, build := range languages {
			text := build(lines)
			b.Run(fmt.Sprintf("%s_lines=%d", language, lines), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = ChunkText(text, language, 1200, 120)
				}
			})
		}
	}
}

func BenchmarkBuildDocuments(b *testing.B) {
	root := b.TempDir()
	path := filepath.Join(root, "bench.go")
	text := benchGoSource(2000)
	require.NoError(b, os.WriteFile(path, []byte(text), 0o644))
	hashBytes := sha256.Sum256([]byte(text))
	file := ScannedFile{
		AbsPath:   path,
		RelPath:   "bench.go",
		Hash:      hex.EncodeToString(hashBytes[:]),
		SizeBytes: int64(len(text)),
		Language:  "go",
	}
	opts := Options{ChunkSize: 1200, ChunkOverlap: 120}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := BuildDocuments(ctx, root, "repo", file, opts)
		require.NoError(b, err)
	}
}

func benchGoSource(lines int) string {
	var b strings.Builder
	for i := range lines / 8 {
		fmt.Fprintf(&b, "func Function%d(input string) string {\n", i)
		fmt.Fprintf(&b, "\tvalue := input + %q\n", fmt.Sprintf("-%d", i))
		b.WriteString("\tif value == \"\" {\n")
		b.WriteString("\t\treturn input\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn value\n")
		b.WriteString("}\n\n")
	}
	return b.String()
}

func benchJavaScriptSource(lines int) string {
	var b strings.Builder
	for i := range lines / 8 {
		fmt.Fprintf(&b, "export function function%d(input) {\n", i)
		fmt.Fprintf(&b, "  const value = input + %q;\n", fmt.Sprintf("-%d", i))
		b.WriteString("  if (value === '') {\n")
		b.WriteString("    return input;\n")
		b.WriteString("  }\n")
		b.WriteString("  return value;\n")
		b.WriteString("}\n\n")
	}
	return b.String()
}

func benchTypeScriptSource(lines int) string {
	var b strings.Builder
	for i := range lines / 8 {
		fmt.Fprintf(&b, "export function function%d(input: string): string {\n", i)
		fmt.Fprintf(&b, "  const value = input + %q;\n", fmt.Sprintf("-%d", i))
		b.WriteString("  if (value === '') {\n")
		b.WriteString("    return input;\n")
		b.WriteString("  }\n")
		b.WriteString("  return value;\n")
		b.WriteString("}\n\n")
	}
	return b.String()
}

func benchPythonSource(lines int) string {
	var b strings.Builder
	for i := range lines / 7 {
		fmt.Fprintf(&b, "def function_%d(input_value):\n", i)
		fmt.Fprintf(&b, "    value = input_value + %q\n", fmt.Sprintf("-%d", i))
		b.WriteString("    if value == '':\n")
		b.WriteString("        return input_value\n")
		b.WriteString("    return value\n")
		b.WriteString("\n")
	}
	return b.String()
}

func benchRustSource(lines int) string {
	var b strings.Builder
	for i := range lines / 8 {
		fmt.Fprintf(&b, "pub fn function_%d(input: &str) -> String {\n", i)
		fmt.Fprintf(&b, "    let value = format!(\"{}-%d\", input);\n", i)
		b.WriteString("    if value.is_empty() {\n")
		b.WriteString("        return input.to_string();\n")
		b.WriteString("    }\n")
		b.WriteString("    value\n")
		b.WriteString("}\n\n")
	}
	return b.String()
}
