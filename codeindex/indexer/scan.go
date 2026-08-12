package codeindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

func Scan(ctx context.Context, root string, opts Options, defaultExcludePatterns []string) ([]ScannedFile, int, error) {
	paths, err := candidatePaths(ctx, root, opts.IncludeUntracked, defaultExcludePatterns)
	if err != nil {
		return nil, 0, err
	}
	files := make([]ScannedFile, 0, len(paths))
	skipped := 0
	for _, rel := range paths {
		file, ok, err := scanFile(ctx, root, rel, opts)
		if err != nil {
			return nil, skipped, err
		}
		if !ok {
			skipped++
			continue
		}
		files = append(files, file)
	}
	return files, skipped, nil
}

func candidatePaths(
	ctx context.Context,
	root string,
	includeUntracked bool,
	defaultExcludePatterns []string,
) ([]string, error) {
	paths, gitOK := gitPaths(ctx, root, includeUntracked)
	if !gitOK {
		var err error
		paths, err = walkPaths(ctx, root, defaultExcludePatterns)
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func scanFile(ctx context.Context, root, rel string, opts Options) (ScannedFile, bool, error) {
	if err := ctx.Err(); err != nil {
		return ScannedFile{}, false, err
	}
	rel = filepath.ToSlash(filepath.Clean(rel))
	if !IsEligibleRelPath(rel, opts) {
		return ScannedFile{}, false, nil
	}
	abs := filepath.Join(root, filepath.FromSlash(rel))
	info, err := os.Lstat(abs)
	if err != nil || info.IsDir() || info.Size() <= 0 || info.Size() > opts.MaxFileBytes {
		return ScannedFile{}, false, nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ScannedFile{}, false, nil
	}
	binary, err := isLikelyBinaryContent(abs)
	if err != nil {
		return ScannedFile{}, false, err
	}
	if binary {
		return ScannedFile{}, false, nil
	}
	hash, err := FileSHA256(ctx, abs)
	if err != nil {
		return ScannedFile{}, false, err
	}
	return ScannedFile{
		AbsPath:   abs,
		RelPath:   rel,
		Hash:      hash,
		SizeBytes: info.Size(),
		Language:  DetectLanguage(rel),
	}, true, nil
}

func IsEligibleRelPath(rel string, opts Options) bool {
	if rel == "." || strings.HasPrefix(rel, "../") || filepath.IsAbs(rel) {
		return false
	}
	if !MatchesAnyPattern(rel, opts.Include) || MatchesAnyPattern(rel, opts.Exclude) {
		return false
	}
	return !IsLikelySecretPath(rel)
}

func gitPaths(ctx context.Context, root string, includeUntracked bool) ([]string, bool) {
	args := []string{"-C", root, "ls-files", "-z", "-c", "--exclude-standard"}
	if includeUntracked {
		args = []string{"-C", root, "ls-files", "-z", "-c", "-o", "--exclude-standard"}
	}
	out, err := exec.CommandContext(ctx, "git", args...).Output()
	if err != nil {
		return nil, false
	}
	return SplitNullDelimited(string(out)), true
}

func walkPaths(ctx context.Context, root string, defaultExcludePatterns []string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel != "." && MatchesAnyPattern(rel+"/x", defaultExcludePatterns) {
				return filepath.SkipDir
			}
			return nil
		}
		out = append(out, rel)
		return nil
	})
	return out, err
}

func isLikelyBinaryContent(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buf := make([]byte, 8192)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return false, err
	}
	return IsLikelyBinaryBytes(buf[:n]), nil
}

func IsLikelyBinaryBytes(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	if bytes.Contains(data, []byte{0}) || !utf8.Valid(data) {
		return true
	}
	control := 0
	for _, b := range data {
		if isBinaryControlByte(b) {
			control++
		}
	}
	return float64(control)/float64(len(data)) > 0.30
}

func isBinaryControlByte(b byte) bool {
	return b < 0x20 && b != '\n' && b != '\r' && b != '\t' && b != '\f' && b != '\b'
}

func MatchesAnyPattern(rel string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}
	rel = filepath.ToSlash(rel)
	base := filepath.Base(rel)
	for _, raw := range patterns {
		if patternMatches(filepath.ToSlash(strings.TrimSpace(raw)), rel, base) {
			return true
		}
	}
	return false
}

func patternMatches(pattern string, rel string, base string) bool {
	if pattern == "" {
		return false
	}
	return directoryPatternMatches(pattern, rel) || matchRecursivePattern(pattern, rel) ||
		filepathPatternMatches(pattern, rel) ||
		basePatternMatches(pattern, base)
}

func directoryPatternMatches(pattern string, rel string) bool {
	if !strings.HasSuffix(pattern, "/**") {
		return false
	}
	prefix := strings.TrimSuffix(pattern, "/**")
	return rel == prefix || strings.HasPrefix(rel, prefix+"/")
}

func filepathPatternMatches(pattern string, value string) bool {
	ok, _ := filepath.Match(pattern, value)
	return ok
}

func basePatternMatches(pattern string, base string) bool {
	return !strings.Contains(pattern, "/") && filepathPatternMatches(pattern, base)
}

func matchRecursivePattern(pattern, rel string) bool {
	re, err := regexp.Compile(globRegexp(pattern))
	if err != nil {
		return false
	}
	return re.MatchString(rel)
}

func globRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); {
		switch pattern[i] {
		case '*':
			if i+2 < len(pattern) && pattern[i:i+3] == "**/" {
				b.WriteString("(?:.*/)?")
				i += 3
				continue
			}
			if i+1 < len(pattern) && pattern[i:i+2] == "**" {
				b.WriteString(".*")
				i += 2
				continue
			}
			b.WriteString("[^/]*")
			i++
		case '?':
			b.WriteString("[^/]")
			i++
		default:
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
			i++
		}
	}
	b.WriteString("$")
	return b.String()
}

func IsLikelySecretPath(rel string) bool {
	base := strings.ToLower(filepath.Base(rel))
	return base == ".env" || strings.HasPrefix(base, ".env.") || strings.HasSuffix(base, ".pem") ||
		strings.HasSuffix(base, ".key") ||
		strings.Contains(base, "credentials") ||
		strings.Contains(base, "secret")
}

func SplitLines(s string) []string {
	fields := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	out := fields[:0]
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func SplitNullDelimited(s string) []string {
	fields := strings.Split(s, "\x00")
	out := fields[:0]
	for _, field := range fields {
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func FileSHA256(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
