package codeindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type ScannedFile struct {
	AbsPath   string
	RelPath   string
	Hash      string
	SizeBytes int64
	Language  string
}

type ChunkMetadata struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	Language  string `json:"language,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
	Kind      string `json:"kind,omitempty"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
}

type Document struct {
	ID       string
	Text     string
	Metadata map[string]any
}

type Chunk struct {
	Text      string
	Symbol    string
	Kind      string
	StartLine int
	EndLine   int
}

func BuildDocuments(
	ctx context.Context,
	root, repoID string,
	file ScannedFile,
	opts Options,
) ([]Document, []ChunkMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(file.AbsPath)
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != file.Hash {
		return nil, nil, fmt.Errorf("code index file changed while scanning: %s", file.RelPath)
	}
	return BuildDocumentsFromBytes(ctx, repoID, file, data, opts)
}

func BuildDocumentsFromBytes(
	ctx context.Context,
	repoID string,
	file ScannedFile,
	data []byte,
	opts Options,
) ([]Document, []ChunkMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	text := strings.TrimSpace(string(data))
	if text == "" || IsLikelyBinaryBytes(data) {
		return nil, nil, nil
	}
	chunks := ChunkText(text, file.Language, opts.ChunkSize, opts.ChunkOverlap)
	docs := make([]Document, 0, len(chunks))
	metas := make([]ChunkMetadata, 0, len(chunks))
	for _, chunk := range chunks {
		id := StableChunkID(
			ChunkIDInput{
				RepoID:    repoID,
				RelPath:   file.RelPath,
				StartLine: chunk.StartLine,
				EndLine:   chunk.EndLine,
				FileHash:  file.Hash,
				Text:      chunk.Text,
			},
		)
		meta := ChunkMetadata{
			ID:        id,
			Path:      file.RelPath,
			Hash:      file.Hash,
			Language:  file.Language,
			Symbol:    chunk.Symbol,
			Kind:      chunk.Kind,
			StartLine: chunk.StartLine,
			EndLine:   chunk.EndLine,
		}
		docs = append(
			docs,
			Document{
				ID:       id,
				Text:     FormatChunkText(file.RelPath, file.Language, chunk),
				Metadata: map[string]any{"code": meta},
			},
		)
		metas = append(metas, meta)
	}
	return docs, metas, nil
}

// ChunkText is exported, so sizes the caller never normalized reach it here.
func normalizeChunkSizes(chunkSize, overlap int) (int, int) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if overlap < 0 || overlap >= chunkSize {
		overlap = 0
	}
	return chunkSize, overlap
}

func ChunkText(text, language string, chunkSize, overlap int) []Chunk {
	chunkSize, overlap = normalizeChunkSizes(chunkSize, overlap)
	lines := splitLineViews(text)
	markers := symbolMarkers(lines, language)
	if len(markers) == 0 || len(lines) > 2000 {
		return lineChunks(lineChunkInput{source: text, lines: lines, chunkSize: chunkSize, overlap: overlap})
	}
	chunks := make([]Chunk, 0, len(markers))
	for i, marker := range markers {
		start := marker.line
		end := len(lines)
		if i+1 < len(markers) {
			end = markers[i+1].line - 1
		}
		if start > end || start < 1 {
			continue
		}
		sectionLines := lines[start-1 : end]
		if lineRangeLen(sectionLines) <= chunkSize {
			if section := joinTrimmedLineViews(text, sectionLines); section != "" {
				chunks = append(
					chunks,
					Chunk{Text: section, Symbol: marker.symbol, Kind: marker.kind, StartLine: start, EndLine: end},
				)
			}
			continue
		}
		for _, c := range lineChunks(
			lineChunkInput{
				source:    text,
				lines:     sectionLines,
				chunkSize: chunkSize,
				overlap:   overlap,
				symbol:    marker.symbol,
				kind:      marker.kind,
			},
		) {
			c.StartLine += start - 1
			c.EndLine += start - 1
			chunks = append(chunks, c)
		}
	}
	return nonEmptyChunks(chunks)
}

type lineView struct {
	text  string
	start int
	end   int
}

func splitLineViews(text string) []lineView {
	lines := make([]lineView, 0, strings.Count(text, "\n")+1)
	start := 0
	for i := range len(text) {
		if text[i] != '\n' {
			continue
		}
		lines = append(lines, lineView{text: text[start:i], start: start, end: i})
		start = i + 1
	}
	return append(lines, lineView{text: text[start:], start: start, end: len(text)})
}

func lineRangeLen(lines []lineView) int {
	length := 0
	for _, line := range lines {
		length += len(line.text) + 1
	}
	return length
}

const (
	languageJavaScript = "javascript"
	languageTypeScript = "typescript"

	symbolKindClass    = "class"
	symbolKindFunction = "function"
	symbolKindType     = "type"
)

type symbolMarker struct {
	line   int
	symbol string
	kind   string
}

func symbolMarkers(lines []lineView, language string) []symbolMarker {
	switch language {
	case "go":
		return goSymbolMarkers(lines)
	case languageJavaScript, languageTypeScript:
		return jsSymbolMarkers(lines)
	case "python":
		return pythonSymbolMarkers(lines)
	case "rust":
		return rustSymbolMarkers(lines)
	}
	return nil
}

func goSymbolMarkers(lines []lineView) []symbolMarker {
	markers := make([]symbolMarker, 0, estimatedSymbolMarkers(len(lines)))
	for i, line := range lines {
		trimmed := strings.TrimSpace(line.text)
		if symbol, ok := goFunctionSymbol(trimmed); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: symbolKindFunction})
			continue
		}
		if symbol, ok := goTypeSymbol(trimmed); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: symbolKindType})
		}
	}
	return markers
}

func jsSymbolMarkers(lines []lineView) []symbolMarker {
	markers := make([]symbolMarker, 0, estimatedSymbolMarkers(len(lines)))
	for i, line := range lines {
		trimmed := strings.TrimSpace(line.text)
		if symbol, kind, ok := jsSymbol(trimmed); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: kind})
		}
	}
	return markers
}

func jsSymbol(trimmed string) (string, string, bool) {
	trimmed = strings.TrimPrefix(trimmed, "export default ")
	trimmed = strings.TrimPrefix(trimmed, "export ")
	trimmed = strings.TrimPrefix(trimmed, "async ")
	if symbol, ok := prefixedCallSymbol(trimmed, "function ", isJSIdent); ok {
		return symbol, symbolKindFunction, true
	}
	if symbol, ok := prefixedWordSymbol(trimmed, "class ", isJSIdent); ok {
		return symbol, symbolKindClass, true
	}
	if symbol, ok := jsAssignedSymbol(trimmed); ok {
		return symbol, symbolKindFunction, true
	}
	return "", "", false
}

func jsAssignedSymbol(trimmed string) (string, bool) {
	for _, prefix := range []string{"const ", "let ", "var "} {
		name, rest, ok := prefixedNameAndRest(trimmed, prefix, isJSIdent)
		if !ok || !strings.HasPrefix(strings.TrimLeft(rest, " \t"), "=") {
			continue
		}
		return name, true
	}
	return "", false
}

func pythonSymbolMarkers(lines []lineView) []symbolMarker {
	markers := make([]symbolMarker, 0, estimatedSymbolMarkers(len(lines)))
	for i, line := range lines {
		trimmed := strings.TrimSpace(line.text)
		if symbol, ok := prefixedCallSymbol(trimmed, "def ", isGoIdent); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: symbolKindFunction})
			continue
		}
		if symbol, ok := prefixedWordSymbol(trimmed, "class ", isGoIdent); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: symbolKindClass})
		}
	}
	return markers
}

func rustSymbolMarkers(lines []lineView) []symbolMarker {
	markers := make([]symbolMarker, 0, estimatedSymbolMarkers(len(lines)))
	for i, line := range lines {
		trimmed := strings.TrimPrefix(strings.TrimSpace(line.text), "pub ")
		if symbol, ok := prefixedCallSymbol(trimmed, "fn ", isGoIdent); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: symbolKindFunction})
			continue
		}
		if symbol, ok := rustTypeSymbol(trimmed); ok {
			markers = append(markers, symbolMarker{line: i + 1, symbol: symbol, kind: symbolKindType})
		}
	}
	return markers
}

func rustTypeSymbol(trimmed string) (string, bool) {
	for _, prefix := range []string{"struct ", "enum ", "trait "} {
		if symbol, ok := prefixedWordSymbol(trimmed, prefix, isGoIdent); ok {
			return symbol, true
		}
	}
	return "", false
}

func goFunctionSymbol(trimmed string) (string, bool) {
	rest, ok := strings.CutPrefix(trimmed, "func ")
	if !ok {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if strings.HasPrefix(rest, "(") {
		closeParen := strings.IndexByte(rest, ')')
		if closeParen < 0 || closeParen+1 >= len(rest) {
			return "", false
		}
		rest = strings.TrimSpace(rest[closeParen+1:])
	}
	openParen := strings.IndexByte(rest, '(')
	if openParen <= 0 {
		return "", false
	}
	symbol := strings.TrimSpace(rest[:openParen])
	return symbol, isGoIdent(symbol)
}

func goTypeSymbol(trimmed string) (string, bool) {
	rest, ok := strings.CutPrefix(trimmed, "type ")
	if !ok {
		return "", false
	}
	rest = strings.TrimLeft(rest, " \t")
	end := 0
	for end < len(rest) && rest[end] != ' ' && rest[end] != '\t' {
		end++
	}
	if end == 0 || end == len(rest) {
		return "", false
	}
	symbol := rest[:end]
	return symbol, isGoIdent(symbol)
}

func prefixedCallSymbol(trimmed string, prefix string, valid func(string) bool) (string, bool) {
	rest, ok := strings.CutPrefix(trimmed, prefix)
	if !ok {
		return "", false
	}
	openParen := strings.IndexByte(strings.TrimLeft(rest, " \t"), '(')
	if openParen <= 0 {
		return "", false
	}
	symbol := strings.TrimSpace(strings.TrimLeft(rest, " \t")[:openParen])
	return symbol, valid(symbol)
}

func prefixedWordSymbol(trimmed string, prefix string, valid func(string) bool) (string, bool) {
	name, _, ok := prefixedNameAndRest(trimmed, prefix, valid)
	return name, ok
}

func prefixedNameAndRest(trimmed string, prefix string, valid func(string) bool) (string, string, bool) {
	rest, ok := strings.CutPrefix(trimmed, prefix)
	if !ok {
		return "", "", false
	}
	rest = strings.TrimLeft(rest, " \t")
	end := 0
	for end < len(rest) && rest[end] != ' ' && rest[end] != '\t' && rest[end] != '(' && rest[end] != '{' && rest[end] != ':' {
		end++
	}
	if end == 0 {
		return "", "", false
	}
	name := rest[:end]
	return name, rest[end:], valid(name)
}

func isGoIdent(s string) bool {
	if s == "" || !isGoIdentStart(rune(s[0])) {
		return false
	}
	for _, r := range s[1:] {
		if !isGoIdentPart(r) {
			return false
		}
	}
	return true
}

func isGoIdentStart(r rune) bool {
	return r == '_' || isASCIILetter(r)
}

func isGoIdentPart(r rune) bool {
	return isGoIdentStart(r) || isASCIIDigit(r)
}

func isJSIdent(s string) bool {
	if s == "" || !isJSIdentStart(rune(s[0])) {
		return false
	}
	for _, r := range s[1:] {
		if !isJSIdentPart(r) {
			return false
		}
	}
	return true
}

func isJSIdentStart(r rune) bool {
	return r == '_' || r == '$' || isASCIILetter(r)
}

func isJSIdentPart(r rune) bool {
	return isJSIdentStart(r) || isASCIIDigit(r)
}

func isASCIILetter(r rune) bool {
	return r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z'
}

func isASCIIDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func estimatedSymbolMarkers(lineCount int) int {
	estimate := lineCount / 16
	if estimate < 8 {
		return 8
	}
	return estimate
}

type lineChunkInput struct {
	source    string
	lines     []lineView
	chunkSize int
	overlap   int
	symbol    string
	kind      string
}

func lineChunks(input lineChunkInput) []Chunk {
	var chunks []Chunk
	start := 0
	for start < len(input.lines) {
		if len(input.lines[start].text)+1 > input.chunkSize {
			chunks = append(
				chunks,
				splitLongLineChunks(
					longLineChunkInput{
						line:      input.lines[start].text,
						lineNo:    start + 1,
						chunkSize: input.chunkSize,
						overlap:   input.overlap,
						symbol:    input.symbol,
						kind:      input.kind,
					},
				)...)
			start++
			continue
		}
		end := chunkEnd(input.lines, start, input.chunkSize)
		if chunk := chunkFromLines(input, start, end); chunk.Text != "" {
			chunks = append(chunks, chunk)
		}
		if end >= len(input.lines) {
			break
		}
		start = nextChunkStart(input.lines[start:end], end, input.overlap)
	}
	return chunks
}

func chunkEnd(lines []lineView, start int, chunkSize int) int {
	end := start
	length := 0
	for end < len(lines) {
		lineLen := len(lines[end].text) + 1
		if end > start && length+lineLen > chunkSize {
			break
		}
		length += lineLen
		end++
	}
	if end == start {
		return end + 1
	}
	return end
}

func chunkFromLines(input lineChunkInput, start int, end int) Chunk {
	text := joinTrimmedLineViews(input.source, input.lines[start:end])
	if text == "" {
		return Chunk{}
	}
	return Chunk{Text: text, Symbol: input.symbol, Kind: input.kind, StartLine: start + 1, EndLine: end}
}

func joinTrimmedLineViews(source string, lines []lineView) string {
	first := 0
	for first < len(lines) && strings.TrimSpace(lines[first].text) == "" {
		first++
	}
	last := len(lines) - 1
	for last >= first && strings.TrimSpace(lines[last].text) == "" {
		last--
	}
	if first > last {
		return ""
	}
	return strings.TrimSpace(source[lines[first].start:lines[last].end])
}

func nextChunkStart(window []lineView, end int, overlap int) int {
	// Overlapping the whole window repeats the chunk instead of advancing it.
	lines := overlapLines(window, overlap)
	if lines >= len(window) {
		return end
	}
	start := end - lines
	if start < 0 || start >= end {
		return end
	}
	return start
}

type longLineChunkInput struct {
	line      string
	lineNo    int
	chunkSize int
	overlap   int
	symbol    string
	kind      string
}

func splitLongLineChunks(input longLineChunkInput) []Chunk {
	line := strings.TrimSpace(input.line)
	if line == "" {
		return nil
	}
	step := input.chunkSize - input.overlap
	if step <= 0 {
		step = input.chunkSize
	}
	var chunks []Chunk
	for start := 0; start < len(line); {
		end := min(start+input.chunkSize, len(line))
		text := strings.TrimSpace(line[start:end])
		if text != "" {
			chunks = append(
				chunks,
				Chunk{
					Text:      text,
					Symbol:    input.symbol,
					Kind:      input.kind,
					StartLine: input.lineNo,
					EndLine:   input.lineNo,
				},
			)
		}
		if end >= len(line) {
			break
		}
		start += step
	}
	return chunks
}

func overlapLines(lines []lineView, overlapChars int) int {
	if overlapChars <= 0 || len(lines) == 0 {
		return 0
	}
	total := 0
	count := 0
	for _, line := range slices.Backward(lines) {
		total += len(line.text) + 1
		count++
		if total >= overlapChars {
			break
		}
	}
	return count
}

func nonEmptyChunks(in []Chunk) []Chunk {
	out := in[:0]
	for _, c := range in {
		if strings.TrimSpace(c.Text) != "" {
			out = append(out, c)
		}
	}
	return out
}

func FormatChunkText(path, language string, chunk Chunk) string {
	var b strings.Builder
	b.WriteString("path: ")
	b.WriteString(path)
	b.WriteString("\n")
	if language != "" {
		b.WriteString("language: ")
		b.WriteString(language)
		b.WriteString("\n")
	}
	if chunk.Symbol != "" {
		b.WriteString("symbol: ")
		b.WriteString(chunk.Symbol)
		b.WriteString("\n")
	}
	b.WriteString("lines: ")
	b.WriteString(strconv.Itoa(chunk.StartLine))
	b.WriteString("-")
	b.WriteString(strconv.Itoa(chunk.EndLine))
	b.WriteString("\n\n")
	b.WriteString(chunk.Text)
	return b.String()
}

type ChunkIDInput struct {
	RepoID    string
	RelPath   string
	StartLine int
	EndLine   int
	FileHash  string
	Text      string
}

func StableChunkID(input ChunkIDInput) string {
	var hashInput strings.Builder
	hashInput.Grow(len(input.RepoID) + len(input.RelPath) + len(input.FileHash) + len(input.Text) + 32)
	hashInput.WriteString(input.RepoID)
	hashInput.WriteByte('\x00')
	hashInput.WriteString(input.RelPath)
	hashInput.WriteByte('\x00')
	hashInput.WriteString(strconv.Itoa(input.StartLine))
	hashInput.WriteByte('\x00')
	hashInput.WriteString(strconv.Itoa(input.EndLine))
	hashInput.WriteByte('\x00')
	hashInput.WriteString(input.FileHash)
	hashInput.WriteByte('\x00')
	hashInput.WriteString(input.Text)
	sum := sha256.Sum256([]byte(hashInput.String()))
	pathToken := SanitizeIDToken(input.RelPath)
	if len(pathToken) > 80 {
		pathToken = pathToken[len(pathToken)-80:]
	}
	return fmt.Sprintf("code-%s-%d-%d-%s", pathToken, input.StartLine, input.EndLine, hex.EncodeToString(sum[:8]))
}

func SanitizeIDToken(s string) string {
	s = strings.Trim(filepath.ToSlash(s), "/")
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.':
			return r
		default:
			return '-'
		}
	}, s)
}

var LanguageByExt = map[string]string{
	".go": "go", ".js": languageJavaScript, ".jsx": languageJavaScript, ".mjs": languageJavaScript, ".cjs": languageJavaScript, ".ts": languageTypeScript, ".tsx": languageTypeScript, ".py": "python", ".rs": "rust", ".java": "java", ".rb": "ruby", ".php": "php", ".c": "c", ".h": "c", ".cc": "cpp", ".cpp": "cpp", ".cxx": "cpp", ".hpp": "cpp", ".cs": "csharp", ".swift": "swift", ".kt": "kotlin", ".kts": "kotlin", ".md": "markdown", ".mdx": "markdown", ".yaml": "yaml", ".yml": "yaml", ".json": "json", ".sh": "shell", ".bash": "shell", ".zsh": "shell",
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
