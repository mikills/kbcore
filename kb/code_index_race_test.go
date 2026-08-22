package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func codeIndexTestHash(contents string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:])
}

func writeCodeIndexScannedFile(t *testing.T, root, rel, contents, scannedHash string) codeScannedFile {
	t.Helper()
	abs := filepath.Join(root, rel)
	require.NoError(t, os.WriteFile(abs, []byte(contents), 0o644))
	return codeScannedFile{
		AbsPath: abs, RelPath: rel, Hash: scannedHash,
		SizeBytes: int64(len(contents)), Language: "go",
	}
}

func TestCodeIndexFileRace(t *testing.T) {
	const oldID = "old-1"
	old := codeIndexedFile{
		Path: "a.go", Hash: codeIndexTestHash("package old\n"), Language: "go", ChunkIDs: []string{oldID},
	}
	oldChunk := CodeChunkMetadata{ID: oldID, Path: "a.go", Hash: old.Hash, Language: "go"}

	tests := []struct {
		name         string
		manifest     codeIndexManifest
		diskContents string
		scannedHash  string
		wantOld      bool
		wantDeletion bool
		wantIndexed  int
		wantChanged  int
	}{
		{
			name: "existing file keeps its published chunks",
			manifest: codeIndexManifest{
				Files:  map[string]codeIndexedFile{"a.go": old},
				Chunks: map[string]CodeChunkMetadata{oldID: oldChunk},
			},
			diskContents: "package third\n",
			scannedHash:  codeIndexTestHash("package second\n"),
			wantOld:      true, wantChanged: 1,
		},
		{
			name: "new file is left for the next run",
			manifest: codeIndexManifest{
				Files: map[string]codeIndexedFile{}, Chunks: map[string]CodeChunkMetadata{},
			},
			diskContents: "package third\n",
			scannedHash:  codeIndexTestHash("package second\n"),
			wantChanged:  1,
		},
		{
			name: "settled replacement still indexes and deletes the old chunks",
			manifest: codeIndexManifest{
				Files:  map[string]codeIndexedFile{"a.go": old},
				Chunks: map[string]CodeChunkMetadata{oldID: oldChunk},
			},
			diskContents: "package settled\n",
			scannedHash:  codeIndexTestHash("package settled\n"),
			wantDeletion: true, wantIndexed: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			file := writeCodeIndexScannedFile(t, root, "a.go", tc.diskContents, tc.scannedHash)
			target := resolvedCodeIndexTarget{Root: root, RepoID: "repo", KBID: "kb"}
			result, deletions, superseded, nextFiles, nextChunks := diffCodeIndexManifest(
				target, tc.manifest, []codeScannedFile{file}, 0,
			)
			streamer := newCodeDocumentStreamer(nil, target, codeIndexPublishState{
				scanned: []codeScannedFile{file}, nextFiles: nextFiles, nextChunks: nextChunks,
				oldChunks: tc.manifest.Chunks, deletions: deletions, superseded: superseded, result: &result,
			})

			require.NoError(t, streamer.addCodeFile(context.Background(), file))

			gotOld := nextFiles["a.go"].Hash == old.Hash
			require.Equal(t, tc.wantOld, gotOld)
			_, deletesOld := deletions[oldID]
			require.Equal(t, tc.wantDeletion, deletesOld)
			require.Equal(t, tc.wantIndexed, result.IndexedFiles)
			require.Equal(t, tc.wantChanged, result.ChangedDuringRun)
			if tc.wantOld {
				require.Contains(t, nextChunks, oldID)
			}
		})
	}
}
