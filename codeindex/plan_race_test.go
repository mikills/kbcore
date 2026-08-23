package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	minnowcode "github.com/mikills/minnow/kb/codeindex"
	"github.com/stretchr/testify/require"
)

func hashOf(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func writeScanned(t *testing.T, root, rel, content, scannedHash string) minnowcode.ScannedFile {
	t.Helper()
	abs := filepath.Join(root, rel)
	require.NoError(t, os.WriteFile(abs, []byte(content), 0o644))
	return minnowcode.ScannedFile{
		AbsPath: abs, RelPath: rel, Hash: scannedHash,
		SizeBytes: int64(len(content)), Language: "go",
	}
}

func planFor(
	t *testing.T, root string, previous indexState, files []minnowcode.ScannedFile,
) (indexPlan, []minnowcode.Document) {
	t.Helper()
	opts := minnowcode.NormalizeOptions(minnowcode.Options{})
	target := indexTarget{Root: root, RepoID: "repo", KBID: "kb", IndexKey: "key"}
	var emitted []minnowcode.Document
	emit := func(_ context.Context, docs []minnowcode.Document) error {
		emitted = append(emitted, docs...)
		return nil
	}
	plan, err := buildIndexPlan(
		context.Background(), target, opts, pipelineFingerprint(opts), previous, files, 0, emit,
	)
	require.NoError(t, err)
	return plan, emitted
}

func statePlus(pipeline string, files map[string]stateFile) indexState {
	return indexState{Pipeline: pipeline, Files: files}
}

func TestIndexPlanFileRace(t *testing.T) {
	opts := minnowcode.NormalizeOptions(minnowcode.Options{})
	pipeline := pipelineFingerprint(opts)

	t.Run("a rewritten file keeps the chunks already indexed for it", func(t *testing.T) {
		root := t.TempDir()
		indexed := stateFile{Hash: hashOf("package a\nfunc A() {}\n"), SizeBytes: 22, Language: "go", ChunkIDs: []string{"old-1"}}
		file := writeScanned(t, root, "a.go", "package a\nfunc C() {}\n", hashOf("package a\nfunc B() {}\n"))

		plan, emitted := planFor(t, root, statePlus(pipeline, map[string]stateFile{"a.go": indexed}), []minnowcode.ScannedFile{file})

		require.Equal(t, 1, plan.result.ChangedDuringRun)
		require.Equal(t, indexed, plan.state.Files["a.go"])
		require.NotContains(t, plan.deleteIDs, "old-1")
		require.NotContains(t, plan.stalePaths, "a.go")
		require.Empty(t, emitted)
	})

	t.Run("a file that vanishes mid-run does not fail the run", func(t *testing.T) {
		root := t.TempDir()
		indexed := stateFile{Hash: hashOf("package a\n"), ChunkIDs: []string{"old-1"}}
		file := minnowcode.ScannedFile{
			AbsPath: filepath.Join(root, "gone.go"), RelPath: "gone.go",
			Hash: hashOf("package gone\n"), Language: "go",
		}

		plan, _ := planFor(t, root, statePlus(pipeline, map[string]stateFile{"gone.go": indexed}), []minnowcode.ScannedFile{file})

		require.Equal(t, 1, plan.result.ChangedDuringRun)
		require.NotContains(t, plan.deleteIDs, "old-1")
	})

	t.Run("a file added and changed before it is read is not indexed at all", func(t *testing.T) {
		root := t.TempDir()
		file := writeScanned(t, root, "new.go", "package new\nfunc C() {}\n", hashOf("package new\nfunc B() {}\n"))

		plan, emitted := planFor(t, root, statePlus(pipeline, map[string]stateFile{}), []minnowcode.ScannedFile{file})

		require.Equal(t, 1, plan.result.ChangedDuringRun)
		require.Zero(t, plan.result.IndexedFiles)
		require.NotContains(t, plan.state.Files, "new.go")
		require.Empty(t, emitted)
	})

	t.Run("old chunks retained during a pipeline change are retried", func(t *testing.T) {
		root := t.TempDir()
		content := "package old\n"
		indexed := stateFile{Hash: hashOf(content), ChunkIDs: []string{"old-1"}}
		file := writeScanned(t, root, "a.go", content, hashOf("package briefly_changed\n"))
		previous := statePlus("old-pipeline", map[string]stateFile{"a.go": indexed})

		first, _ := planFor(t, root, previous, []minnowcode.ScannedFile{file})
		require.Equal(t, previous.Pipeline, first.state.Pipeline)

		settled := writeScanned(t, root, "a.go", content, hashOf(content))
		second, emitted := planFor(t, root, first.state, []minnowcode.ScannedFile{settled})
		require.Equal(t, 1, second.result.IndexedFiles)
		require.Zero(t, second.result.UnchangedFiles)
		require.NotEmpty(t, emitted)
	})

	t.Run("a file that holds still is still reindexed and its old chunks deleted", func(t *testing.T) {
		root := t.TempDir()
		content := "package a\nfunc Settled() {}\n"
		indexed := stateFile{Hash: hashOf("package a\nfunc Old() {}\n"), ChunkIDs: []string{"old-1"}}
		file := writeScanned(t, root, "a.go", content, hashOf(content))

		plan, emitted := planFor(t, root, statePlus(pipeline, map[string]stateFile{"a.go": indexed}), []minnowcode.ScannedFile{file})

		require.Zero(t, plan.result.ChangedDuringRun)
		require.Equal(t, 1, plan.result.IndexedFiles)
		require.Contains(t, plan.deleteIDs, "old-1")
		require.Contains(t, plan.stalePaths, "a.go")
		require.NotEmpty(t, emitted)
	})
}
