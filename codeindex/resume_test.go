package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	indexer "github.com/mikills/minnow/kb/codeindex"
	"github.com/stretchr/testify/require"
)

func TestLegacyResume(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.published["done"] = struct{}{}
	server.mu.Unlock()

	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(path, []byte("kb legacy\ni done\ni missing\n"), 0o600))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	target := indexTarget{KBID: "legacy", ScopeID: "branch"}
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, target, path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.Contains(t, confirmed, "done")
	require.NotContains(t, confirmed, "missing")
	require.NoError(t, journal.close())
}

func TestActiveLegacyResume(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.session = "instance-1:legacy"
	server.ingests = append(server.ingests, ingestRequest{Documents: []ingestDocument{{ID: "done"}}})
	server.mu.Unlock()

	dir := t.TempDir()
	target := indexTarget{KBID: "legacy", ScopeID: "branch", MigrationDir: dir, MigrationIDs: []string{"done"}}
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, sessionFileName(target.KBID)), []byte("instance-1:legacy"), 0o600,
	))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, target, filepath.Join(t.TempDir(), "run.journal"), newProgressReporter(true),
	)
	require.NoError(t, err)
	require.Contains(t, confirmed, "done")
	require.NoError(t, journal.close())
}

func TestLostSession(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(
		path, []byte("kb legacy\nscope branch\ni missing\nc missing\n"), 0o600,
	))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "legacy", ScopeID: "branch"},
		path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.NotContains(t, confirmed, "missing")
	require.NoError(t, journal.close())
}

func TestPublishedResume(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(
		path, []byte("kb legacy\nscope branch\ni missing\nc missing\npublished\n"), 0o600,
	))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "legacy", ScopeID: "branch"},
		path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.NotContains(t, confirmed, "missing")
	require.NoError(t, journal.close())
}

func TestPublishCrash(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	server.scopeStatus = http.StatusInternalServerError
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm

	_, err := refreshIndex(context.Background(), cfg, indexCLIOptions{root: root, yes: true})
	require.Error(t, err)
	server.mu.Lock()
	defer server.mu.Unlock()
	require.NotEmpty(t, server.published)
	require.True(t, server.ingests[0].GCUnscoped)
	require.NotEmpty(t, server.gc)
	scheduled := make(map[string]struct{})
	for _, ids := range server.gc {
		for _, id := range ids {
			scheduled[id] = struct{}{}
		}
	}
	for id := range server.published {
		require.Contains(t, scheduled, id)
	}
}

func TestResumeEpoch(t *testing.T) {
	server := newTestMinnowServer(t)
	defer server.Close()
	server.mu.Lock()
	server.session = "instance-1:retry"
	server.mu.Unlock()
	path := filepath.Join(t.TempDir(), "run.journal")
	require.NoError(t, os.WriteFile(path, []byte(
		"kb legacy\nscope branch\ni old\nc old\npublished\ni retry\nc retry\nsession instance-1:retry\n",
	), 0o600))
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	_, journal, confirmed, err := startUpload(
		context.Background(), cfg, indexTarget{KBID: "legacy", ScopeID: "branch"},
		path, newProgressReporter(true),
	)
	require.NoError(t, err)
	require.Contains(t, confirmed, "retry")
	require.NotContains(t, confirmed, "old")
	require.NoError(t, journal.close())
}

func TestResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.journal")
	journal, contents, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	require.Empty(t, contents.confirmed)

	first := &recordingIngester{}
	sink := &documentSink{
		client: first, kbID: "kb", journal: journal,
		policy:   indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20},
		progress: newProgressReporter(true),
	}
	require.NoError(t, sink.emit(context.Background(), docsOfSize("d", 10, 10)))
	require.NoError(t, sink.close(context.Background()))
	require.NoError(t, journal.close())

	resumed, contents, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"da", "db"}, contents.confirmed)
	confirmed := map[string]struct{}{"da": {}, "db": {}}
	second := &recordingIngester{}
	sink = &documentSink{
		client: second, kbID: "kb", journal: resumed, confirmed: confirmed,
		policy:   indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20},
		progress: newProgressReporter(true),
	}
	require.NoError(t, sink.emit(context.Background(), docsOfSize("d", 10, 10)))
	require.NoError(t, sink.close(context.Background()))
	require.Empty(t, second.batches)
	require.NoError(t, resumed.close())
}

type confirmFailure struct{ uploadJournal }

func (confirmFailure) confirm([]string) error { return errors.New("confirm failed") }

func TestUncertainBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.journal")
	journal, _, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	recorder := &confirmFailure{uploadJournal: *journal}
	sink := &documentSink{
		client: &recordingIngester{}, kbID: "kb", journal: recorder,
		policy:   indexer.ResourcePolicy{EmbedBatchSize: 1, MaxBatchBytes: 1 << 20},
		progress: newProgressReporter(true),
	}
	require.Error(t, sink.emit(context.Background(), docsOfSize("d", 10, 10)))
	require.NoError(t, recorder.close())

	_, contents, err := resumeUploadJournal(path, "kb", "scope")
	require.NoError(t, err)
	require.Empty(t, contents.confirmed)
}
