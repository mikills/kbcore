package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	indexer "github.com/mikills/minnow/codeindex/indexer"
)

type recordingDeleter struct {
	calls   int
	kbIDs   []string
	deleted []string
	failOn  int
	failKB  string
}

func (r *recordingDeleter) delete(_ context.Context, kbID string, ids []string) error {
	r.calls++
	if (r.failOn > 0 && r.calls == r.failOn) || (r.failKB != "" && kbID == r.failKB) {
		return errors.New("delete failed")
	}
	r.kbIDs = append(r.kbIDs, kbID)
	r.deleted = append(r.deleted, ids...)
	return nil
}

func writeStateFile(t *testing.T, path string, chunkIDs ...string) {
	t.Helper()
	writeStateFileWith(t, path, map[string]stateFile{"a.go": {ChunkIDs: chunkIDs}})
}

func writeJournalFile(t *testing.T, path, kbID string, ids ...string) {
	t.Helper()
	writeJournalFileWith(t, path, kbID, ids, nil)
}

func writeJournalFileWith(t *testing.T, path, kbID string, ids, stale []string) {
	t.Helper()
	body := "kb " + kbID + "\n"
	for _, id := range ids {
		body += "i " + id + "\n"
	}
	for _, p := range stale {
		body += "s " + p + "\n"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeStateFileWith(t *testing.T, path string, files map[string]stateFile) {
	t.Helper()
	state := indexState{SchemaVersion: indexStateSchema, Files: files}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverUploadJournal(t *testing.T) {
	t.Run("deletes from the kb recorded in the journal", func(t *testing.T) {
		dir := t.TempDir()
		// No state file: the first-ever index crashed, so the next run mints a new KBID.
		writeJournalFile(t, filepath.Join(dir, "main-abc.json.journal"), "code-gen1", "c1", "c2", "c1")
		deleter := &recordingDeleter{}
		if _, err := recoverUploadJournals(context.Background(), deleter, dir, journalRecovery{}, nil); err != nil {
			t.Fatal(err)
		}
		if strings.Join(deleter.deleted, ",") != "c1,c2" {
			t.Fatalf("deleted %v", deleter.deleted)
		}
		if len(deleter.kbIDs) != 1 || deleter.kbIDs[0] != "code-gen1" {
			t.Fatalf("deleted from %v, want the recorded kb code-gen1", deleter.kbIDs)
		}
	})

	t.Run("keeps chunks the sibling state already records", func(t *testing.T) {
		dir := t.TempDir()
		statePath := filepath.Join(dir, "main-abc.json")
		writeStateFile(t, statePath, "c1", "c2")
		writeJournalFile(t, statePath+".journal", "code-gen1", "c1", "c2")
		deleter := &recordingDeleter{}
		if _, err := recoverUploadJournals(context.Background(), deleter, dir, journalRecovery{}, nil); err != nil {
			t.Fatal(err)
		}
		if len(deleter.deleted) != 0 {
			t.Fatalf("deleted chunks state already records: %v", deleter.deleted)
		}
		if _, err := os.Stat(statePath); err != nil {
			t.Fatalf("state file disturbed: %v", err)
		}
	})

	t.Run("sweeps journals belonging to other index keys", func(t *testing.T) {
		dir := t.TempDir()
		writeJournalFile(t, filepath.Join(dir, "main-abc.json.journal"), "kb-main", "m1")
		writeJournalFile(t, filepath.Join(dir, "feature-def.json.journal"), "kb-feature", "f1")
		deleter := &recordingDeleter{}
		if _, err := recoverUploadJournals(context.Background(), deleter, dir, journalRecovery{}, nil); err != nil {
			t.Fatal(err)
		}
		if len(deleter.kbIDs) != 2 {
			t.Fatalf("swept %v, want both journals recovered", deleter.kbIDs)
		}
		left, _ := filepath.Glob(filepath.Join(dir, "*.journal"))
		if len(left) != 0 {
			t.Fatalf("journals left behind: %v", left)
		}
	})

	t.Run("keeps the journal when a delete batch fails partway", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "main-abc.json.journal")
		ids := make([]string, 0, 250)
		for i := range 250 {
			ids = append(ids, fmt.Sprintf("c%03d", i))
		}
		writeJournalFile(t, path, "code-gen1", ids...)
		deleter := &recordingDeleter{failOn: 2}
		if _, err := recoverUploadJournals(context.Background(), deleter, dir, journalRecovery{}, nil); err == nil {
			t.Fatal("expected the delete failure to propagate")
		}
		if deleter.calls != 2 {
			t.Fatalf("expected the ids to be deleted in batches, got %d call(s)", deleter.calls)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("journal removed after a failed recovery: %v", err)
		}
	})

	t.Run("invalidates state for files whose chunks were deleted", func(t *testing.T) {
		dir := t.TempDir()
		statePath := filepath.Join(dir, "main-abc.json")
		writeStateFileWith(t, statePath, map[string]stateFile{
			"reverted.go": {Hash: "v1", ChunkIDs: []string{"v1a", "v1b"}},
			"other.go":    {Hash: "h", ChunkIDs: []string{"o1"}},
		})
		writeJournalFileWith(t, statePath+".journal", "code-gen1",
			[]string{"v2a"}, []string{"reverted.go"})

		deleter := &recordingDeleter{}
		if _, err := recoverUploadJournals(context.Background(), deleter, dir, journalRecovery{}, nil); err != nil {
			t.Fatal(err)
		}
		got := strings.Join(deleter.deleted, ",")
		if !strings.Contains(got, "v2a") || !strings.Contains(got, "v1a") || !strings.Contains(got, "v1b") {
			t.Fatalf("deleted %q, want the orphaned v2 chunk and the stale v1 chunks", got)
		}
		state, err := loadStateFile(statePath)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := state.Files["reverted.go"]; ok {
			t.Fatal("stale file still recorded in state; it would never be re-indexed")
		}
		if _, ok := state.Files["other.go"]; !ok {
			t.Fatal("untouched file dropped from state")
		}
	})

	t.Run("no journal is a no-op", func(t *testing.T) {
		deleter := &recordingDeleter{}
		if _, err := recoverUploadJournals(context.Background(), deleter, t.TempDir(), journalRecovery{}, nil); err != nil {
			t.Fatal(err)
		}
		if deleter.calls != 0 {
			t.Fatalf("unexpected deletes: %v", deleter.deleted)
		}
	})
}

func TestUploadJournalRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json.journal")
	journal, err := openUploadJournal(path, "code-gen1")
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.record([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := journal.record(nil); err != nil {
		t.Fatal(err)
	}

	// Readable before close: every batch is flushed as it is written.
	contents, err := loadUploadJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if contents.kbID != "code-gen1" || strings.Join(contents.ids, ",") != "a,b" {
		t.Fatalf("loaded kb=%q ids=%v", contents.kbID, contents.ids)
	}
	if err := journal.remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal not removed: %v", err)
	}
}

func TestOpenUploadJournalRequiresKBID(t *testing.T) {
	if _, err := openUploadJournal(filepath.Join(t.TempDir(), "s.json.journal"), ""); err == nil {
		t.Fatal("expected an error without a kb id")
	}
}

type orderedRecorder struct {
	order *[]string
	err   error
}

func (o *orderedRecorder) record(ids []string) error {
	if o.err != nil {
		return o.err
	}
	*o.order = append(*o.order, "record:"+strings.Join(ids, "+"))
	return nil
}

type orderedIngester struct {
	order *[]string
}

func (o *orderedIngester) ingest(_ context.Context, _ string, docs []indexer.Document) error {
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	*o.order = append(*o.order, "ingest:"+strings.Join(ids, "+"))
	return nil
}

func TestDocumentSinkJournalsBeforeIngest(t *testing.T) {
	var order []string
	policy := indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20}
	sink := &documentSink{
		client:  &orderedIngester{order: &order},
		kbID:    "kb",
		policy:  policy,
		journal: &orderedRecorder{order: &order},
	}
	if err := sink.emit(context.Background(), docsOfSize("f1", 10, 10, 10, 10)); err != nil {
		t.Fatal(err)
	}
	if err := sink.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "record:f1a+f1b,ingest:f1a+f1b,record:f1c+f1d,ingest:f1c+f1d"
	if strings.Join(order, ",") != want {
		t.Fatalf("got %q, want %q", strings.Join(order, ","), want)
	}
}

func TestDocumentSinkSkipsIngestWhenJournalFails(t *testing.T) {
	var order []string
	policy := indexer.ResourcePolicy{EmbedBatchSize: 1, MaxBatchBytes: 1 << 20}
	sink := &documentSink{
		client:  &orderedIngester{order: &order},
		kbID:    "kb",
		policy:  policy,
		journal: &orderedRecorder{order: &order, err: errors.New("journal failed")},
	}
	err := sink.emit(context.Background(), docsOfSize("f1", 10, 10))
	if err == nil {
		err = sink.close(context.Background())
	}
	if err == nil {
		t.Fatal("expected the journal error to propagate")
	}
	if len(order) != 0 {
		t.Fatalf("ingested despite journal failure: %v", order)
	}
}

func TestJournalRecoveryRebuildsFilesItInvalidated(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	for name, body := range map[string]string{
		"a.go": "package main\nfunc A() {}\n",
		"b.go": "package main\nfunc B() {}\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")

	server := newTestMinnowServer(t)
	defer server.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = server.URL
	cfg.CodeIndex.PollInterval = "1ms"
	cfg.CodeIndex.OperationTimeout = "1s"
	requireConfirm := false
	cfg.CodeIndex.RequireConfirm = &requireConfirm
	opts := indexCLIOptions{root: root, yes: true}

	first, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	target, err := resolveTarget(opts)
	if err != nil {
		t.Fatal(err)
	}
	// A run killed after it recorded a.go as stale: recovery deletes those
	// chunks, so the next run has to rebuild the file rather than trust the
	// state entry it read a moment earlier.
	writeJournalFileWith(t, indexStatePath(target)+".journal", first.KBID, nil, []string{"a.go"})

	server.mu.Lock()
	server.ingests = nil
	server.mu.Unlock()

	second, err := refreshIndex(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.IndexedFiles != 1 || second.UnchangedFiles != 1 {
		t.Fatalf("indexed %d, unchanged %d, want a.go rebuilt and b.go left alone",
			second.IndexedFiles, second.UnchangedFiles)
	}
	server.mu.Lock()
	ingests := append([]ingestRequest(nil), server.ingests...)
	server.mu.Unlock()
	var paths []string
	for _, in := range ingests {
		for _, doc := range in.Documents {
			if path, ok := doc.Metadata["code_path"].(string); ok {
				paths = append(paths, path)
			}
		}
	}
	if len(paths) == 0 || !slices.Contains(paths, "a.go") {
		t.Fatalf("uploaded %v, want a.go back in the index", paths)
	}
}

func TestJournalRecoveryScope(t *testing.T) {
	ctx := context.Background()

	t.Run("a journal held by a live run is left alone", func(t *testing.T) {
		dir := t.TempDir()
		other := filepath.Join(dir, "other-abc.json.journal")
		writeJournalFile(t, other, "kb-other", "c1")
		// One checkout can hold several indexes. The chunks in a running index's
		// journal are about to be recorded, not orphaned.
		lock := indexLock{PID: os.Getpid(), Token: "t", CreatedAt: time.Now().UTC()}
		data, err := json.Marshal(lock)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "other-abc.json.lock"), data, 0o600); err != nil {
			t.Fatal(err)
		}

		deleter := &recordingDeleter{}
		own := journalRecovery{ownJournal: filepath.Join(dir, "mine-abc.json.journal"), staleAfter: time.Hour}
		if _, err := recoverUploadJournals(ctx, deleter, dir, own, nil); err != nil {
			t.Fatal(err)
		}
		if len(deleter.deleted) != 0 {
			t.Fatalf("deleted %v out from under a running index", deleter.deleted)
		}
		if _, err := os.Stat(other); err != nil {
			t.Fatalf("a running index's journal was removed: %v", err)
		}
	})

	t.Run("another index's journal that cannot be cleaned does not stop this run", func(t *testing.T) {
		dir := t.TempDir()
		other := filepath.Join(dir, "other-abc.json.journal")
		writeJournalFile(t, other, "kb-other", "c1")
		mine := filepath.Join(dir, "mine-abc.json.journal")
		writeJournalFile(t, mine, "kb-mine", "c2")

		// A knowledge base with an open session refuses an unrelated delete.
		deleter := &recordingDeleter{failKB: "kb-other"}
		own := journalRecovery{ownJournal: mine, staleAfter: time.Hour}
		if _, err := recoverUploadJournals(ctx, deleter, dir, own, nil); err != nil {
			t.Fatalf("another index's leftovers stopped this run: %v", err)
		}
		if _, err := os.Stat(other); err != nil {
			t.Fatalf("a journal that was never cleaned was removed anyway: %v", err)
		}
		// This run's own journal still has to be cleaned before it uploads.
		if strings.Join(deleter.deleted, ",") != "c2" {
			t.Fatalf("deleted %v, want this run's own orphans", deleter.deleted)
		}
	})

	t.Run("the run's own journal failing stops the run", func(t *testing.T) {
		dir := t.TempDir()
		mine := filepath.Join(dir, "mine-abc.json.journal")
		writeJournalFile(t, mine, "kb-mine", "c1")

		// Uploading on top of orphans this run cannot remove leaves chunks in
		// the knowledge base that no later run can ever address.
		deleter := &recordingDeleter{failKB: "kb-mine"}
		own := journalRecovery{ownJournal: mine, staleAfter: time.Hour}
		if _, err := recoverUploadJournals(ctx, deleter, dir, own, nil); err == nil {
			t.Fatal("this run uploaded over orphans it could not remove")
		}
	})

	t.Run("the run's own journal is reconciled against the state it loaded", func(t *testing.T) {
		dir := t.TempDir()
		journal := filepath.Join(dir, "new-abc.json.journal")
		// An index key migration leaves state at the legacy path, so the file beside
		// the journal does not exist.
		legacy := filepath.Join(dir, "old-abc.json")
		writeStateFileWith(t, legacy, map[string]stateFile{"a.go": {ChunkIDs: []string{"c1", "c2"}}})
		writeJournalFileWith(t, journal, "kb-1", nil, []string{"a.go"})

		deleter := &recordingDeleter{}
		own := journalRecovery{ownJournal: journal, statePath: legacy, staleAfter: time.Hour}
		invalidated, err := recoverUploadJournals(ctx, deleter, dir, own, nil)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(deleter.deleted, ",") != "c1,c2" {
			t.Fatalf("deleted %v, want the stale file's chunks", deleter.deleted)
		}
		if strings.Join(invalidated[journal], ",") != "a.go" {
			t.Fatalf("invalidated %v, want a.go", invalidated[journal])
		}
	})
}
