package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	indexer "github.com/mikills/minnow/codeindex/indexer"
)

type recordingDeleter struct {
	calls   int
	kbIDs   []string
	deleted []string
	failOn  int
}

func (r *recordingDeleter) delete(_ context.Context, kbID string, ids []string) error {
	r.calls++
	if r.failOn > 0 && r.calls == r.failOn {
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
		if err := recoverUploadJournals(context.Background(), deleter, dir, nil); err != nil {
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
		if err := recoverUploadJournals(context.Background(), deleter, dir, nil); err != nil {
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
		if err := recoverUploadJournals(context.Background(), deleter, dir, nil); err != nil {
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
		if err := recoverUploadJournals(context.Background(), deleter, dir, nil); err == nil {
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
		if err := recoverUploadJournals(context.Background(), deleter, dir, nil); err != nil {
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
		if err := recoverUploadJournals(context.Background(), deleter, t.TempDir(), nil); err != nil {
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
