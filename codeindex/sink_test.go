package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	indexer "github.com/mikills/minnow/codeindex/indexer"
)

type recordingIngester struct {
	batches [][]string
	failOn  int
}

func (r *recordingIngester) ingest(_ context.Context, _ string, docs []indexer.Document) error {
	if r.failOn > 0 && len(r.batches)+1 == r.failOn {
		return errors.New("ingest failed")
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	r.batches = append(r.batches, ids)
	return nil
}

func docsOfSize(prefix string, sizes ...int) []indexer.Document {
	docs := make([]indexer.Document, 0, len(sizes))
	for i, size := range sizes {
		docs = append(docs, indexer.Document{
			ID:   prefix + string(rune('a'+i)),
			Text: strings.Repeat("x", size),
		})
	}
	return docs
}

// The whole-list batching the sink replaced, kept as the reference partition.
func referenceBatches(policy indexer.ResourcePolicy, docs []indexer.Document) [][]string {
	var batches [][]string
	for len(docs) > 0 {
		lengths := make([]int, len(docs))
		for i, doc := range docs {
			lengths[i] = len(doc.Text)
		}
		end := policy.BatchEndByTextBytes(lengths)
		if end <= 0 {
			end = 1
		}
		ids := make([]string, 0, end)
		for _, doc := range docs[:end] {
			ids = append(ids, doc.ID)
		}
		batches = append(batches, ids)
		docs = docs[end:]
	}
	return batches
}

func TestDocumentSinkBatching(t *testing.T) {
	cases := []struct {
		name   string
		policy indexer.ResourcePolicy
		files  [][]indexer.Document
	}{
		{
			name:   "count limit splits across files",
			policy: indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20},
			files: [][]indexer.Document{
				docsOfSize("f1", 10, 10, 10),
				docsOfSize("f2", 10, 10),
			},
		},
		{
			name:   "byte limit cuts before count limit",
			policy: indexer.ResourcePolicy{EmbedBatchSize: 8, MaxBatchBytes: 25},
			files: [][]indexer.Document{
				docsOfSize("f1", 10, 10, 10),
				docsOfSize("f2", 10),
			},
		},
		{
			name:   "single document exceeds the byte limit",
			policy: indexer.ResourcePolicy{EmbedBatchSize: 4, MaxBatchBytes: 5},
			files: [][]indexer.Document{
				docsOfSize("f1", 100),
				docsOfSize("f2", 100),
			},
		},
		{
			name:   "pending lands exactly on the count limit",
			policy: indexer.ResourcePolicy{EmbedBatchSize: 3, MaxBatchBytes: 1 << 20},
			files: [][]indexer.Document{
				docsOfSize("f1", 10, 10, 10),
				docsOfSize("f2", 10, 10, 10),
			},
		},
		{
			name:   "files yielding no documents",
			policy: indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20},
			files: [][]indexer.Document{
				docsOfSize("f1", 10),
				nil,
				docsOfSize("f3", 10, 10),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &recordingIngester{}
			sink := &documentSink{client: client, kbID: "kb", policy: tc.policy}
			var all []indexer.Document
			for _, docs := range tc.files {
				all = append(all, docs...)
				if err := sink.emit(context.Background(), docs); err != nil {
					t.Fatal(err)
				}
			}
			if err := sink.close(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := referenceBatches(tc.policy, all)
			if len(client.batches) != len(want) {
				t.Fatalf("batch count: got %d %v, want %d %v", len(client.batches), client.batches, len(want), want)
			}
			for i := range want {
				if strings.Join(client.batches[i], ",") != strings.Join(want[i], ",") {
					t.Fatalf("batch %d: got %v, want %v", i, client.batches[i], want[i])
				}
			}
			if len(sink.pending) != 0 {
				t.Fatalf("pending not drained: %d", len(sink.pending))
			}
		})
	}
}

func TestDocumentSinkReleasesSent(t *testing.T) {
	policy := indexer.ResourcePolicy{EmbedBatchSize: 2, MaxBatchBytes: 1 << 20}
	sink := &documentSink{client: &recordingIngester{}, kbID: "kb", policy: policy}
	if err := sink.emit(context.Background(), docsOfSize("f1", 1000, 1000, 1000, 1000, 1000)); err != nil {
		t.Fatal(err)
	}
	tail := sink.pending[len(sink.pending):cap(sink.pending)]
	for i, doc := range tail {
		if doc.Text != "" || doc.ID != "" || doc.Metadata != nil {
			t.Fatalf("sent document still reachable at tail index %d: %+v", i, doc)
		}
	}
}

func TestDocumentSinkIngestError(t *testing.T) {
	policy := indexer.ResourcePolicy{EmbedBatchSize: 1, MaxBatchBytes: 1 << 20}
	client := &recordingIngester{failOn: 2}
	sink := &documentSink{client: client, kbID: "kb", policy: policy}
	err := sink.emit(context.Background(), docsOfSize("f1", 10, 10, 10))
	if err == nil {
		err = sink.close(context.Background())
	}
	if err == nil {
		t.Fatal("expected the ingest error to propagate")
	}
	if len(client.batches) != 1 {
		t.Fatalf("sending continued past the failure: %v", client.batches)
	}
}

func TestBuildIndexPlanEmitError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte("package a\n\nfunc A() int { return 1 }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := indexer.NormalizeOptions(indexer.Options{Root: root})
	files, skipped, err := indexer.Scan(context.Background(), root, opts, indexer.DefaultExcludePatterns)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("scan found no files")
	}
	target := directoryTarget(root, indexCLIOptions{})
	want := errors.New("emit failed")
	_, err = buildIndexPlan(
		context.Background(), target, opts, pipelineFingerprint(opts), indexState{}, files, skipped,
		func(context.Context, []indexer.Document) error { return want },
	)
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}
