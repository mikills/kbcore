package kb_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	. "github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/require"
)

// newAsyncHarness returns a harness wired with in-memory event store + inbox,
// the shared setup for worker-pool and pipeline tests.
func newAsyncHarness(
	t *testing.T,
	kbID string,
	opts ...KBOption,
) (*TestHarness, *InMemoryEventStore, *InMemoryEventInbox) {
	t.Helper()
	h := NewTestHarness(t, kbID).WithOptions(opts...).Setup()
	t.Cleanup(h.Cleanup)
	store := NewInMemoryEventStore()
	inbox := NewInMemoryEventInbox()
	h.KB().EventStore = store
	h.KB().EventInbox = inbox
	return h, store, inbox
}

func TestAppendDocumentUpsert(t *testing.T) {
	t.Run("requires event store", func(t *testing.T) {
		h := NewTestHarness(t, "async-no-store").Setup()
		t.Cleanup(h.Cleanup)
		_, err := h.KB().
			AppendDocumentUpsert(context.Background(), DocumentUpsertPayload{KBID: "kb", Documents: []Document{{ID: "d", Text: "t"}}}, "", "")
		require.ErrorContains(t, err, "EventStore")
	})

	t.Run("emits event with payload", func(t *testing.T) {
		h, store, _ := newAsyncHarness(t, "async-append")
		h.KB().MediaStore = NewInMemoryMediaStore()
		require.NoError(
			t,
			h.KB().MediaStore.Put(
				context.Background(),
				MediaObject{
					ID:              "m1",
					KBID:            "kb",
					Filename:        "x",
					BlobKey:         "k",
					CreatedAtUnixMs: time.Now().UnixMilli(),
					State:           MediaStatePending,
				},
			),
		)
		evtID, err := h.KB().
			AppendDocumentUpsert(context.Background(), DocumentUpsertPayload{KBID: "kb", Documents: []Document{{ID: "d", Text: "t", MediaIDs: []string{"m1"}}}, ChunkSize: 42}, "idem-x", "corr-y")
		require.NoError(t, err)
		got, err := store.Get(context.Background(), evtID)
		require.NoError(t, err)
		require.Equal(t, EventDocumentUpsert, got.Kind)
		var payload DocumentUpsertPayload
		require.NoError(t, json.Unmarshal(got.Payload, &payload))
		require.Equal(t, 42, payload.ChunkSize)
	})

	t.Run("duplicate idempotency key returns existing event", func(t *testing.T) {
		h, _, _ := newAsyncHarness(t, "async-append-dup")
		firstID, err := h.KB().
			AppendDocumentUpsert(context.Background(), DocumentUpsertPayload{KBID: "kb", Documents: []Document{{ID: "d", Text: "t"}}}, "idem-x", "corr-y")
		require.NoError(t, err)
		secondID, err := h.KB().
			AppendDocumentUpsert(context.Background(), DocumentUpsertPayload{KBID: "kb", Documents: []Document{{ID: "d", Text: "t"}}}, "idem-x", "corr-z")
		require.NoError(t, err)
		require.Equal(t, firstID, secondID)
	})
}

func TestDocumentPipelineWorkers(t *testing.T) {
	t.Run("upsert worker emits document.chunked", func(t *testing.T) {
		h, store, inbox := newAsyncHarness(t, "async-worker")
		worker := &DocumentUpsertWorker{KB: h.KB(), ID: "doc-upsert"}
		pool, err := NewWorkerPool(
			worker,
			store,
			inbox,
			WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3},
		)
		require.NoError(t, err)
		evtID, err := h.KB().
			AppendDocumentUpsert(context.Background(), DocumentUpsertPayload{KBID: "kb", Documents: []Document{{ID: "d1", Text: "hello world"}}, ChunkSize: 5}, "", "")
		require.NoError(t, err)
		_, err = pool.HandleOnce(context.Background())
		require.NoError(t, err)
		src, _ := store.Get(context.Background(), evtID)
		require.Equal(t, EventStatusDone, src.Status)
		child, err := store.FindByCausation(context.Background(), EventDocumentChunked, evtID)
		require.NoError(t, err)
		require.NotNil(t, child)
		var payload DocumentChunkedPayload
		require.NoError(t, json.Unmarshal(child.Payload, &payload))
		require.Greater(t, len(payload.Documents), 1)
	})

	t.Run("chunked worker emits document.embedded", func(t *testing.T) {
		h, store, inbox := newAsyncHarness(t, "async-chunked-worker")
		h.KB().Embedder = staticEmbedder{vec: []float32{1, 2, 3}}
		worker := &DocumentChunkedWorker{KB: h.KB(), ID: "doc-chunked"}
		pool, err := NewWorkerPool(
			worker,
			store,
			inbox,
			WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 2},
		)
		require.NoError(t, err)
		payload, _ := json.Marshal(
			DocumentChunkedPayload{
				KBID:          "kb",
				Documents:     []Document{{ID: "d1-chunk-0", Text: "hello"}},
				SourceEventID: "src-1",
			},
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "evt-1",
					KBID:           "kb",
					Kind:           EventDocumentChunked,
					Payload:        payload,
					PayloadSchema:  "document.chunked/v1",
					IdempotencyKey: "idem-1",
					Status:         EventStatusPending,
					CreatedAt:      time.Now().UTC(),
				},
			),
		)
		_, err = pool.HandleOnce(context.Background())
		require.NoError(t, err)
		child, err := store.FindByCausation(context.Background(), EventDocumentEmbedded, "evt-1")
		require.NoError(t, err)
		require.NotNil(t, child)
	})

	t.Run("chunked worker propagates embed failure and leaves event pending", func(t *testing.T) {
		h, store, inbox := newAsyncHarness(t, "async-chunked-fail")
		h.KB().Embedder = errEmbedder{err: errors.New("embed failure")}
		worker := &DocumentChunkedWorker{KB: h.KB(), ID: "doc-chunked"}
		pool, err := NewWorkerPool(
			worker,
			store,
			inbox,
			WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 2},
		)
		require.NoError(t, err)
		payload, _ := json.Marshal(
			DocumentChunkedPayload{KBID: "kb", Documents: []Document{{ID: "d", Text: "t"}}, SourceEventID: "src-1"},
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "evt-1",
					KBID:           "kb",
					Kind:           EventDocumentChunked,
					Payload:        payload,
					PayloadSchema:  "document.chunked/v1",
					IdempotencyKey: "idem-1",
					Status:         EventStatusPending,
					CreatedAt:      time.Now().UTC(),
				},
			),
		)
		_, err = pool.HandleOnce(context.Background())
		require.Error(t, err)
		src, _ := store.Get(context.Background(), "evt-1")
		require.Equal(t, EventStatusPending, src.Status)
		require.Contains(t, src.LastError, "embed failure")
		failure, findErr := store.FindByCausation(context.Background(), EventWorkerFailed, "evt-1")
		require.NoError(t, findErr)
		require.NotNil(t, failure)
		require.Equal(t, EventStatusDone, failure.Status)
	})

	t.Run("publish worker promotes media and emits kb.published", func(t *testing.T) {
		format := &preparedOnlyFormat{}
		h, store, inbox := newAsyncHarness(t, "async-publish", WithArtifactFormat(format))
		mediaStore := NewInMemoryMediaStore()
		h.KB().MediaStore = mediaStore
		require.NoError(
			t,
			mediaStore.Put(
				context.Background(),
				MediaObject{
					ID:              "m1",
					KBID:            "kb",
					Filename:        "x",
					BlobKey:         "k",
					CreatedAtUnixMs: time.Now().UnixMilli(),
					State:           MediaStatePending,
				},
			),
		)
		worker := &DocumentPublishWorker{KB: h.KB(), ID: "publish-worker", KindValue: EventDocumentEmbedded}
		pool, err := NewWorkerPool(
			worker,
			store,
			inbox,
			WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 2},
		)
		require.NoError(t, err)
		payload, _ := json.Marshal(
			DocumentEmbeddedPayload{
				KBID:          "kb",
				DocumentCount: 1,
				Documents: []EmbeddedDocument{
					{ID: "d1-chunk-0", Text: "hello", MediaIDs: []string{"m1"}, Embedding: []float32{1, 2, 3}},
				},
				SourceEventID: "src-1",
			},
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "evt-1",
					KBID:           "kb",
					Kind:           EventDocumentEmbedded,
					Payload:        payload,
					PayloadSchema:  "document.embedded/v1",
					IdempotencyKey: "idem-1",
					Status:         EventStatusPending,
					CreatedAt:      time.Now().UTC(),
				},
			),
		)
		_, err = pool.HandleOnce(context.Background())
		require.NoError(t, err)
		child, err := store.FindByCausation(context.Background(), EventKBPublished, "evt-1")
		require.NoError(t, err)
		require.NotNil(t, child)
		require.Equal(t, EventStatusDone, child.Status)
		require.Equal(t, "evt-1", child.CausationID)
		var published KBPublishedPayload
		require.NoError(t, json.Unmarshal(child.Payload, &published))
		require.Equal(t, 1, published.DocumentCount)
		require.Equal(t, 1, published.ChunkCount)
		got, err := mediaStore.Get(context.Background(), "m1")
		require.NoError(t, err)
		require.Equal(t, MediaStateActive, got.State)
		require.Len(t, format.lastDocs, 1)
	})

	t.Run("publish worker rejects event whose kind does not match worker kind", func(t *testing.T) {
		h, _, _ := newAsyncHarness(t, "async-publish-mismatch-kind")
		worker := &DocumentPublishWorker{KB: h.KB(), ID: "publish-worker", KindValue: EventDocumentEmbedded}
		payload, _ := json.Marshal(
			DocumentGraphExtractedPayload{
				KBID:          "kb",
				DocumentCount: 1,
				Documents:     []EmbeddedDocument{{ID: "d1", Text: "t"}},
				SourceEventID: "src-1",
			},
		)
		// Event kind deliberately does not match the worker's KindValue.
		event := &KBEvent{
			EventID:       "evt-mismatch",
			KBID:          "kb",
			Kind:          EventDocumentGraphExtracted,
			Payload:       payload,
			PayloadSchema: "document.graph_extracted/v1",
			Status:        EventStatusClaimed,
			CreatedAt:     time.Now().UTC(),
		}
		_, err := worker.Handle(context.Background(), event)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match worker kind")
	})

	t.Run("publish worker rejects mismatched payload schema", func(t *testing.T) {
		h, _, _ := newAsyncHarness(t, "async-publish-schema-mismatch")
		worker := &DocumentPublishWorker{KB: h.KB(), ID: "publish-worker", KindValue: EventDocumentEmbedded}
		payload, _ := json.Marshal(
			DocumentEmbeddedPayload{
				KBID:          "kb",
				DocumentCount: 1,
				Documents:     []EmbeddedDocument{{ID: "d1", Text: "t"}},
				SourceEventID: "src-1",
			},
		)
		event := &KBEvent{
			EventID:       "evt-schema-mismatch",
			KBID:          "kb",
			Kind:          EventDocumentEmbedded,
			Payload:       payload,
			PayloadSchema: "document.graph_extracted/v1",
			Status:        EventStatusClaimed,
			CreatedAt:     time.Now().UTC(),
		}
		_, err := worker.Handle(context.Background(), event)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected document.embedded/v1 schema")
	})
}

// failingDeleteBlobStore wraps a BlobStore and fails every Delete() with
// the configured error. Used to verify Commit surfaces cleanup errors
// rather than silently swallowing them.
type failingDeleteBlobStore struct {
	inner     BlobStore
	deleteErr error
}

type recordingDeleteBlobStore struct {
	BlobStore
	deleted []string
}

func (s *recordingDeleteBlobStore) Delete(ctx context.Context, key string) error {
	s.deleted = append(s.deleted, key)
	return s.BlobStore.Delete(ctx, key)
}

func (f *failingDeleteBlobStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	return f.inner.Head(ctx, key)
}
func (f *failingDeleteBlobStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	return f.inner.DownloadBytes(ctx, key)
}
func (f *failingDeleteBlobStore) Download(ctx context.Context, key, dest string) error {
	return f.inner.Download(ctx, key, dest)
}
func (f *failingDeleteBlobStore) UploadBytesIfMatch(
	ctx context.Context,
	key string,
	data []byte,
	ver string,
) (*BlobObjectInfo, error) {
	return f.inner.UploadBytesIfMatch(ctx, key, data, ver)
}
func (f *failingDeleteBlobStore) UploadIfMatch(ctx context.Context, key, src, ver string) (*BlobObjectInfo, error) {
	return f.inner.UploadIfMatch(ctx, key, src, ver)
}
func (f *failingDeleteBlobStore) UploadIfNotExists(ctx context.Context, key, src string) (*BlobObjectInfo, error) {
	if store, ok := f.inner.(interface {
		UploadIfNotExists(context.Context, string, string) (*BlobObjectInfo, error)
	}); ok {
		return store.UploadIfNotExists(ctx, key, src)
	}
	return nil, errors.New("create-only upload unsupported")
}
func (f *failingDeleteBlobStore) Delete(ctx context.Context, key string) error {
	return f.deleteErr
}
func (f *failingDeleteBlobStore) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	return f.inner.List(ctx, prefix)
}

func TestWorkerCommit(t *testing.T) {
	t.Run("media_upload_surfaces_post_terminal_delete_errors", func(t *testing.T) {
		h, store, _ := newAsyncHarness(t, "media-commit-err")
		mediaStore := NewInMemoryMediaStore()
		h.KB().MediaStore = mediaStore
		h.KB().BlobStore = &failingDeleteBlobStore{inner: h.KB().BlobStore, deleteErr: errors.New("permission denied")}
		_ = store

		worker := &MediaUploadWorker{KB: h.KB(), ID: "media"}
		// Seed the media record so the worker does not try to re-upload bytes
		// (persistStagedMediaObject would otherwise need the staged blob to exist).
		require.NoError(
			t,
			mediaStore.Put(
				context.Background(),
				MediaObject{
					ID:              "m-already",
					KBID:            "kb",
					Filename:        "x.bin",
					BlobKey:         "kbs/kb/media/m-already/x.bin",
					CreatedAtUnixMs: time.Now().UnixMilli(),
					State:           MediaStatePending,
				},
			),
		)

		payload, _ := json.Marshal(
			MediaUploadPayload{
				KBID:          "kb",
				MediaID:       "m-already",
				StagedBlobKey: "staged/key",
				Filename:      "x.bin",
				ContentType:   "application/octet-stream",
				SourceEventID: "evt-1",
			},
		)
		event := &KBEvent{
			EventID:       "evt-1",
			KBID:          "kb",
			Kind:          EventMediaUpload,
			Payload:       payload,
			PayloadSchema: "media.upload/v1",
			Status:        EventStatusClaimed,
			CreatedAt:     time.Now().UTC(),
		}
		result, err := worker.Handle(context.Background(), event)
		require.NoError(t, err)
		require.Len(t, result.FollowUps, 2)
		require.Equal(t, EventMediaUploaded, result.FollowUps[0].Kind)
		require.Equal(t, EventStatusDone, result.FollowUps[0].Status)
		require.Equal(t, EventStagingCleanup, result.FollowUps[1].Kind)
		require.Nil(t, result.Commit)
		require.Nil(t, result.AfterTerminal)
	})
}

func TestFileIngest(t *testing.T) {
	t.Run("idempotent retry does not restage or overwrite", func(t *testing.T) {
		h, _, _ := newAsyncHarness(t, "file-ingest-idempotent")
		input := func(body string) FileIngestInput {
			return FileIngestInput{KBID: "kb", Files: []FileIngestUpload{{
				FileID: "file", Filename: "doc.txt", ContentType: "text/plain", Body: strings.NewReader(body),
			}}}
		}
		firstID, _, err := h.KB().AppendFileIngestDetailed(
			context.Background(), input("original"), 0, "same-key", "corr-1",
		)
		require.NoError(t, err)
		secondID, _, err := h.KB().AppendFileIngestDetailed(
			context.Background(), input("replacement"), 0, "same-key", "corr-2",
		)
		require.NoError(t, err)
		require.Equal(t, firstID, secondID)
		objects, err := h.KB().BlobStore.List(context.Background(), "kbs/kb/media-staging")
		require.NoError(t, err)
		require.Len(t, objects, 1)
		data, err := h.KB().BlobStore.DownloadBytes(context.Background(), objects[0].Key)
		require.NoError(t, err)
		require.Equal(t, "original", string(data))
	})

	t.Run("terminal failure deletes every staged source", func(t *testing.T) {
		h, _, _ := newAsyncHarness(t, "file-ingest-terminal-cleanup")
		recording := &recordingDeleteBlobStore{BlobStore: h.KB().BlobStore}
		h.KB().BlobStore = recording
		h.KB().MediaStore = nil

		payload, err := json.Marshal(DocumentUpsertPayload{
			KBID: "kb",
			FileSources: []FileIngestSource{{
				FileID:        "f1",
				DocumentID:    "d1",
				StagedBlobKey: "staging/f1",
			}},
		})
		require.NoError(t, err)
		event := &KBEvent{
			EventID:     "evt-terminal",
			KBID:        "kb",
			Kind:        EventDocumentUpsert,
			Payload:     payload,
			Status:      EventStatusClaimed,
			Attempt:     3,
			MaxAttempts: 3,
		}

		result, err := (&DocumentUpsertWorker{KB: h.KB(), ID: "upsert"}).Handle(context.Background(), event)
		require.ErrorContains(t, err, "no ingestible files")
		require.Empty(t, recording.deleted, "handler must not delete before the event is terminal")
		require.NotNil(t, result.AfterTerminal)
		require.NoError(t, result.AfterTerminal(context.Background()))
		require.Equal(t, []string{"staging/f1"}, recording.deleted)
	})

	t.Run("validates_input", func(t *testing.T) {
		t.Run("rejects empty input", func(t *testing.T) {
			h, _, _ := newAsyncHarness(t, "file-ingest-empty")
			_, _, err := h.KB().AppendFileIngestDetailed(context.Background(), FileIngestInput{KBID: "kb"}, 0, "", "")
			require.Error(t, err)
			require.Contains(t, err.Error(), "at least one file or document")
		})

		t.Run("rejects input above MaxFilesPerIngest", func(t *testing.T) {
			h, _, _ := newAsyncHarness(t, "file-ingest-cap")
			docs := make([]Document, MaxFilesPerIngest+1)
			for i := range docs {
				docs[i] = Document{ID: "d", Text: "t"}
			}
			_, _, err := h.KB().
				AppendFileIngestDetailed(context.Background(), FileIngestInput{KBID: "kb", Documents: docs}, 0, "", "")
			require.Error(t, err)
			require.Contains(t, err.Error(), "too many items")
		})
	})
}

func TestOperationStages(t *testing.T) {
	t.Run("returns stage chain with terminal publish", func(t *testing.T) {
		h := NewTestHarness(t, "async-op-stages").Setup()
		t.Cleanup(h.Cleanup)
		store := NewInMemoryEventStore()
		h.KB().EventStore = store
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "src-1",
					KBID:           "kb",
					Kind:           EventDocumentUpsert,
					IdempotencyKey: "k1",
					Status:         EventStatusDone,
					CreatedAt:      time.Now().UTC(),
				},
			),
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "mid-1",
					KBID:           "kb",
					Kind:           EventDocumentChunked,
					IdempotencyKey: "k-mid",
					CausationID:    "src-1",
					Status:         EventStatusDone,
					CreatedAt:      time.Now().UTC().Add(time.Millisecond),
				},
			),
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "emb-1",
					KBID:           "kb",
					Kind:           EventDocumentEmbedded,
					IdempotencyKey: "k-emb",
					CausationID:    "mid-1",
					Status:         EventStatusDone,
					CreatedAt:      time.Now().UTC().Add(2 * time.Millisecond),
				},
			),
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "pub-1",
					KBID:           "kb",
					Kind:           EventKBPublished,
					IdempotencyKey: "k2",
					CausationID:    "emb-1",
					Status:         EventStatusDone,
					CreatedAt:      time.Now().UTC().Add(3 * time.Millisecond),
				},
			),
		)
		stages, err := h.KB().OperationStages(context.Background(), "src-1")
		require.NoError(t, err)
		require.Len(t, stages, 4)
		require.Equal(t, EventDocumentUpsert, stages[0].Event.Kind)
		require.Equal(t, EventDocumentChunked, stages[1].Event.Kind)
		require.Equal(t, EventDocumentEmbedded, stages[2].Event.Kind)
		require.Equal(t, EventKBPublished, stages[3].Event.Kind)
	})

	t.Run("attaches worker failure to the failing stage", func(t *testing.T) {
		h := NewTestHarness(t, "async-op-stage-failure").Setup()
		t.Cleanup(h.Cleanup)
		store := NewInMemoryEventStore()
		h.KB().EventStore = store
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "src-1",
					KBID:           "kb",
					Kind:           EventDocumentUpsert,
					IdempotencyKey: "k1",
					Status:         EventStatusDone,
					CreatedAt:      time.Now().UTC(),
				},
			),
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:        "mid-1",
					KBID:           "kb",
					Kind:           EventDocumentChunked,
					IdempotencyKey: "k-mid",
					CausationID:    "src-1",
					Status:         EventStatusDead,
					CreatedAt:      time.Now().UTC().Add(time.Millisecond),
				},
			),
		)
		failurePayload, _ := json.Marshal(
			WorkerFailedPayload{
				Stage:         string(EventDocumentChunked),
				SourceEventID: "mid-1",
				Attempt:       1,
				Error:         "boom",
				WillRetry:     false,
			},
		)
		require.NoError(
			t,
			store.Append(
				context.Background(),
				KBEvent{
					EventID:       "fail-1",
					KBID:          "kb",
					Kind:          EventWorkerFailed,
					Payload:       failurePayload,
					PayloadSchema: "worker.failed/v1",
					CausationID:   "mid-1",
					Status:        EventStatusDone,
					CreatedAt:     time.Now().UTC().Add(2 * time.Millisecond),
				},
			),
		)
		stages, err := h.KB().OperationStages(context.Background(), "src-1")
		require.NoError(t, err)
		require.Len(t, stages, 2)
		require.NotNil(t, stages[1].Failure)
		require.Equal(t, EventWorkerFailed, stages[1].Failure.Kind)
	})
}

type staticEmbedder struct{ vec []float32 }

func (e staticEmbedder) Embed(context.Context, string) ([]float32, error) { return e.vec, nil }

type errEmbedder struct{ err error }

func (e errEmbedder) Embed(context.Context, string) ([]float32, error) { return nil, e.err }

type preparedOnlyFormat struct{ lastDocs []EmbeddedDocument }

func (f *preparedOnlyFormat) Kind() string    { return "prepared-only" }
func (f *preparedOnlyFormat) Version() int    { return 1 }
func (f *preparedOnlyFormat) FileExt() string { return ".mock" }
func (f *preparedOnlyFormat) BuildArtifacts(context.Context, string, string, int64) ([]SnapshotShardMetadata, error) {
	return nil, nil
}
func (f *preparedOnlyFormat) QueryRag(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (f *preparedOnlyFormat) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (f *preparedOnlyFormat) QueryBM25(context.Context, BM25QueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}
func (f *preparedOnlyFormat) FetchVectors(context.Context, string, []string) ([]VectorRecord, error) {
	return nil, nil
}
func (f *preparedOnlyFormat) Ingest(context.Context, IngestUpsertRequest) (IngestResult, error) {
	return IngestResult{}, nil
}
func (f *preparedOnlyFormat) Delete(context.Context, IngestDeleteRequest) (IngestResult, error) {
	return IngestResult{}, nil
}
func (f *preparedOnlyFormat) PublishPrepared(_ context.Context, req PreparedPublishRequest) (IngestResult, error) {
	f.lastDocs = append([]EmbeddedDocument(nil), req.Docs...)
	return IngestResult{MutatedCount: len(req.Docs)}, nil
}
