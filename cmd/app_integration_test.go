package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	kbduckdb "github.com/mikills/minnow/kb/duckdb"
	"github.com/mikills/minnow/kb/testutil"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queryResultPayload struct {
	Results []kb.QueryResult `json:"results"`
}

type ingestAcceptedPayload struct {
	EventID string `json:"event_id"`
}

func setupAppTest(t *testing.T, embedder kb.Embedder, kbID string) string {
	return setupAppTestWithOptions(t, embedder, kbID)
}

func setupAppTestWithOptions(t *testing.T, embedder kb.Embedder, kbID string, opts ...kb.KBOption) string {
	t.Helper()

	ctx := context.Background()
	bucket := "minnow-" + strings.ReplaceAll(strings.ToLower(kbID), "_", "-")

	s3Mock, err := testutil.StartMockS3(ctx, bucket)
	require.NoError(t, err)
	t.Cleanup(s3Mock.Close)

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	leaseMgr, err := kb.NewRedisWriteLeaseManager(redisClient, "test:lease:")
	require.NoError(t, err)

	cacheDir := filepath.Join(t.TempDir(), "cache")
	blobStore := kb.NewS3BlobStore(s3Mock.Client, s3Mock.Bucket, "")

	kbOpts := []kb.KBOption{
		kb.WithEmbedder(embedder),
		kb.WithWriteLeaseManager(leaseMgr),
		kb.WithWriteLeaseTTL(3 * time.Second),
		kb.WithEventStore(kb.NewInMemoryEventStore()),
		kb.WithEventInbox(kb.NewInMemoryEventInbox()),
	}
	kbOpts = append(kbOpts, opts...)

	loader := kb.NewKB(blobStore, cacheDir, kbOpts...)

	af, err := kbduckdb.NewArtifactFormat(kbduckdb.NewDepsFromKB(loader,
		kbduckdb.WithMemoryLimit("128MB"),
	))
	require.NoError(t, err)
	require.NoError(t, loader.RegisterFormat(af))

	app := NewApp(loader, AppConfig{Address: "127.0.0.1:0"})
	workers := []kb.Worker{
		&kb.DocumentUpsertWorker{KB: loader, ID: "test-document-upsert-worker"},
		&kb.DocumentChunkedWorker{KB: loader, ID: "test-document-chunked-worker"},
		&kb.DocumentPublishWorker{
			KB:        loader,
			ID:        "test-document-publish-embedded-worker",
			KindValue: kb.EventDocumentEmbedded,
		},
		&kb.DocumentPublishWorker{
			KB:        loader,
			ID:        "test-document-publish-graph-worker",
			KindValue: kb.EventDocumentGraphExtracted,
		},
	}
	if loader.MediaStore != nil {
		workers = append(workers, &kb.MediaUploadWorker{KB: loader, ID: "test-media-upload-worker"})
	}
	pools := make([]*kb.WorkerPool, 0, len(workers))
	for _, worker := range workers {
		pool, err := kb.NewWorkerPool(worker, loader.EventStore, loader.EventInbox, kb.WorkerPoolConfig{
			Concurrency:       1,
			MaxAttempts:       3,
			PollInterval:      10 * time.Millisecond,
			VisibilityTimeout: 30 * time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, pool.Start(context.Background()))
		pools = append(pools, pool)
	}
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Stop()
		}
		_ = app.Stop(context.Background())
		_ = app.Wait()
	})

	require.NotEmpty(t, app.Address())
	return "http://" + app.Address()
}

func readFinanceDocument(t *testing.T) string {
	t.Helper()

	financePath := filepath.Join("..", "kb", "testdata", "documents", "finance-aapl-10k.txt")
	financeDoc, err := os.ReadFile(financePath)
	require.NoError(t, err)
	return string(financeDoc)
}

func TestAppRAG(t *testing.T) {
	t.Run("multipart_file_ingest_partial_success", testAppMultipartFileIngestPartialSuccess)
	t.Run("s3_redis", testAppRAGS3Redis)
	t.Run("media_upload_async", testAppMediaUploadAsync)
	t.Run("media_upload_too_large", testAppMediaUploadTooLarge)
	t.Run("operations_rate_limited", testAppMediaUploadRateLimited)
	t.Run("ollama_smoke", testAppRAGOllama)
	t.Run("validation", testAppRAGValidation)
	t.Run("modes_sharded", testAppRAGModesSharded)
	t.Run("query_fields_graph", testAppQueryFieldsGraph)
}

func testAppMultipartFileIngestPartialSuccess(t *testing.T) {
	baseURL := setupAppTestWithOptions(
		t,
		appKeywordEmbedder{},
		"kb-app-file-ingest",
		kb.WithMediaStore(kb.NewInMemoryMediaStore()),
	)
	resp, err := postMultipart(baseURL+"/rag/ingest", func(w *multipart.Writer) error {
		require.NoError(t, w.WriteField(kbIDContextKey, "kb-app-file-ingest"))
		require.NoError(t, w.WriteField("graph_enabled", "false"))
		require.NoError(t, w.WriteField("documents", `[{"id":"inline-1","text":"inline companion document"}]`))
		require.NoError(t, w.WriteField("file_ids", "ferrets-file"))
		require.NoError(t, w.WriteField("file_ids", "broken-file"))
		require.NoError(
			t,
			w.WriteField(
				"file_metadata",
				`{"ferrets-file":{"metadata":{"topic":"ferrets","origin":"upload"}},"broken-file":{}}`,
			),
		)
		part, partErr := w.CreateFormFile("files", "ferrets.txt")
		if partErr != nil {
			return partErr
		}
		if _, partErr = io.Copy(part, strings.NewReader("Ferrets are curious pets that love tunnels and play.")); partErr != nil {
			return partErr
		}
		badHeader := textproto.MIMEHeader{}
		badHeader.Set("Content-Disposition", `form-data; name="files"; filename="broken.pdf"`)
		badHeader.Set("Content-Type", "application/pdf")
		badPart, partErr := w.CreatePart(badHeader)
		if partErr != nil {
			return partErr
		}
		_, partErr = io.Copy(badPart, strings.NewReader("not a real pdf"))
		return partErr
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		require.Failf(t, "unexpected status", "status %d: %s", resp.StatusCode, string(body))
	}
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var payload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	operation, err := waitForOperation(baseURL, payload.EventID)
	require.NoError(t, err)
	stages, _ := operation["stages"].([]any)
	require.NotEmpty(t, stages, "expected operation stages to be populated")
	stageKinds := collectStageKinds(stages)
	for _, want := range []string{
		string(kb.EventDocumentUpsert),
		string(kb.EventDocumentChunked),
		string(kb.EventDocumentEmbedded),
		string(kb.EventKBPublished),
	} {
		assert.Contains(t, stageKinds, want, "expected stage %s in %v", want, stageKinds)
	}
	terminal := operation["terminal"].(map[string]any)
	fileResults, _ := terminal["file_results"].([]any)
	require.Len(t, fileResults, 2)
	for _, item := range fileResults {
		result, ok := item.(map[string]any)
		require.True(t, ok, "file_result must be an object")
		assert.NotEmpty(t, result["file_id"], "file_result must include file_id")
		assert.NotEmpty(t, result["status"], "file_result must include status")
	}
	statuses := make(map[string]string)
	var successMediaID string
	for _, item := range fileResults {
		result := item.(map[string]any)
		statuses[result["file_id"].(string)] = result["status"].(string)
		if result["status"].(string) == "succeeded" {
			require.Equal(t, "ferrets-file", result["file_id"].(string))
			successMediaID, _ = result["media_id"].(string)
		}
	}
	require.Equal(t, "succeeded", statuses["ferrets-file"], "file results: %#v", fileResults)
	require.Equal(t, "failed", statuses["broken-file"], "file results: %#v", fileResults)
	require.NotEmpty(t, successMediaID)

	getResp, err := http.Get(baseURL + "/rag/media/" + successMediaID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var media kb.MediaObject
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&media))
	require.Equal(t, "ferrets", media.Metadata["topic"])

	queryResp, err := postJSON(
		baseURL+"/rag/query",
		map[string]any{kbIDContextKey: "kb-app-file-ingest", "query": "Which pets love tunnels?", "k": 5},
	)
	require.NoError(t, err)
	defer queryResp.Body.Close()
	require.Equal(t, http.StatusOK, queryResp.StatusCode)
	var queryPayload queryResultPayload
	require.NoError(t, json.NewDecoder(queryResp.Body).Decode(&queryPayload))
	require.NotEmpty(t, queryPayload.Results)
	found := false
	for _, result := range queryPayload.Results {
		if len(result.MediaRefs) == 0 {
			continue
		}
		if result.MediaRefs[0].MediaID == successMediaID {
			require.Equal(t, "ferrets", result.MediaRefs[0].Metadata["topic"])
			found = true
			break
		}
	}
	require.True(t, found, "expected at least one result to point back to uploaded media")
}

func testAppMediaUploadAsync(t *testing.T) {
	baseURL := setupAppTestWithOptions(
		t,
		appKeywordEmbedder{},
		"kb-app-media",
		kb.WithMediaStore(kb.NewInMemoryMediaStore()),
	)
	resp, err := postMultipart(baseURL+"/rag/media/upload", func(w *multipart.Writer) error {
		require.NoError(t, w.WriteField(kbIDContextKey, "kb-app-media"))
		require.NoError(t, w.WriteField("content_type", "text/plain"))
		part, partErr := w.CreateFormFile("file", "hello.txt")
		if partErr != nil {
			return partErr
		}
		_, partErr = io.Copy(part, strings.NewReader("hello media world"))
		return partErr
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var payload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.NotEmpty(t, payload.EventID)
	operation, err := waitForOperation(baseURL, payload.EventID)
	require.NoError(t, err)
	stages, _ := operation["stages"].([]any)
	require.NotEmpty(t, stages)
	terminal := operation["terminal"].(map[string]any)
	mediaID, _ := terminal["media_id"].(string)
	require.NotEmpty(t, mediaID)
	getResp, err := http.Get(baseURL + "/rag/media/" + mediaID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var media kb.MediaObject
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&media))
	assert.Equal(t, mediaID, media.ID)
	assert.Equal(t, "kb-app-media", media.KBID)
	// A freshly-uploaded media object lives in pending state until the GC
	// sweep promotes it to active once a doc references it. Either state is
	// a "retrievable" outcome for the upload flow.
	assert.Contains(t, []kb.MediaState{kb.MediaStatePending, kb.MediaStateActive}, media.State,
		"media state should be pending or active, got %q", media.State)
}

func testAppMediaUploadTooLarge(t *testing.T) {
	// MaxMediaBytes defaults to 0 (unlimited) in AppConfig. we need to set
	// an explicit cap to exercise the 413 path.
	baseURL := setupAppTestWithCap(
		t,
		appKeywordEmbedder{},
		"kb-app-media-too-large",
		1024,
		kb.WithMediaStore(kb.NewInMemoryMediaStore()),
	)
	// Build a body that exceeds the 1KB cap.
	body := make([]byte, 4*1024)
	for i := range body {
		body[i] = 'x'
	}
	resp, err := postMultipart(baseURL+"/rag/media/upload", func(w *multipart.Writer) error {
		require.NoError(t, w.WriteField(kbIDContextKey, "kb-app-media-too-large"))
		require.NoError(t, w.WriteField("content_type", "text/plain"))
		part, partErr := w.CreateFormFile("file", "big.bin")
		if partErr != nil {
			return partErr
		}
		_, partErr = io.Copy(part, bytes.NewReader(body))
		return partErr
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func testAppMediaUploadRateLimited(t *testing.T) {
	baseURL := setupAppTestWithOptions(
		t,
		appKeywordEmbedder{},
		"kb-app-ratelimited",
		kb.WithMediaStore(kb.NewInMemoryMediaStore()),
	)
	// Hammer the operations endpoint. Bucket is 20 burst + 10/s refill, so
	// a fast-enough loop of 50+ requests will exceed the bucket and start
	// returning 429.
	var got429 bool
	for range 60 {
		resp, err := http.Get(baseURL + "/rag/operations/does-not-exist")
		require.NoError(t, err)
		status := resp.StatusCode
		resp.Body.Close()
		if status == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}
	require.True(t, got429, "expected to observe at least one 429 after burst")
}

func collectStageKinds(stages []any) []string {
	out := make([]string, 0, len(stages))
	for _, s := range stages {
		if stage, ok := s.(map[string]any); ok {
			if kind, kOK := stage["kind"].(string); kOK {
				out = append(out, kind)
			}
		}
	}
	return out
}

// setupAppTestWithCap wires MaxMediaBytes alongside the usual KB options so
// large-body rejection tests can exercise the 413 path.
func setupAppTestWithCap(
	t *testing.T,
	embedder kb.Embedder,
	kbID string,
	maxMediaBytes int64,
	opts ...kb.KBOption,
) string {
	t.Helper()

	ctx := context.Background()
	bucket := "minnow-" + strings.ReplaceAll(strings.ToLower(kbID), "_", "-")

	s3Mock, err := testutil.StartMockS3(ctx, bucket)
	require.NoError(t, err)
	t.Cleanup(s3Mock.Close)

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	leaseMgr, err := kb.NewRedisWriteLeaseManager(redisClient, "test:lease:")
	require.NoError(t, err)

	cacheDir := filepath.Join(t.TempDir(), "cache")
	blobStore := kb.NewS3BlobStore(s3Mock.Client, s3Mock.Bucket, "")

	kbOpts := []kb.KBOption{
		kb.WithEmbedder(embedder),
		kb.WithWriteLeaseManager(leaseMgr),
		kb.WithWriteLeaseTTL(3 * time.Second),
		kb.WithEventStore(kb.NewInMemoryEventStore()),
		kb.WithEventInbox(kb.NewInMemoryEventInbox()),
	}
	kbOpts = append(kbOpts, opts...)

	loader := kb.NewKB(blobStore, cacheDir, kbOpts...)

	af, err := kbduckdb.NewArtifactFormat(kbduckdb.NewDepsFromKB(loader,
		kbduckdb.WithMemoryLimit("128MB"),
	))
	require.NoError(t, err)
	require.NoError(t, loader.RegisterFormat(af))

	app := NewApp(loader, AppConfig{Address: "127.0.0.1:0", MaxMediaBytes: maxMediaBytes})
	workers := []kb.Worker{
		&kb.DocumentUpsertWorker{KB: loader, ID: "test-document-upsert-worker"},
		&kb.DocumentChunkedWorker{KB: loader, ID: "test-document-chunked-worker"},
		&kb.DocumentPublishWorker{
			KB:        loader,
			ID:        "test-document-publish-embedded-worker",
			KindValue: kb.EventDocumentEmbedded,
		},
		&kb.DocumentPublishWorker{
			KB:        loader,
			ID:        "test-document-publish-graph-worker",
			KindValue: kb.EventDocumentGraphExtracted,
		},
	}
	if loader.MediaStore != nil {
		workers = append(workers, &kb.MediaUploadWorker{KB: loader, ID: "test-media-upload-worker"})
	}
	pools := make([]*kb.WorkerPool, 0, len(workers))
	for _, worker := range workers {
		pool, err := kb.NewWorkerPool(worker, loader.EventStore, loader.EventInbox, kb.WorkerPoolConfig{
			Concurrency:       1,
			MaxAttempts:       3,
			PollInterval:      10 * time.Millisecond,
			VisibilityTimeout: 30 * time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, pool.Start(context.Background()))
		pools = append(pools, pool)
	}
	require.NoError(t, app.Start())
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Stop()
		}
		_ = app.Stop(context.Background())
		_ = app.Wait()
	})

	require.NotEmpty(t, app.Address())
	return "http://" + app.Address()
}

func testAppRAGS3Redis(t *testing.T) {
	embedder := appKeywordEmbedder{}
	kbID := "kb-app-integration"
	baseURL := setupAppTest(t, embedder, kbID)
	financeDoc := readFinanceDocument(t)

	ingestResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		kbIDContextKey:  kbID,
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "finance-aapl-10k", "text": financeDoc},
			{
				"id":   "pasta-guide",
				"text": "Pasta carbonara recipe with parmesan, eggs, pancetta, and black pepper. Boil pasta, whisk egg and cheese, then combine off heat to avoid scrambling.",
			},
		},
		"chunk_size": 450,
	})
	require.NoError(t, err)
	defer ingestResp.Body.Close()
	require.Equal(t, http.StatusAccepted, ingestResp.StatusCode)
	var ingestPayload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(ingestResp.Body).Decode(&ingestPayload))
	require.NotEmpty(t, ingestPayload.EventID)
	operation, waitErr := waitForOperation(baseURL, ingestPayload.EventID)
	require.NoError(t, waitErr)
	stages, _ := operation["stages"].([]any)
	require.NotEmpty(t, stages)

	type queryCase struct {
		name               string
		query              string
		expectFinanceChunk bool
	}

	queryCases := []queryCase{
		{name: "expected_hit", query: "iphone revenue and services growth", expectFinanceChunk: true},
		{name: "expected_miss", query: "pasta carbonara parmesan recipe", expectFinanceChunk: false},
	}

	for _, tc := range queryCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp, err := postJSON(baseURL+"/rag/query", map[string]any{
				kbIDContextKey: kbID,
				"query":        tc.query,
				"k":            5,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var payload queryResultPayload
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			require.NotEmpty(t, payload.Results)

			hasFinanceChunk := false
			for _, item := range payload.Results {
				assert.NotEmpty(t, item.Content)
				if strings.HasPrefix(item.ID, "finance-aapl-10k-chunk-") {
					hasFinanceChunk = true
					break
				}
			}

			if tc.expectFinanceChunk {
				assert.True(t, hasFinanceChunk, "%s expected finance chunk hit", tc.name)
			} else {
				assert.False(t, strings.HasPrefix(payload.Results[0].ID, "finance-aapl-10k-chunk-"), "%s expected top result to miss finance chunks", tc.name)
			}
		})
	}
}

func testAppRAGOllama(t *testing.T) {
	ollamaBaseURL := strings.TrimSpace(os.Getenv("OLLAMA_BASE_URL"))
	if ollamaBaseURL == "" {
		t.Skip("set OLLAMA_BASE_URL to run Ollama-backed app integration test")
	}

	model := strings.TrimSpace(os.Getenv("OLLAMA_MODEL"))
	if model == "" {
		model = "all-minilm"
	}

	embedder := &appOllamaEmbedder{BaseURL: ollamaBaseURL, Model: model}
	vec, err := embedder.Embed(context.Background(), "ping")
	require.NoError(t, err, "Ollama not reachable or model not pulled")
	require.NotEmpty(t, vec, "ollama embedding vector must be non-empty")

	kbID := "kb-app-ollama"
	appBaseURL := setupAppTest(t, embedder, kbID)
	financeDoc := readFinanceDocument(t)

	ingestResp, err := postJSON(appBaseURL+"/rag/ingest", map[string]any{
		kbIDContextKey:  kbID,
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "finance-aapl-10k", "text": financeDoc},
			{
				"id":   "pasta-guide",
				"text": "Pasta carbonara recipe with parmesan, eggs, pancetta, and black pepper. Boil pasta, whisk egg and cheese, then combine off heat to avoid scrambling.",
			},
		},
		"chunk_size": 450,
	})
	require.NoError(t, err)
	defer ingestResp.Body.Close()
	require.Equal(t, http.StatusAccepted, ingestResp.StatusCode)
	var ingestPayload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(ingestResp.Body).Decode(&ingestPayload))
	require.NotEmpty(t, ingestPayload.EventID)
	_, waitErr := waitForOperation(appBaseURL, ingestPayload.EventID)
	require.NoError(t, waitErr)

	resp, err := postJSON(appBaseURL+"/rag/query", map[string]any{
		kbIDContextKey: kbID,
		"query":        "iphone sales and services revenue trends",
		"k":            5,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload queryResultPayload
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.NotEmpty(t, payload.Results)
	assert.NotEmpty(t, payload.Results[0].ID)
	assert.NotEmpty(t, payload.Results[0].Content)
}

func testAppRAGValidation(t *testing.T) {
	baseURL := setupAppTest(t, appKeywordEmbedder{}, "kb-app-validation")

	seedResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		kbIDContextKey:  "kb-app-validation",
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "query-seed", "text": "seed queryable document"},
		},
	})
	require.NoError(t, err)
	defer seedResp.Body.Close()
	require.Equal(t, http.StatusAccepted, seedResp.StatusCode)
	var seedPayload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(seedResp.Body).Decode(&seedPayload))
	_, waitErr := waitForOperation(baseURL, seedPayload.EventID)
	require.NoError(t, waitErr)

	testCases := []struct {
		name           string
		path           string
		body           map[string]any
		expectedStatus int
		expectedError  string
	}{
		{
			name: "ingest_missing_kb_id",
			path: "/rag/ingest",
			body: map[string]any{
				"graph_enabled": false,
				"documents":     []map[string]string{{"id": "doc-1", "text": "hello"}},
			},
			expectedStatus: http.StatusAccepted,
		},
		{
			name: "ingest_missing_graph_enabled",
			path: "/rag/ingest",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"documents":    []map[string]string{{"id": "doc-1", "text": "hello"}},
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph_enabled is required",
		},
		{
			name: "ingest_empty_documents",
			path: "/rag/ingest",
			body: map[string]any{
				kbIDContextKey:  "kb-app-validation",
				"graph_enabled": false,
				"documents":     []map[string]string{},
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "documents are required",
		},
		{
			name: "ingest_graph_enabled_true_unavailable",
			path: "/rag/ingest",
			body: map[string]any{
				kbIDContextKey:  "kb-app-validation",
				"graph_enabled": true,
				"documents":     []map[string]string{{"id": "doc-2", "text": "hello graph"}},
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph extraction is not configured",
		},
		{
			name:           "query_empty_query",
			path:           "/rag/query",
			body:           map[string]any{kbIDContextKey: "kb-app-validation", "query": "", "k": 5},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "query is required",
		},
		{
			name: "query_mode_omitted_defaults_vector",
			path: "/rag/query",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"query":        "seed queryable document",
				"k":            5,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "query_mode_vector_success",
			path: "/rag/query",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"query":        "seed queryable document",
				"k":            5,
				"search_mode":  "vector",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "query_mode_adaptive_success",
			path: "/rag/query",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"query":        "seed queryable document",
				"k":            5,
				"search_mode":  "adaptive",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "query_mode_graph_unavailable",
			path: "/rag/query",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"query":        "seed queryable document",
				"k":            5,
				"search_mode":  "graph",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph query requested but graph data is unavailable",
		},
		{
			name: "query_mode_graph_case_insensitive_unavailable",
			path: "/rag/query",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"query":        "seed queryable document",
				"k":            5,
				"search_mode":  "GRAPH",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "graph query requested but graph data is unavailable",
		},
		{
			name: "query_mode_invalid",
			path: "/rag/query",
			body: map[string]any{
				kbIDContextKey: "kb-app-validation",
				"query":        "seed queryable document",
				"k":            5,
				"search_mode":  "bad-mode",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  `invalid search_mode: "bad-mode" (allowed: vector, graph, adaptive, bm25, hybrid)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, postErr := postJSON(baseURL+tc.path, tc.body)
			require.NoError(t, postErr)
			t.Cleanup(func() { _ = resp.Body.Close() })

			require.Equal(t, tc.expectedStatus, resp.StatusCode)

			if tc.expectedError != "" {
				var payload map[string]string
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, tc.expectedError, payload[errorResponseKey])
			}
		})
	}
}

func testAppRAGModesSharded(t *testing.T) {
	kbID := "kb-app-query-modes-sharded"
	baseURL := setupAppTestWithOptions(t, appKeywordEmbedder{}, kbID,
		kb.WithShardingPolicy(kb.ShardingPolicy{ShardTriggerVectorRows: 1}),
	)

	seedResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		kbIDContextKey:  kbID,
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "query-seed", "text": "seed queryable document"},
		},
	})
	require.NoError(t, err)
	defer seedResp.Body.Close()
	require.Equal(t, http.StatusAccepted, seedResp.StatusCode)
	var seedPayload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(seedResp.Body).Decode(&seedPayload))
	_, waitErr := waitForOperation(baseURL, seedPayload.EventID)
	require.NoError(t, waitErr)

	testCases := []struct {
		name           string
		searchMode     string
		expectedStatus int
		expectedError  string
	}{
		{name: "mode_vector", searchMode: "vector", expectedStatus: http.StatusOK},
		{
			name:           "mode_graph_unavailable",
			searchMode:     "graph",
			expectedStatus: http.StatusBadRequest,
			expectedError:  kb.ErrGraphQueryUnavailable.Error(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				kbIDContextKey: kbID,
				"query":        "seed queryable document",
				"k":            5,
			}
			if tc.searchMode != "" {
				body["search_mode"] = tc.searchMode
			}

			resp, err := postJSON(baseURL+"/rag/query", body)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, tc.expectedStatus, resp.StatusCode)

			if tc.expectedError != "" {
				var payload map[string]string
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, tc.expectedError, payload[errorResponseKey])
			}
		})
	}
}
