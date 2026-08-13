package fileingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/graphbuild"
	"github.com/mikills/minnow/kb/media"
)

type Document struct {
	ID        string
	Text      string
	MediaIDs  []string
	MediaRefs []media.ChunkMediaRef
	Metadata  map[string]any
}

type MediaStore interface {
	Put(ctx context.Context, m media.MediaObject) error
}

type Processor struct {
	BlobStore       Store
	MediaStore      MediaStore
	Now             func() time.Time
	Chunker         Chunker
	ChunkSize       int
	MediaBlobKey    func(kbID, mediaID, filename string) string
	VersionMismatch error
	MediaDuplicate  error
}

type StageError struct {
	Err     error
	Results []Result
}

func (e *StageError) Error() string               { return e.Err.Error() }
func (e *StageError) Unwrap() error               { return e.Err }
func (e *StageError) FileIngestResults() []Result { return append([]Result(nil), e.Results...) }

func (p Processor) Normalize(
	ctx context.Context,
	kbID string,
	sourceDocs []Document,
	sources []Source,
) ([]Document, []Result, []string, []string, error) {
	chunkedDocs, err := p.ChunkDocuments(ctx, sourceDocs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	results := make([]Result, 0, len(sources))
	mediaIDs := CollectMediaIDs(sourceDocs)
	stagedBlobKeys := make([]string, 0, len(sources))
	for _, src := range sources {
		// Every staged source must be reclaimed after this attempt reaches a
		// terminal outcome, including files that fail extraction.
		stagedBlobKeys = append(stagedBlobKeys, src.StagedBlobKey)
		extracted, err := p.ExtractFileSource(ctx, kbID, src)
		if err != nil {
			results = append(results, FailedResult(src, err))
			continue
		}
		results = append(results, extracted.Result)
		mediaIDs = append(mediaIDs, src.MediaID)
		chunkedDocs = append(chunkedDocs, extracted.Documents...)
	}
	if len(chunkedDocs) == 0 && len(sources) > 0 {
		return nil, results, mediaIDs, stagedBlobKeys, &StageError{
			Err:     errors.New("ingest: no ingestible files in request"),
			Results: results,
		}
	}
	return chunkedDocs, results, mediaIDs, stagedBlobKeys, nil
}

type ExtractedFile struct {
	Documents []Document
	Result    Result
}

func (p Processor) ExtractFileSource(ctx context.Context, kbID string, src Source) (*ExtractedFile, error) {
	if p.BlobStore == nil || p.MediaStore == nil {
		return nil, errors.New("ingest worker: media subsystem not configured")
	}
	if err := p.PersistStagedMediaObject(ctx, src.StagedBlobKey, p.mediaObject(kbID, src)); err != nil {
		return nil, err
	}
	tmpPath, cleanup, err := DownloadStagedBlobTemp(ctx, p.BlobStore, src.StagedBlobKey, "minnow-file-ingest-*")
	if err != nil {
		return nil, err
	}
	defer cleanup()
	pages, err := SourceText(ctx, tmpPath, src.ContentType)
	if err != nil {
		return nil, err
	}
	chunked, pageCount, err := ChunkPages(ctx, src, pages, p.chunker())
	if err != nil {
		return nil, err
	}
	docs := make([]Document, 0, len(chunked))
	for _, doc := range chunked {
		docs = append(docs, Document{ID: doc.ID, Text: doc.Text, MediaRefs: doc.MediaRefs, Metadata: doc.Metadata})
	}
	return &ExtractedFile{Documents: docs, Result: SucceededResult(src, pageCount)}, nil
}

func (p Processor) mediaObject(kbID string, src Source) media.MediaObject {
	now := time.Now()
	if p.Now != nil {
		now = p.Now()
	}
	return media.MediaObject{
		ID:              src.MediaID,
		KBID:            kbID,
		Filename:        src.Filename,
		ContentType:     src.ContentType,
		SizeBytes:       src.SizeBytes,
		Checksum:        src.Checksum,
		CreatedAtUnixMs: now.UnixMilli(),
		Metadata:        CloneMap(src.Metadata),
		State:           media.MediaStatePending,
	}
}

func (p Processor) PersistStagedMediaObject(ctx context.Context, stagedBlobKey string, rec media.MediaObject) error {
	tmpPath, cleanup, err := DownloadStagedBlobTemp(ctx, p.BlobStore, stagedBlobKey, "minnow-staged-media-*")
	if err != nil {
		return err
	}
	defer cleanup()
	finalBlobKey := p.MediaBlobKey(rec.KBID, rec.ID, rec.Filename)
	if _, err := p.BlobStore.UploadIfMatch(ctx, finalBlobKey, tmpPath, ""); err != nil &&
		!errors.Is(err, p.VersionMismatch) {
		return fmt.Errorf("upload final media: %w", err)
	}
	rec.BlobKey = finalBlobKey
	if rec.CreatedAtUnixMs == 0 {
		rec.CreatedAtUnixMs = time.Now().UnixMilli()
	}
	if err := p.MediaStore.Put(ctx, rec); err != nil && !errors.Is(err, p.MediaDuplicate) {
		return err
	}
	return nil
}

func (p Processor) ChunkDocuments(ctx context.Context, docs []Document) ([]Document, error) {
	out := make([]Document, 0, len(docs))
	for _, doc := range docs {
		chunks, err := p.chunker().Chunk(ctx, doc.ID, doc.Text)
		if err != nil {
			return nil, err
		}
		for _, chunk := range chunks {
			out = append(
				out,
				Document{
					ID:        chunk.ChunkID,
					Text:      chunk.Text,
					MediaIDs:  doc.MediaIDs,
					MediaRefs: doc.MediaRefs,
					Metadata:  doc.Metadata,
				},
			)
		}
	}
	return out, nil
}

func (p Processor) chunker() Chunker {
	if p.Chunker != nil {
		return p.Chunker
	}
	return passthroughChunker{}
}

type passthroughChunker struct{}

func (passthroughChunker) Chunk(_ context.Context, docID string, text string) ([]graphbuild.Chunk, error) {
	return []graphbuild.Chunk{{DocID: docID, ChunkID: docID, Text: text}}, nil
}

func FailedResult(src Source, err error) Result {
	return Result{
		FileID:      src.FileID,
		DocumentID:  src.DocumentID,
		MediaID:     src.MediaID,
		Filename:    src.Filename,
		ContentType: src.ContentType,
		Status:      "failed",
		Error:       err.Error(),
		Metadata:    CloneMap(src.Metadata),
	}
}

func SucceededResult(src Source, pageCount int) Result {
	return Result{
		FileID:      src.FileID,
		DocumentID:  src.DocumentID,
		MediaID:     src.MediaID,
		Filename:    src.Filename,
		ContentType: src.ContentType,
		Status:      "succeeded",
		PageCount:   pageCount,
		Metadata:    CloneMap(src.Metadata),
	}
}

func CollectMediaIDs(docs []Document) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	visit := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, d := range docs {
		for _, id := range d.MediaIDs {
			visit(id)
		}
		for _, r := range d.MediaRefs {
			visit(r.MediaID)
		}
	}
	return out
}

func DownloadStagedBlobTemp(ctx context.Context, store interface {
	Download(context.Context, string, string) error
}, stagedBlobKey, pattern string) (string, func(), error) {
	tmp, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", nil, err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		return "", nil, err
	}
	cleanup := func() { RemoveDownloadedStagedBlobTemp(tmpPath) }
	if err := store.Download(ctx, stagedBlobKey, tmpPath); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("download staged media: %w", err)
	}
	return tmpPath, cleanup, nil
}

func RemoveDownloadedStagedBlobTemp(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, blobstore.ErrNotFound) {
		slog.Default().Warn("staged blob temp remove failed", "path", path, "error", err)
	}
}
