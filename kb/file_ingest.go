package kb

import (
	"context"
	"errors"

	"github.com/mikills/minnow/kb/fileingest"
)

type fileResultCarrier interface {
	FileIngestResults() []FileIngestResult
}

type fileIngestStageError struct {
	err         error
	fileResults []FileIngestResult
}

func (e *fileIngestStageError) Error() string { return e.err.Error() }
func (e *fileIngestStageError) Unwrap() error { return e.err }
func (e *fileIngestStageError) FileIngestResults() []FileIngestResult {
	return append([]FileIngestResult(nil), e.fileResults...)
}

// MaxFilesPerIngest caps the combined file + document count accepted by
// AppendFileIngestDetailed in a single request.
const MaxFilesPerIngest = 1000

func (l *KB) AppendFileIngestDetailed(
	ctx context.Context,
	input FileIngestInput,
	maxBytes int64,
	idempotencyKey, correlationID string,
) (string, string, error) {
	if err := l.validateFileIngestInput(input); err != nil {
		return "", "", err
	}
	if l.EventStore == nil {
		return "", "", errors.New("ingest: EventStore not configured")
	}
	idempotencyKey, correlationID = l.ensureEventKeys(idempotencyKey, correlationID)
	if existing, err := l.EventStore.FindByIdempotency(ctx, EventDocumentUpsert, input.KBID, idempotencyKey); err != nil {
		return "", "", err
	} else if existing != nil {
		return existing.EventID, idempotencyKey, nil
	}
	staged, cleanupAll, err := l.stageFileIngestUploads(ctx, input, maxBytes)
	if err != nil {
		return "", "", err
	}
	payload := fileIngestUpsertPayload(input, staged)
	eventID, effectiveIdem, created, err := l.appendDocumentUpsertDetailed(
		ctx, payload, idempotencyKey, correlationID,
	)
	if err != nil || !created {
		cleanupAll()
	}
	if err != nil {
		return "", "", err
	}
	return eventID, effectiveIdem, nil
}

func (l *KB) validateFileIngestInput(input FileIngestInput) error {
	return fileingest.ValidateInput(l.BlobStore, len(input.Files), len(input.Documents))
}

func (l *KB) stageFileIngestUploads(
	ctx context.Context,
	input FileIngestInput,
	maxBytes int64,
) ([]fileingest.StagedUpload, func(), error) {
	return fileingest.StageUploads(ctx, fileingest.StageInput{
		Store:          l.BlobStore,
		KBID:           input.KBID,
		Files:          input.Files,
		DocumentCount:  len(input.Documents),
		MaxBytes:       maxBytes,
		MediaID:        l.newMediaID,
		StagingBlobKey: mediaStagingBlobKey,
		MaxUploadBytes: DefaultMaxUploadBytes,
	})
}

func fileIngestUpsertPayload(input FileIngestInput, staged []fileingest.StagedUpload) DocumentUpsertPayload {
	payload := DocumentUpsertPayload{
		KBID:      input.KBID,
		Documents: input.Documents,
		ChunkSize: input.ChunkSize,
		Options:   input.Options,
	}
	for _, s := range staged {
		payload.FileSources = append(payload.FileSources, s.Source)
	}
	return payload
}

func (w *DocumentUpsertWorker) normalizeDocuments(
	ctx context.Context,
	kbID string,
	sourceDocs []Document,
	fileSources []FileIngestSource,
	chunkSize int,
) ([]Document, []FileIngestResult, []string, []string, error) {
	processor := fileingest.Processor{
		BlobStore:       w.KB.BlobStore,
		MediaStore:      w.KB.MediaStore,
		Now:             w.KB.Clock.Now,
		Chunker:         TextChunker{ChunkSize: normalizedFileChunkSize(chunkSize)},
		MediaBlobKey:    MediaBlobKey,
		VersionMismatch: ErrBlobVersionMismatch,
		MediaDuplicate:  ErrMediaDuplicateKey,
	}
	docs, results, mediaIDs, stagedKeys, err := processor.Normalize(
		ctx,
		kbID,
		fileIngestDocuments(sourceDocs),
		fileSources,
	)
	return kbDocuments(docs), results, mediaIDs, stagedKeys, convertFileStageError(err)
}

func normalizedFileChunkSize(chunkSize int) int {
	if chunkSize <= 0 {
		return DefaultTextChunkSize
	}
	return chunkSize
}

func fileIngestDocuments(docs []Document) []fileingest.Document {
	out := make([]fileingest.Document, 0, len(docs))
	for _, doc := range docs {
		out = append(
			out,
			fileingest.Document{
				ID:        doc.ID,
				Text:      doc.Text,
				MediaIDs:  doc.MediaIDs,
				MediaRefs: doc.MediaRefs,
				Metadata:  doc.Metadata,
			},
		)
	}
	return out
}

func kbDocuments(docs []fileingest.Document) []Document {
	out := make([]Document, 0, len(docs))
	for _, doc := range docs {
		out = append(
			out,
			Document{
				ID:        doc.ID,
				Text:      doc.Text,
				MediaIDs:  doc.MediaIDs,
				MediaRefs: doc.MediaRefs,
				Metadata:  doc.Metadata,
			},
		)
	}
	return out
}

func (l *KB) persistStagedMediaObject(ctx context.Context, stagedBlobKey string, rec MediaObject) error {
	return fileingest.Processor{
		BlobStore:       l.BlobStore,
		MediaStore:      l.MediaStore,
		Now:             l.Clock.Now,
		MediaBlobKey:    MediaBlobKey,
		VersionMismatch: ErrBlobVersionMismatch,
		MediaDuplicate:  ErrMediaDuplicateKey,
	}.PersistStagedMediaObject(ctx, stagedBlobKey, rec)
}

func convertFileStageError(err error) error {
	if err == nil {
		return nil
	}
	var stageErr *fileingest.StageError
	if errors.As(err, &stageErr) {
		return &fileIngestStageError{err: stageErr.Err, fileResults: stageErr.Results}
	}
	return err
}
