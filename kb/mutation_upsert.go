package kb

import "context"

type DeleteDocsOptions struct {
	HardDelete   bool
	CleanupGraph bool
	DeferPublish bool
}

type UpsertDocsOptions struct {
	GraphEnabled *bool
	// Publishing rebuilds every index, so a bulk load pays it once.
	DeferPublish bool
}

func (l *KB) UpsertDocs(ctx context.Context, kbID string, docs []Document) error {
	if err := l.ValidateDocumentReferences(ctx, kbID, docs); err != nil {
		return err
	}
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return err
	}
	_, err = format.Ingest(
		ctx,
		IngestUpsertRequest{KBID: kbID, Docs: docs, Upload: false, Options: UpsertDocsOptions{}},
	)
	return err
}

func (l *KB) UpsertDocsAndUpload(ctx context.Context, kbID string, docs []Document) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, 0, UpsertDocsOptions{})
}

func (l *KB) UpsertDocsAndUploadWithOptions(
	ctx context.Context,
	kbID string,
	docs []Document,
	opts UpsertDocsOptions,
) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, 0, opts)
}

func (l *KB) UpsertDocsAndUploadWithRetry(ctx context.Context, kbID string, docs []Document, maxRetries int) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, maxRetries, UpsertDocsOptions{})
}

func (l *KB) UpsertDocsAndUploadWithRetryAndOptions(
	ctx context.Context,
	kbID string,
	docs []Document,
	maxRetries int,
	opts UpsertDocsOptions,
) error {
	return runWithUploadRetry(ctx, "upsert_docs_upload", maxRetries, l.RetryObserver, func() error {
		format, err := l.resolveFormat(ctx, kbID)
		if err != nil {
			return err
		}

		_, err = format.Ingest(ctx, IngestUpsertRequest{
			KBID: kbID, Docs: docs, Upload: !opts.DeferPublish, Options: opts,
		})
		return err
	})
}

func (l *KB) PublishPreparedDocs(
	ctx context.Context,
	kbID string,
	docs []EmbeddedDocument,
	graphResult *GraphBuildResult,
	opts UpsertDocsOptions,
) error {
	return l.publishPreparedDocs(
		ctx,
		PreparedPublishRequest{
			KBID: kbID, Docs: docs, GraphResult: graphResult, Options: opts, Upload: !opts.DeferPublish,
		},
	)
}

func (l *KB) publishPreparedDocs(ctx context.Context, req PreparedPublishRequest) error {
	format, err := l.resolveFormat(ctx, req.KBID)
	if err != nil {
		return err
	}
	publisher, ok := format.(PreparedArtifactPublisher)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	_, err = publisher.PublishPrepared(ctx, req)
	return err
}

// CommitPreparedDocs publishes writes accumulated by DeferPublish.
func (l *KB) CommitPreparedDocs(ctx context.Context, kbID string) error {
	return l.commitPreparedDocs(ctx, kbID)
}

func (l *KB) commitPreparedDocs(ctx context.Context, kbID string) error {
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return err
	}
	committer, ok := format.(PreparedArtifactCommitter)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	return committer.CommitPrepared(ctx, kbID)
}

func (l *KB) publishPreparedStream(ctx context.Context, req PreparedStreamRequest) error {
	format, err := l.resolveFormat(ctx, req.KBID)
	if err != nil {
		return err
	}
	streamer, ok := format.(PreparedArtifactStreamer)
	if !ok {
		return ErrArtifactFormatNotConfigured
	}
	_, err = streamer.PublishPreparedStream(ctx, req)
	return err
}

func (l *KB) DeleteDocs(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions) error {
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return err
	}

	_, err = format.Delete(ctx, IngestDeleteRequest{KBID: kbID, DocIDs: docIDs, Upload: false, Options: opts})

	return err
}

func (l *KB) DeleteDocsAndUpload(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions) error {
	return l.DeleteDocsAndUploadWithRetry(ctx, kbID, docIDs, opts, 0)
}

func (l *KB) DeleteDocsAndUploadWithRetry(
	ctx context.Context,
	kbID string,
	docIDs []string,
	opts DeleteDocsOptions,
	maxRetries int,
) error {
	return runWithUploadRetry(ctx, "delete_docs_upload", maxRetries, l.RetryObserver, func() error {
		format, err := l.resolveFormat(ctx, kbID)
		if err != nil {
			return err
		}

		_, err = format.Delete(ctx, IngestDeleteRequest{
			KBID: kbID, DocIDs: docIDs, Upload: !opts.DeferPublish, Options: opts,
		})
		return err
	})
}
