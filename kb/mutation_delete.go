package kb

import "context"

// DeleteDocs deletes documents from the KB.
func (l *KB) DeleteDocs(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions) error {
	if l.ArtifactFormat == nil {
		return ErrArtifactFormatNotConfigured
	}
	_, err := l.ArtifactFormat.Delete(ctx, IngestDeleteRequest{KBID: kbID, DocIDs: docIDs, Upload: false, Options: opts})
	return err
}

// DeleteDocsAndUpload uploads after deletion without retry.
func (l *KB) DeleteDocsAndUpload(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions) error {
	return l.DeleteDocsAndUploadWithRetry(ctx, kbID, docIDs, opts, 0)
}

// DeleteDocsAndUploadWithRetry deletes docs and uploads with retry logic.
func (l *KB) DeleteDocsAndUploadWithRetry(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions, maxRetries int) error {
	return runWithUploadRetry(ctx, "delete_docs_upload", maxRetries, l.RetryObserver, func() error {
		if l.ArtifactFormat == nil {
			return ErrArtifactFormatNotConfigured
		}
		_, err := l.ArtifactFormat.Delete(ctx, IngestDeleteRequest{KBID: kbID, DocIDs: docIDs, Upload: true, Options: opts})
		return err
	})
}
