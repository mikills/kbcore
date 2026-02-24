package kb

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

// DeleteDocs deletes documents from the KB.
func (l *KB) DeleteDocs(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions) error {
	return l.deleteDocs(ctx, kbID, docIDs, opts, false)
}

// DeleteDocsAndUpload uploads after deletion without retry.
func (l *KB) DeleteDocsAndUpload(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions) error {
	return l.DeleteDocsAndUploadWithRetry(ctx, kbID, docIDs, opts, 0)
}

// DeleteDocsAndUploadWithRetry deletes docs and uploads with retry logic.
func (l *KB) DeleteDocsAndUploadWithRetry(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions, maxRetries int) error {
	return runWithUploadRetry(ctx, "delete_docs_upload", maxRetries, l.RetryObserver, func() error {
		return l.deleteDocs(ctx, kbID, docIDs, opts, true)
	})
}

func (l *KB) deleteDocs(ctx context.Context, kbID string, docIDs []string, opts DeleteDocsOptions, upload bool) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kbID cannot be empty")
	}
	if len(docIDs) == 0 {
		return nil
	}

	cleanIDs := make([]string, 0, len(docIDs))
	for _, id := range docIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return fmt.Errorf("doc id cannot be empty")
		}
		cleanIDs = append(cleanIDs, trimmed)
	}

	lock := l.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(l.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	if err := l.ensureMutableShardDBLocked(ctx, kbID, kbDir, dbPath, 0, false); err != nil {
		return err
	}

	db, err := l.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete docs tx: %w", err)
	}

	placeholders := buildInClausePlaceholders(len(cleanIDs))
	args := make([]any, 0, len(cleanIDs))
	for _, id := range cleanIDs {
		args = append(args, id)
	}

	if opts.HardDelete {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM docs WHERE id IN (%s)`, placeholders), args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("hard delete docs: %w", err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM doc_tombstones WHERE doc_id IN (%s)`, placeholders), args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("remove tombstones: %w", err)
		}
	} else {
		stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO doc_tombstones (doc_id, deleted_at) VALUES (?, NOW())`)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("prepare tombstone upsert: %w", err)
		}
		for _, id := range cleanIDs {
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("soft delete doc %q: %w", id, err)
			}
		}
		if err := stmt.Close(); err != nil {
			tx.Rollback()
			return fmt.Errorf("close tombstone stmt: %w", err)
		}
	}

	if err := pruneGraphForDocsTx(ctx, tx, cleanIDs, opts.HardDelete && opts.CleanupGraph); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete docs tx: %w", err)
	}
	policy := normalizeShardingPolicy(l.ShardingPolicy)
	if err := l.postMutationCommit(ctx, db, kbID, upload, policy.TargetShardBytes); err != nil {
		return err
	}

	return nil
}
