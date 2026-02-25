package kb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// DeleteDocsOptions controls the behavior of DeleteDocs.
type DeleteDocsOptions struct {
	HardDelete   bool
	CleanupGraph bool
}

// UpsertDocsOptions controls per-call upsert behavior.
type UpsertDocsOptions struct {
	// GraphEnabled overrides graph extraction for this upsert call when set.
	// nil keeps default behavior (graph extraction follows configured GraphBuilder).
	GraphEnabled *bool
}

// UpsertDocs inserts or updates documents in the KB.
func (l *KB) UpsertDocs(ctx context.Context, kbID string, docs []Document) error {
	return l.upsertDocs(ctx, kbID, docs, false, UpsertDocsOptions{})
}

// UpsertDocsAndUpload uploads after upsert without retry.
func (l *KB) UpsertDocsAndUpload(ctx context.Context, kbID string, docs []Document) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, 0, UpsertDocsOptions{})
}

// UpsertDocsAndUploadWithOptions uploads after upsert without retry.
func (l *KB) UpsertDocsAndUploadWithOptions(ctx context.Context, kbID string, docs []Document, opts UpsertDocsOptions) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, 0, opts)
}

// UpsertDocsAndUploadWithRetry upserts docs and uploads with retry logic.
func (l *KB) UpsertDocsAndUploadWithRetry(ctx context.Context, kbID string, docs []Document, maxRetries int) error {
	return l.UpsertDocsAndUploadWithRetryAndOptions(ctx, kbID, docs, maxRetries, UpsertDocsOptions{})
}

// UpsertDocsAndUploadWithRetryAndOptions upserts docs and uploads with retry logic.
func (l *KB) UpsertDocsAndUploadWithRetryAndOptions(ctx context.Context, kbID string, docs []Document, maxRetries int, opts UpsertDocsOptions) error {
	return runWithUploadRetry(ctx, "upsert_docs_upload", maxRetries, l.RetryObserver, func() error {
		return l.upsertDocs(ctx, kbID, docs, true, opts)
	})
}

func (l *KB) upsertDocs(ctx context.Context, kbID string, docs []Document, upload bool, opts UpsertDocsOptions) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kbID cannot be empty")
	}
	if len(docs) == 0 {
		return nil
	}
	if l.Embedder == nil {
		return fmt.Errorf("embedder is not configured")
	}

	graphBuilder := l.GraphBuilder
	if opts.GraphEnabled != nil {
		if *opts.GraphEnabled {
			if graphBuilder == nil {
				return ErrGraphUnavailable
			}
		} else {
			graphBuilder = nil
		}
	}

	lock := l.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(l.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	seedVec, err := l.Embed(ctx, docs[0].Text)
	if err != nil {
		return fmt.Errorf("embed doc %q for shard bootstrap: %w", docs[0].ID, err)
	}
	if len(seedVec) == 0 {
		return fmt.Errorf("embed doc %q for shard bootstrap: empty embedding", docs[0].ID)
	}
	if err := l.ensureMutableShardDBLocked(ctx, kbID, kbDir, dbPath, len(seedVec), true); err != nil {
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

	var graphResult *GraphBuildResult
	if graphBuilder != nil {
		graphResult, err = graphBuilder.Build(ctx, docs)
		if err != nil {
			return fmt.Errorf("build graph for upsert docs: %w", err)
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}

	stmtUndelete, err := tx.PrepareContext(ctx, `DELETE FROM doc_tombstones WHERE doc_id = ?`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare doc_tombstones delete: %w", err)
	}
	defer stmtUndelete.Close()

	for _, doc := range docs {
		if strings.TrimSpace(doc.ID) == "" {
			tx.Rollback()
			return fmt.Errorf("doc id cannot be empty")
		}
		if strings.TrimSpace(doc.Text) == "" {
			tx.Rollback()
			return fmt.Errorf("doc %q text cannot be empty", doc.ID)
		}

		vec, err := l.Embed(ctx, doc.Text)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("embed doc %q: %w", doc.ID, err)
		}
		if len(vec) == 0 {
			tx.Rollback()
			return fmt.Errorf("embed doc %q: empty embedding", doc.ID)
		}

		vecStr := formatVectorForSQL(vec)
		if _, err := tx.ExecContext(ctx, `DELETE FROM docs WHERE id = ?`, doc.ID); err != nil {
			tx.Rollback()
			return fmt.Errorf("delete existing doc %q before upsert: %w", doc.ID, err)
		}
		upsertSQL := fmt.Sprintf(`INSERT INTO docs (id, content, embedding) VALUES (?, ?, %s::FLOAT[%d])`, vecStr, len(vec))
		if _, err := tx.ExecContext(ctx, upsertSQL, doc.ID, doc.Text); err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert doc %q: %w", doc.ID, err)
		}
		if _, err := stmtUndelete.ExecContext(ctx, doc.ID); err != nil {
			tx.Rollback()
			return fmt.Errorf("clear tombstone for doc %q: %w", doc.ID, err)
		}
	}

	if graphResult != nil {
		docIDs := make([]string, 0, len(docs))
		for _, d := range docs {
			docIDs = append(docIDs, d.ID)
		}
		if err := pruneGraphForDocsTx(ctx, tx, docIDs, true); err != nil {
			tx.Rollback()
			return err
		}
		if err := InsertGraphBuildResultTx(ctx, tx, graphResult); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert tx: %w", err)
	}
	activeDocs, err := activeDocCount(ctx, db)
	if err != nil {
		return err
	}
	snapshotInfo, statErr := os.Stat(dbPath)
	snapshotBytes := int64(0)
	if statErr == nil {
		snapshotBytes = snapshotInfo.Size()
	}
	shardingActive := shouldActivateSharding(l.ShardingPolicy, snapshotBytes, int64(activeDocs))
	policy := normalizeShardingPolicy(l.ShardingPolicy)
	rotationReason := shardingActivationReason(l.ShardingPolicy, snapshotBytes, int64(activeDocs))
	if !shardingActive {
		rotationReason = "within_rotation_threshold"
	}
	if upload {
		slog.Default().InfoContext(
			ctx,
			"evaluated shard publish",
			"kb_id", kbID,
			"reason", rotationReason,
			"snapshot_bytes", snapshotBytes,
			"vector_rows", activeDocs,
		)
	}
	if err := l.postMutationCommit(ctx, db, kbID, upload, policy.TargetShardBytes); err != nil {
		return err
	}
	if upload {
		if err := l.cleanupLegacySnapshotObjectsBestEffort(ctx, kbID); err != nil {
			return err
		}
	}

	return nil
}

func (l *KB) cleanupLegacySnapshotObjectsBestEffort(ctx context.Context, kbID string) error {
	legacyKeys := []string{
		kbID + ".duckdb",
		kbID + ".snapshot.json",
	}
	for _, key := range legacyKeys {
		err := l.BlobStore.Delete(ctx, key)
		if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			continue
		}
		slog.Default().WarnContext(ctx, "failed to delete legacy snapshot object", "kb_id", kbID, "key", key, "error", err)
	}
	return nil
}

func (l *KB) shouldActivateShardingForUpsert(dbPath string, vectorRows int64) (bool, error) {
	info, err := os.Stat(dbPath)
	if err != nil {
		return false, err
	}
	return shouldActivateSharding(l.ShardingPolicy, info.Size(), vectorRows), nil
}

func shouldActivateSharding(policy ShardingPolicy, snapshotBytes int64, vectorRows int64) bool {
	resolved := normalizeShardingPolicy(policy)
	if snapshotBytes >= resolved.ShardTriggerBytes {
		return true
	}
	if vectorRows >= int64(resolved.ShardTriggerVectorRows) {
		return true
	}
	return false
}

func shardingActivationReason(policy ShardingPolicy, snapshotBytes int64, vectorRows int64) string {
	resolved := normalizeShardingPolicy(policy)
	switch {
	case snapshotBytes >= resolved.ShardTriggerBytes && vectorRows >= int64(resolved.ShardTriggerVectorRows):
		return "bytes_and_vector_rows"
	case snapshotBytes >= resolved.ShardTriggerBytes:
		return "snapshot_bytes"
	case vectorRows >= int64(resolved.ShardTriggerVectorRows):
		return "vector_rows"
	default:
		return "within_threshold"
	}
}

func (l *KB) postMutationCommit(ctx context.Context, db *sql.DB, kbID string, upload bool, targetShardBytes int64) error {
	kbDir := filepath.Join(l.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")

	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	if err := checkpointDB(ctx, db); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db after mutation: %w", err)
	}
	dbClosed = true

	if upload {
		expectedManifestVersion, err := l.ManifestStore.HeadVersion(ctx, kbID)
		if err != nil {
			return err
		}
		info, err := l.UploadSnapshotShardedIfMatch(ctx, kbID, dbPath, expectedManifestVersion, targetShardBytes)
		if err != nil {
			return err
		}
		if err := writeLocalShardManifestVersion(localShardManifestVersionPath(kbDir), info.Version); err != nil {
			return err
		}
	}

	if err := l.evictCacheIfNeeded(ctx, kbID); err != nil {
		return err
	}

	return nil
}
