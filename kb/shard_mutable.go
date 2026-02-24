// shard_mutable.go manages the mutable per-KB DuckDB database that serves as
// the writable accumulation layer for document mutations before they are
// published as immutable shard files.
//
// System fit:
//
//   - Each write operation (upsert, delete) materialises the current shard
//     snapshot into a local mutable DuckDB file, applies the mutation, then
//     checkpoints and publishes updated shards.
//   - The local manifest version sentinel (manifest.version) records which
//     blob manifest the local DB was last built from. On the next write, if
//     the stored version matches the current remote manifest, the existing DB
//     is reused without a download; otherwise it is rebuilt from the shards.
//   - Bootstrap: when a KB has no remote manifest yet (first write ever), an
//     empty DB with the correct schema is created locally without downloading
//     anything.
//
// Failure modes:
//
//   - If the local version file is unreadable for any reason other than
//     ErrNotExist, the error is surfaced immediately and the write aborts.
//   - If the DB file is missing despite a matching version sentinel, a fresh
//     download is triggered automatically.

package kb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const localShardManifestVersionFile = "manifest.version"

// localShardManifestVersionPath returns the path to the local manifest version
// sentinel file within kbDir. The sentinel stores the blob manifest version
// that the current mutable DB was built from.
func localShardManifestVersionPath(kbDir string) string {
	return filepath.Join(kbDir, localShardManifestVersionFile)
}

// readLocalShardManifestVersion reads the stored manifest version from the
// sentinel file at path. Returns os.ErrNotExist (via the wrapped error) when
// the file is absent, which callers treat as "no local version known".
func readLocalShardManifestVersion(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// writeLocalShardManifestVersion writes version to the sentinel file at path.
// Called after a successful snapshot download to record the manifest version
// the local DB was built from, so subsequent writes can skip the download when
// the version is unchanged.
func writeLocalShardManifestVersion(path, version string) error {
	return os.WriteFile(path, []byte(strings.TrimSpace(version)), 0o644)
}

// ensureMutableShardDBLocked ensures a mutable DuckDB file is present at
// dbPath and consistent with the current remote manifest.
//
// The caller must hold the per-KB write lock before calling this function.
//
// Logic:
//  1. Fetch the current remote manifest version.
//  2. If no manifest exists and allowBootstrap is true, create an empty DB.
//     If allowBootstrap is false, return ErrKBUninitialized.
//  3. If a matching local version sentinel exists and the DB file is present,
//     return immediately — the local DB is already up to date.
//  4. Otherwise download and reconstruct the DB from the current shards, then
//     write the new version sentinel.
func (l *KB) ensureMutableShardDBLocked(
	ctx context.Context,
	kbID string,
	kbDir string,
	dbPath string,
	embeddingDim int,
	allowBootstrap bool,
) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kbID cannot be empty")
	}

	manifestVersion, err := l.currentShardManifestVersion(ctx, kbID)
	if err != nil {
		return err
	}
	if manifestVersion == "" {
		if !allowBootstrap {
			return ErrKBUninitialized
		}
		if embeddingDim <= 0 {
			return fmt.Errorf("embedding dimension must be > 0")
		}
		return l.createEmptyMutableShardDBLocked(ctx, kbDir, dbPath, embeddingDim)
	}

	localVersionPath := localShardManifestVersionPath(kbDir)
	localVersion, readErr := readLocalShardManifestVersion(localVersionPath)
	hasLocalVersion := readErr == nil
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	if hasLocalVersion && localVersion == manifestVersion {
		if _, statErr := os.Stat(dbPath); statErr == nil {
			return nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}

	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}
	if _, err := l.DownloadSnapshotFromShards(ctx, kbID, dbPath); err != nil {
		return err
	}
	if err := writeLocalShardManifestVersion(localVersionPath, manifestVersion); err != nil {
		return err
	}

	return nil
}

// createEmptyMutableShardDBLocked initialises an empty DuckDB file at dbPath
// with the docs, doc_tombstones, entities, edges, and doc_entities tables.
//
// Used on the first write to a KB that has no remote manifest yet (bootstrap
// path). The caller must hold the per-KB write lock.
func (l *KB) createEmptyMutableShardDBLocked(ctx context.Context, kbDir, dbPath string, embeddingDim int) error {
	if embeddingDim <= 0 {
		return fmt.Errorf("embedding dimension must be > 0")
	}
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}

	db, err := l.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}

	createDocsSQL := `
		CREATE TABLE IF NOT EXISTS docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[` + strconv.Itoa(embeddingDim) + `]
		)
	`
	if _, err := db.ExecContext(ctx, createDocsSQL); err != nil {
		_ = db.Close()
		return fmt.Errorf("create docs table for shard bootstrap: %w", err)
	}
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := EnsureGraphTables(ctx, db); err != nil {
		_ = db.Close()
		return err
	}

	if err := checkpointAndCloseDB(ctx, db, "close db after shard bootstrap"); err != nil {
		return err
	}
	return nil
}
