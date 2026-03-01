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
	"os"
	"path/filepath"
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

