package kb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	pendingSessionFileName = ".pending-session"

	// committedSessionFileName names the commit that published the last
	// session, which the cleared pending marker no longer records.
	committedSessionFileName = ".committed-session"
)

// ErrUnpublishedWrites reports rows that refreshing or evicting would destroy.
var ErrUnpublishedWrites = errors.New("knowledge base has unpublished writes")

func PendingSessionPath(kbDir string) string {
	return filepath.Join(kbDir, pendingSessionFileName)
}

// MarkPendingSession syncs, because the rows it guards are checkpointed right
// after and a marker left in the page cache would not survive a crash.
func MarkPendingSession(kbDir string) error {
	return writeSyncedFile(kbDir, PendingSessionPath(kbDir), "1")
}

// RecordSessionCommit remembers which commit published this knowledge base.
// Written after the publish, so a crash before it fails a retryable commit
// rather than reporting a publish that did not happen.
func RecordSessionCommit(kbDir, commitID string) error {
	if strings.TrimSpace(commitID) == "" {
		return nil
	}
	return writeSyncedFile(kbDir, filepath.Join(kbDir, committedSessionFileName), commitID)
}

// SessionCommitted reports whether commitID published the last session here.
func SessionCommitted(kbDir, commitID string) bool {
	if strings.TrimSpace(commitID) == "" {
		return false
	}
	data, err := os.ReadFile(filepath.Join(kbDir, committedSessionFileName))
	return err == nil && strings.TrimSpace(string(data)) == commitID
}

func writeSyncedFile(kbDir, path, contents string) error {
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(contents)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	syncDirEntry(kbDir)
	return nil
}

// syncDirEntry is best effort, since the marker itself is already synced.
func syncDirEntry(dir string) {
	handle, err := os.Open(dir)
	if err != nil {
		return
	}
	defer handle.Close()
	_ = handle.Sync()
}

func ClearPendingSession(kbDir string) error {
	err := os.Remove(PendingSessionPath(kbDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// HasPendingSession counts an unreadable directory as marked, so it is never
// overwritten.
func HasPendingSession(kbDir string) bool {
	info, err := os.Stat(kbDir)
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	if !info.IsDir() {
		return false
	}
	_, err = os.Stat(PendingSessionPath(kbDir))
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

func UnpublishedWritesError(kbID string) error {
	return fmt.Errorf("%w: %s: commit the ingest session before reading it back", ErrUnpublishedWrites, kbID)
}
