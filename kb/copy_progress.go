package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

// Copy progress journal: a durable per-backup copy-state object that makes
// cross-region/server-side Store.Copy resumable after a crash or outage and
// observable while it runs.
//
// Layout: one JSON object per (dstKBID, copyID) at CopyProgressKey, holding a
// per-shard entry (pending/copied/failed + bytes + etag) plus aggregate
// counters. CopyBackupWithProgress loads the journal if present (resume) or
// creates it CreateOnly (first attempt), copies each pending shard with
// server-side Store.Copy (through-process Download+Upload fallback where the
// store reports copy unsupported), and persists the journal after every shard
// so a crash loses at most the in-flight shard. Failed shards record
// last_error and stop the attempt with an error; the next attempt retries
// exactly the non-copied shards.
//
// Where the store offers no server-side copy, the tiered store's Copy already
// routes bytes through the process via the replication journal; the fallback
// below covers plain stores that return not-implemented/unsupported.

const (
	copyProgressKeyInfix  = ".copies/"
	copyProgressKeySuffix = ".copyprogress.json"

	copyShardPending = "pending"
	copyShardCopied  = "copied"
	copyShardFailed  = "failed"

	copyProgressDocVersion = 1
)

// CopyProgressKey returns the blob key for a per-backup copy-state object.
func CopyProgressKey(dstKBID, copyID string) string {
	return dstKBID + copyProgressKeyInfix + copyID + copyProgressKeySuffix
}

// CopyShardEntry is the journal row for one shard copy.
type CopyShardEntry struct {
	SrcKey    string `json:"src_key"`
	DstKey    string `json:"dst_key"`
	State     string `json:"state"`
	SizeBytes int64  `json:"size_bytes"`
	ETag      string `json:"etag,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// CopyProgressDoc is the durable journal for one backup copy.
type CopyProgressDoc struct {
	Version   int              `json:"version"`
	CopyID    string           `json:"copy_id"`
	SrcKBID   string           `json:"src_kb_id"`
	DstKBID   string           `json:"dst_kb_id"`
	BackupID  string           `json:"backup_id"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	Shards    []CopyShardEntry `json:"shards"`
}

// stampCopyDoc sets UpdatedAt from the deterministic clock, failing closed
// on a nil Clock instead of silently using wall-clock.
func (l *KB) stampCopyDoc(doc *CopyProgressDoc) error {
	now, err := l.clockNow()
	if err != nil {
		return err
	}
	doc.UpdatedAt = now
	return nil
}

func (l *KB) copyLoopPersist(ctx context.Context, doc *CopyProgressDoc) {
	if err := l.stampCopyDoc(doc); err != nil {
		return
	}
	_ = l.saveCopyProgress(ctx, doc)
}

// CopyProgressStatus is the observable snapshot for one copy.
type CopyProgressStatus struct {
	Total          int    `json:"total"`
	Copied         int    `json:"copied"`
	Failed         int    `json:"failed"`
	PendingEntries int    `json:"pending_entries"`
	PendingBytes   int64  `json:"pending_bytes"`
	BytesCopied    int64  `json:"bytes_copied"`
	BytesTotal     int64  `json:"bytes_total"`
	LastError      string `json:"last_error,omitempty"`
}

func copyProgressChecksum(doc *CopyProgressDoc) (string, error) {
	shadow := *doc
	data, err := json.Marshal(shadow)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// CopyProgress reads the durable journal for (dstKBID, copyID) and returns
// the observable status: pending_entries/pending_bytes/last_error/
// bytes_copied/total.
func (l *KB) CopyProgress(ctx context.Context, dstKBID, copyID string) (*CopyProgressStatus, error) {
	if strings.TrimSpace(dstKBID) == "" {
		return nil, fmt.Errorf("kb_id required")
	}
	if err := validateBackupID("copy", copyID); err != nil {
		return nil, err
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	doc, err := l.loadCopyProgress(ctx, dstKBID, copyID)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, fmt.Errorf("%w: copy %s/%s", ErrBackupNotFound, dstKBID, copyID)
	}
	st := &CopyProgressStatus{Total: len(doc.Shards)}
	for _, s := range doc.Shards {
		st.BytesTotal += s.SizeBytes
		switch s.State {
		case copyShardCopied:
			st.Copied++
			st.BytesCopied += s.SizeBytes
		case copyShardFailed:
			st.Failed++
			st.PendingEntries++
			st.PendingBytes += s.SizeBytes
			if st.LastError == "" && s.LastError != "" {
				st.LastError = s.LastError
			}
		default:
			st.PendingEntries++
			st.PendingBytes += s.SizeBytes
		}
	}
	// Surface the most recent failure as last_error.
	for i := len(doc.Shards) - 1; i >= 0; i-- {
		if doc.Shards[i].LastError != "" {
			st.LastError = doc.Shards[i].LastError
			break
		}
	}
	return st, nil
}

// CopyBackupWithProgress server-side copies every shard pinned by a backup
// descriptor into dstKBID shard keys (same remap as CloneKBFromBackup),
// resumably via the durable journal. A crash or outage loses at most the
// in-flight shard: re-calling with the same copyID resumes the remaining
// shards. Returns nil only when every shard is copied.
func (l *KB) CopyBackupWithProgress(ctx context.Context, srcKBID, backupID, dstKBID, copyID string) error {
	if err := validateKBID(srcKBID); err != nil {
		return fmt.Errorf("source %v", err)
	}
	if err := validateKBID(dstKBID); err != nil {
		return fmt.Errorf("target %v", err)
	}
	if srcKBID == dstKBID {
		return fmt.Errorf("copy target must differ from source")
	}
	if err := validateBackupID("backup", backupID); err != nil {
		return err
	}
	if err := validateBackupID("copy", copyID); err != nil {
		return err
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	if _, err := l.clockNow(); err != nil {
		return err
	}
	desc, err := l.GetBackup(ctx, srcKBID, backupID)
	if err != nil {
		return err
	}
	doc, err := l.loadOrInitCopyProgress(ctx, desc, dstKBID, copyID)
	if err != nil {
		return err
	}
	for i := range doc.Shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := &doc.Shards[i]
		if entry.State == copyShardCopied {
			// Crash between copy and journal persist is impossible (persist
			// follows every copy), but a post-copy delete or a stale journal
			// from a wiped target must not read as done: re-check the object.
			if info, err := l.BlobStore.Head(ctx, entry.DstKey); err == nil && info != nil {
				continue
			} else if err != nil && !errors.Is(err, ErrBlobNotFound) && !errors.Is(err, os.ErrNotExist) {
				entry.State = copyShardFailed
				entry.LastError = err.Error()
				l.copyLoopPersist(ctx, doc)
				return fmt.Errorf("head copy target %s: %w", entry.DstKey, err)
			}
			entry.State = copyShardPending
		}
		info, err := l.copyShardServerSide(ctx, entry.SrcKey, entry.DstKey)
		if err != nil {
			if errors.Is(err, ErrBackupExists) || errors.Is(err, ErrBlobVersionMismatch) {
				// Another attempter staged the same target (native
				// Copy CreateOnly conflicts surface as
				// ErrBlobVersionMismatch; CreateOnly uploads synthesize
				// ErrBackupExists): verify the winner's bytes match the
				// source before adopting them.
				if srcData, dErr := l.BlobStore.DownloadBytes(ctx, entry.SrcKey); dErr == nil {
					want := blobstore.BytesSHA256(srcData)
					if vErr := l.verifyCopiedBytes(ctx, entry.SrcKey, entry.DstKey, srcData, want); vErr != nil {
						// Poisoned winner: best-effort delete so a
						// retry can proceed, then journal the failure.
						_ = l.BlobStore.Delete(ctx, entry.DstKey)
						entry.State = copyShardFailed
						entry.LastError = vErr.Error()
						l.copyLoopPersist(ctx, doc)
						return fmt.Errorf("copy shard %s: %w", entry.SrcKey, vErr)
					}
					if head, herr := l.BlobStore.Head(ctx, entry.DstKey); herr == nil && head != nil {
						entry.State = copyShardCopied
						entry.ETag = head.Version
						entry.LastError = ""
						l.copyLoopPersist(ctx, doc)
						continue
					}
				}
			}
			entry.State = copyShardFailed
			entry.LastError = err.Error()
			l.copyLoopPersist(ctx, doc)
			return fmt.Errorf("copy shard %s: %w", entry.SrcKey, err)
		}
		entry.State = copyShardCopied
		entry.LastError = ""
		if info != nil {
			entry.ETag = info.Version
			if info.Size > 0 {
				entry.SizeBytes = info.Size
			}
		}
		if err := l.stampCopyDoc(doc); err != nil {
			return err
		}
		if err := l.saveCopyProgress(ctx, doc); err != nil {
			return fmt.Errorf("persist copy progress %s/%s: %w", dstKBID, copyID, err)
		}
	}
	return nil
}

// copyShardServerSide prefers server-side Store.Copy and falls back to a
// through-process Download+CreateOnly-upload when the store reports copy as
// unsupported. Local and tiered stores implement Copy natively (tiered via
// the replication journal); the fallback covers plain custom stores. The
// fallback re-hashes: the uploaded bytes are re-read and SHA-verified
// against the source bytes (like verifyStagedClone) so a torn
// Download+Upload never reads as done.
func (l *KB) copyShardServerSide(ctx context.Context, srcKey, dstKey string) (*blobstore.ObjectInfo, error) {
	info, err := l.BlobStore.Copy(ctx, srcKey, dstKey, blobstore.CopyOptions{CreateOnly: true})
	if err == nil {
		return info, nil
	}
	if !isCopyUnsupported(err) {
		return nil, err
	}
	data, err := l.BlobStore.DownloadBytes(ctx, srcKey)
	if err != nil {
		return nil, err
	}
	want := blobstore.BytesSHA256(data)
	uploaded, err := l.uploadBytesCreateOnly(ctx, dstKey, data)
	if err != nil {
		if errors.Is(err, ErrBackupExists) {
			// Another attempter staged the target first: verify the
			// winner's bytes match the source before adopting them.
			// A torn winner is deleted best-effort so a retry can
			// proceed; the verify error fails this attempt.
			if herr := l.verifyCopiedBytes(ctx, srcKey, dstKey, data, want); herr != nil {
				_ = l.BlobStore.Delete(ctx, dstKey)
				return nil, herr
			}
			head, herr := l.BlobStore.Head(ctx, dstKey)
			if herr != nil {
				return nil, herr
			}
			return head, nil
		}
		return nil, err
	}
	if herr := l.verifyCopiedBytes(ctx, srcKey, dstKey, data, want); herr != nil {
		_ = l.BlobStore.Delete(ctx, dstKey)
		return nil, herr
	}
	_ = uploaded
	head, err := l.BlobStore.Head(ctx, dstKey)
	if err != nil {
		return nil, err
	}
	return head, nil
}

// verifyCopiedBytes re-reads dstKey and checks size+SHA against the source
// bytes the fallback downloaded. A mismatch fails with ErrBackupCorrupt so
// the copy attempt retries instead of journaling a torn object as copied.
func (l *KB) verifyCopiedBytes(ctx context.Context, srcKey, dstKey string, srcData []byte, wantSHA string) error {
	dstData, err := l.BlobStore.DownloadBytes(ctx, dstKey)
	if err != nil {
		return fmt.Errorf("re-read fallback copy %s: %w", dstKey, err)
	}
	if len(dstData) != len(srcData) {
		return fmt.Errorf("%w: fallback copy %s size %d does not match source %s size %d",
			ErrBackupCorrupt, dstKey, len(dstData), srcKey, len(srcData))
	}
	if got := blobstore.BytesSHA256(dstData); got != wantSHA {
		return fmt.Errorf("%w: fallback copy %s failed sha256 verification", ErrBackupCorrupt, dstKey)
	}
	return nil
}

func isCopyUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{"not implemented", "not supported", "unsupported", "unknown method"} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func (l *KB) loadCopyProgress(ctx context.Context, dstKBID, copyID string) (*CopyProgressDoc, error) {
	data, err := l.BlobStore.DownloadBytes(ctx, CopyProgressKey(dstKBID, copyID))
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var doc CopyProgressDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("%w: copy progress %s/%s: %w", ErrBackupCorrupt, dstKBID, copyID, err)
	}
	if doc.Version != copyProgressDocVersion {
		return nil, fmt.Errorf("%w: unknown copy progress version %d", ErrBackupCorrupt, doc.Version)
	}
	return &doc, nil
}

func (l *KB) loadOrInitCopyProgress(ctx context.Context, desc *BackupDescriptor, dstKBID, copyID string) (*CopyProgressDoc, error) {
	if existing, err := l.loadCopyProgress(ctx, dstKBID, copyID); err != nil {
		return nil, err
	} else if existing != nil {
		if err := checkCopyProgressResume(existing, desc, dstKBID); err != nil {
			return nil, err
		}
		return existing, nil
	}
	now, err := l.clockNow()
	if err != nil {
		return nil, err
	}
	doc := &CopyProgressDoc{
		Version:   copyProgressDocVersion,
		CopyID:    copyID,
		SrcKBID:   desc.SourceKBID,
		DstKBID:   dstKBID,
		BackupID:  desc.BackupID,
		CreatedAt: now,
		UpdatedAt: now,
		Shards:    make([]CopyShardEntry, 0, len(desc.Shards)),
	}
	seen := make(map[string]struct{}, len(desc.Shards))
	for _, ref := range desc.Shards {
		dstKey := remapShardKey(desc.SourceKBID, dstKBID, ref.Key)
		if _, dup := seen[dstKey]; dup {
			return nil, fmt.Errorf("%w: copy remap collides on target key %q", ErrBackupCorrupt, dstKey)
		}
		seen[dstKey] = struct{}{}
		doc.Shards = append(doc.Shards, CopyShardEntry{
			SrcKey: ref.Key, DstKey: dstKey,
			State: copyShardPending, SizeBytes: ref.SizeBytes,
		})
	}
	sort.Slice(doc.Shards, func(i, j int) bool { return doc.Shards[i].DstKey < doc.Shards[j].DstKey })
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	if _, err := l.uploadBytesCreateOnly(ctx, CopyProgressKey(dstKBID, copyID), data); err != nil {
		if errors.Is(err, ErrBackupExists) {
			// Lost the create race: another attempter owns the journal.
			// Validate before adopting so a stale journal for a different
			// source/backup/shard-set cannot resume here.
			if existing, lerr := l.loadCopyProgress(ctx, dstKBID, copyID); lerr == nil && existing != nil {
				if verr := checkCopyProgressResume(existing, desc, dstKBID); verr != nil {
					return nil, verr
				}
				return existing, nil
			}
		}
		return nil, err
	}
	return doc, nil
}

// checkCopyProgressResume rejects resuming a journal that names a different
// source KB, destination KB, backup, or shard set. A mismatch means the
// caller reused a copyID: use a fresh copyID instead of merging unrelated
// copies. The error wraps ErrBackupExists so callers can distinguish "pick
// a new copyID" from corruption.
func checkCopyProgressResume(existing *CopyProgressDoc, desc *BackupDescriptor, dstKBID string) error {
	if existing.SrcKBID != desc.SourceKBID ||
		existing.DstKBID != dstKBID ||
		existing.BackupID != desc.BackupID {
		return fmt.Errorf("%w: copy journal %s/%s names %s/%s/%s, want %s/%s/%s: use a fresh copyID",
			ErrBackupExists, existing.DstKBID, existing.CopyID,
			existing.SrcKBID, existing.DstKBID, existing.BackupID,
			desc.SourceKBID, dstKBID, desc.BackupID)
	}
	want := make(map[string]string, len(desc.Shards))
	for _, ref := range desc.Shards {
		want[remapShardKey(desc.SourceKBID, dstKBID, ref.Key)] = ref.Key
	}
	if len(existing.Shards) != len(want) {
		return fmt.Errorf("%w: copy journal %s/%s has %d shards, want %d: use a fresh copyID",
			ErrBackupExists, existing.DstKBID, existing.CopyID, len(existing.Shards), len(want))
	}
	for _, entry := range existing.Shards {
		srcWant, ok := want[entry.DstKey]
		if !ok || srcWant != entry.SrcKey {
			return fmt.Errorf("%w: copy journal %s/%s shard %s/%s does not match backup: use a fresh copyID",
				ErrBackupExists, existing.DstKBID, existing.CopyID, entry.SrcKey, entry.DstKey)
		}
	}
	return nil
}

func (l *KB) saveCopyProgress(ctx context.Context, doc *CopyProgressDoc) error {
	if doc == nil {
		return fmt.Errorf("copy progress doc is nil")
	}
	if _, err := copyProgressChecksum(doc); err != nil {
		return err
	}
	if _, err := l.clockNow(); err != nil {
		return err
	}
	key := CopyProgressKey(doc.DstKBID, doc.CopyID)
	// Fenced write: the in-process stripe plus the shared write lease
	// serialize writers, and the version-checked put with merge-retry keeps
	// a concurrent same-copyID writer from silently dropping the other's
	// shard states (no state loss: per-shard states merge, copied wins).
	mu := l.LockFor("copy-progress/" + key)
	mu.Lock()
	defer mu.Unlock()
	mgr, lease, leaseErr := l.AcquireWriteLease(ctx, "copy-progress/"+key)
	if leaseErr != nil {
		return fmt.Errorf("fence copy progress %s/%s: %w", doc.DstKBID, doc.CopyID, leaseErr)
	}
	defer func() { _ = mgr.Release(context.Background(), lease) }()
	for tries := 0; tries < 8; tries++ {
		expected := ""
		if info, herr := l.BlobStore.Head(ctx, key); herr == nil && info != nil {
			expected = info.Version
		} else if herr != nil && !errors.Is(herr, ErrBlobNotFound) && !errors.Is(herr, os.ErrNotExist) {
			return herr
		}
		toWrite := doc
		if current, lerr := l.loadCopyProgress(ctx, doc.DstKBID, doc.CopyID); lerr == nil && current != nil {
			toWrite = mergeCopyProgress(current, doc)
		} else if lerr != nil {
			return lerr
		}
		data, err := json.Marshal(toWrite)
		if err != nil {
			return err
		}
		if _, err := l.BlobStore.UploadBytesIfMatch(ctx, key, data, expected); err != nil {
			if errors.Is(err, ErrBlobVersionMismatch) {
				continue // rival won the race; reload, merge, retry
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: copy progress %s/%s is contended", ErrBlobVersionMismatch, doc.DstKBID, doc.CopyID)
}

// mergeCopyProgress unions per-shard states so concurrent same-copyID
// savers keep each other's work: copied wins over failed/pending, failed
// wins over pending (newest LastError), sizes/ETags follow the winning
// state. Shard identity is the destination key.
func mergeCopyProgress(base, updated *CopyProgressDoc) *CopyProgressDoc {
	out := *base
	out.Shards = append([]CopyShardEntry(nil), base.Shards...)
	byDst := make(map[string]int, len(out.Shards))
	for i, s := range out.Shards {
		byDst[s.DstKey] = i
	}
	for _, s := range updated.Shards {
		i, ok := byDst[s.DstKey]
		if !ok {
			byDst[s.DstKey] = len(out.Shards)
			out.Shards = append(out.Shards, s)
			continue
		}
		cur := &out.Shards[i]
		switch {
		case s.State == copyShardCopied || cur.State == copyShardCopied:
			if s.State == copyShardCopied {
				*cur = s
			}
			cur.State = copyShardCopied
			if cur.LastError != "" && s.State == copyShardCopied && s.LastError == "" {
				cur.LastError = ""
			}
			if cur.ETag == "" {
				cur.ETag = s.ETag
			}
			if cur.SizeBytes == 0 {
				cur.SizeBytes = s.SizeBytes
			}
		case s.State == copyShardFailed:
			*cur = s
		default:
			if cur.State == copyShardPending && s.SizeBytes != 0 {
				cur.SizeBytes = s.SizeBytes
			}
		}
	}
	sort.Slice(out.Shards, func(i, j int) bool { return out.Shards[i].DstKey < out.Shards[j].DstKey })
	if updated.UpdatedAt.After(out.UpdatedAt) {
		out.UpdatedAt = updated.UpdatedAt
	}
	return &out
}
