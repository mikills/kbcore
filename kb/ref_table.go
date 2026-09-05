package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Global shard-reference table: a single index mapping each shared shard key
// to the set of owner IDs (branches/restores/backups) still referencing it.
// GC consults it alongside the legacy per-owner fan-out markers, and
// DeleteBranch releases through it.
//
// Migration: markers written by the old fan-out path (one BranchRecord per
// owner prefix) are still honored — backupPinnedShardKeys reads every marker
// under the KB prefix and pinnedKeysForGC unions those with this table. New
// branches write ONE marker under the source prefix plus one owner entry per
// shard key here ("read both, write new").

const (
	refTableKey         = "_global.shard-refs/ref-table.json"
	refTableDocVersion  = 1
	refTableLeaseSuffix = "ref-table"
)

// RefTableDoc is the single global shard-reference index.
type RefTableDoc struct {
	Version   int                 `json:"version"`
	UpdatedAt time.Time           `json:"updated_at"`
	Refs      map[string][]string `json:"refs"`
}

// branchRefOwner is the ref-table owner ID for one branch/restore marker.
func branchRefOwner(rec *BranchRecord) string {
	return "branch:" + rec.SourceKBID + "/" + rec.BranchID
}

// legacyBranchRefOwner rebuilds the owner ID when the record itself is gone
// (DeleteBranch on an already-removed marker).
func legacyBranchRefOwner(ownerKBID, branchID string) string {
	return "branch:" + ownerKBID + "/" + branchID
}

func (l *KB) loadRefTable(ctx context.Context) (*RefTableDoc, error) {
	if l.BlobStore == nil {
		return &RefTableDoc{Version: refTableDocVersion, Refs: map[string][]string{}}, nil
	}
	data, err := l.BlobStore.DownloadBytes(ctx, refTableKey)
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return &RefTableDoc{Version: refTableDocVersion, Refs: map[string][]string{}}, nil
		}
		return nil, fmt.Errorf("read shard ref table: %w", err)
	}
	var doc RefTableDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("%w: shard ref table: %w", ErrBackupCorrupt, err)
	}
	if doc.Version != refTableDocVersion {
		return nil, fmt.Errorf("%w: unknown shard ref table version %d", ErrBackupCorrupt, doc.Version)
	}
	if doc.Refs == nil {
		doc.Refs = map[string][]string{}
	}
	return &doc, nil
}

// withRefTableLock serializes global-table writers sharing this process
// (stripe) and, where the write-lease manager is shared across processes,
// across processes too. Fencing is real: a lease conflict or lease error
// propagates and fn is NOT executed, so a rival holder never runs
// concurrently. Callers that hit ErrWriteLeaseConflict should back off and
// retry the table mutation.
//
// Residual risk (quantified): stores whose UploadBytesIfMatch ignores the
// expected version AND that share no lease manager across processes keep a
// last-writer-wins window: two writers in different processes can both read
// the same base and the second put silently drops the first's owners. The
// CAS retry in mutateRefTable closes the gap on every store with a
// conditional put (Local/S3/tiered) even without a shared lease; without
// either primitive, re-branching (or the legacy markers GC still honors)
// repairs a dropped entry.
func (l *KB) withRefTableLock(ctx context.Context, fn func() error) error {
	mu := l.LockFor("reftable/" + refTableLeaseSuffix)
	mu.Lock()
	defer mu.Unlock()
	mgr, lease, err := l.AcquireWriteLease(ctx, "reftable/"+refTableLeaseSuffix)
	if err != nil {
		return fmt.Errorf("fence shard ref table: %w", err)
	}
	defer func() { _ = mgr.Release(context.Background(), lease) }()
	return fn()
}

// loadRefTableVersioned returns the table plus the version precondition for
// a CAS put ("" when the object is absent).
func (l *KB) loadRefTableVersioned(ctx context.Context) (*RefTableDoc, string, error) {
	doc, err := l.loadRefTable(ctx)
	if err != nil {
		return nil, "", err
	}
	info, err := l.BlobStore.Head(ctx, refTableKey)
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			return doc, "", nil
		}
		return nil, "", fmt.Errorf("head shard ref table: %w", err)
	}
	if info == nil {
		return doc, "", nil
	}
	return doc, info.Version, nil
}

// mutateRefTable applies mutate under the ref-table fence with CAS retry:
// each attempt reloads the table, re-applies the mutation, and writes with
// the freshly-read version. A version conflict reloads and retries (up to
// 8 tries) instead of dropping a rival's owners.
func (l *KB) mutateRefTable(ctx context.Context, mutate func(doc *RefTableDoc) bool) error {
	if _, err := l.clockNow(); err != nil {
		return err
	}
	return l.withRefTableLock(ctx, func() error {
		for tries := 0; tries < 8; tries++ {
			doc, version, err := l.loadRefTableVersioned(ctx)
			if err != nil {
				return err
			}
			if !mutate(doc) {
				return nil
			}
			if doc.Refs == nil {
				doc.Refs = map[string][]string{}
			}
			if now, err := l.clockNow(); err != nil {
				return err
			} else {
				doc.UpdatedAt = now
			}
			data, err := json.Marshal(doc)
			if err != nil {
				return err
			}
			if _, err := l.BlobStore.UploadBytesIfMatch(ctx, refTableKey, data, version); err != nil {
				if errors.Is(err, ErrBlobVersionMismatch) {
					continue
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("%w: shard ref table is contended", ErrBlobVersionMismatch)
	})
}

// AddRefOwners records owner as referencing every shard key.
func (l *KB) AddRefOwners(ctx context.Context, shardKeys []string, owner string) error {
	if owner == "" || len(shardKeys) == 0 {
		return nil
	}
	if l.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	return l.mutateRefTable(ctx, func(doc *RefTableDoc) bool {
		changed := false
		for _, key := range shardKeys {
			if key == "" {
				continue
			}
			owners := doc.Refs[key]
			found := false
			for _, o := range owners {
				if o == owner {
					found = true
					break
				}
			}
			if !found {
				doc.Refs[key] = append(owners, owner)
				sort.Strings(doc.Refs[key])
				changed = true
			}
		}
		return changed
	})
}

// RemoveRefOwner drops owner from every shard entry, pruning empty keys.
// It reports whether any entry changed.
func (l *KB) RemoveRefOwner(ctx context.Context, owner string) (bool, error) {
	if owner == "" {
		return false, nil
	}
	if l.BlobStore == nil {
		return false, fmt.Errorf("blob store is not configured")
	}
	changed := false
	err := l.mutateRefTable(ctx, func(doc *RefTableDoc) bool {
		innerChanged := false
		for key, owners := range doc.Refs {
			kept := owners[:0]
			for _, o := range owners {
				if o != owner {
					kept = append(kept, o)
				}
			}
			if len(kept) != len(owners) {
				innerChanged = true
			}
			if len(kept) == 0 {
				delete(doc.Refs, key)
			} else {
				doc.Refs[key] = kept
			}
		}
		if innerChanged {
			changed = true
		}
		return innerChanged
	})
	return changed, err
}

// refTablePinnedKeys returns every shard key with at least one ref-table
// owner. A missing table pins nothing; a list/read failure is returned so GC
// retries instead of collecting shared bytes.
func (l *KB) refTablePinnedKeys(ctx context.Context) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	doc, err := l.loadRefTable(ctx)
	if err != nil {
		return nil, err
	}
	for key, owners := range doc.Refs {
		if key != "" && len(owners) > 0 {
			out[key] = struct{}{}
		}
	}
	return out, nil
}

// refOwnersOf is a test/debug helper listing owners for one shard key.
func (l *KB) refOwnersOf(ctx context.Context, shardKey string) []string {
	doc, err := l.loadRefTable(ctx)
	if err != nil {
		return nil
	}
	out := append([]string(nil), doc.Refs[shardKey]...)
	sort.Strings(out)
	return out
}

var _ = strings.TrimSpace
