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

// RetentionPolicy bounds how many recent valid recovery points to keep and
// how long a valid descriptor may age before it becomes deletion-eligible.
//
// KeepLastN keeps the newest KeepLastN valid descriptors (values < 1 mean 1:
// the newest valid descriptor is never deleted). MaxAge additionally expires
// valid descriptors older than now-MaxAge (<= 0 disables age expiry). Only
// expired VALID descriptors are ever deleted: corrupt/unreadable markers are
// skipped, never deleted, and any list/read failure aborts the sweep with
// zero deletions. Shard bytes are never deleted here; reachability GC
// (shard_gc.go) reclaims them once no live manifest, backup, branch, journal
// pending, reader pin, or ref-table entry names them.
type RetentionPolicy struct {
	KeepLastN int
	MaxAge    time.Duration
}

// DefaultRetentionPolicy keeps the newest 7 valid descriptors with no age expiry.
func DefaultRetentionPolicy() RetentionPolicy { return RetentionPolicy{KeepLastN: 7} }

// RetentionJobID is the scheduled retention sweep registered by RegisterDefaultJobs.
const RetentionJobID = "retention"

// RetentionJobExpr runs daily; retention is a slow lifecycle sweep, not a hot loop.
const RetentionJobExpr = "0 3 * * *"

// SweepRetention deletes expired backup descriptors for one KB per policy and
// returns the deleted backup IDs (sorted). It never deletes the newest valid
// descriptor. Corrupt descriptors are skipped (left in place, no error). Any
// list or read failure aborts before any delete and returns delete==0 with
// the error. A delete-phase failure returns the IDs deleted so far plus a
// joined error.
func (l *KB) SweepRetention(ctx context.Context, kbID string, policy RetentionPolicy) ([]string, error) {
	if err := validateKBID(kbID); err != nil {
		return nil, err
	}
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	now, err := l.clockNow()
	if err != nil {
		return nil, err
	}
	ids, err := l.ListBackupIDs(ctx, kbID)
	if err != nil {
		return nil, fmt.Errorf("list backups for retention %q: %w", kbID, err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	type loaded struct {
		id   string
		desc BackupDescriptor
	}
	var valid []loaded
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := l.BlobStore.DownloadBytes(ctx, BackupDescriptorKey(kbID, id))
		if err != nil {
			// Partial verify: a missing/unreadable descriptor means the
			// listing may be racing a writer, so delete nothing.
			return nil, fmt.Errorf("read backup %s/%s for retention: %w", kbID, id, err)
		}
		var desc BackupDescriptor
		if err := json.Unmarshal(data, &desc); err != nil {
			continue // corrupt: skipped, never deleted
		}
		if err := ValidateBackupDescriptor(&desc); err != nil {
			if errors.Is(err, ErrBackupLegacyFormat) {
				continue // legacy: skipped like corrupt; operator migrates by re-seal
			}
			continue
		}
		valid = append(valid, loaded{id: id, desc: desc})
	}
	if len(valid) == 0 {
		return nil, nil
	}
	sort.Slice(valid, func(i, j int) bool {
		if valid[i].desc.CreatedAt.Equal(valid[j].desc.CreatedAt) {
			return valid[i].id < valid[j].id
		}
		return valid[i].desc.CreatedAt.After(valid[j].desc.CreatedAt)
	})
	keep := policy.KeepLastN
	if keep < 1 {
		keep = 1
	}
	var expired []string
	for i, v := range valid {
		if i == 0 {
			continue // newest valid is never deleted, even when expired
		}
		overCount := i >= keep
		overAge := policy.MaxAge > 0 && now.Sub(v.desc.CreatedAt) > policy.MaxAge
		if overCount || overAge {
			expired = append(expired, v.id)
		}
	}
	sort.Strings(expired)
	var deleted []string
	var errs []error
	for _, id := range expired {
		if err := ctx.Err(); err != nil {
			return deleted, errors.Join(append(errs, err)...)
		}
		if err := l.DeleteBackup(ctx, kbID, id); err != nil {
			if errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
				continue // raced with another sweeper; not a failure
			}
			errs = append(errs, fmt.Errorf("delete expired backup %s/%s: %w", kbID, id, err))
			continue
		}
		deleted = append(deleted, id)
	}
	return deleted, errors.Join(errs...)
}

// SweepRetentionAll runs SweepRetention for every KB that owns backup
// descriptors, discovered from storage. A per-KB failure is joined and the
// sweep continues with the remaining KBs; a discovery-list failure aborts
// with no deletes anywhere.
func (l *KB) SweepRetentionAll(ctx context.Context, policy RetentionPolicy) (map[string][]string, error) {
	if l.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	objects, err := l.BlobStore.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list knowledge bases for retention: %w", err)
	}
	seen := make(map[string]struct{})
	for _, obj := range objects {
		key := obj.Key
		if idx := strings.Index(key, backupKeyInfix); idx > 0 {
			candidate := key[:idx]
			if !strings.Contains(candidate, "/") {
				seen[candidate] = struct{}{}
			}
		}
		if kbID, ok := KBIDFromManifestKey(key); ok {
			seen[kbID] = struct{}{}
		}
	}
	kbIDs := make([]string, 0, len(seen))
	for kbID := range seen {
		kbIDs = append(kbIDs, kbID)
	}
	sort.Strings(kbIDs)
	out := make(map[string][]string, len(kbIDs))
	var errs []error
	for _, kbID := range kbIDs {
		if err := ctx.Err(); err != nil {
			return out, errors.Join(append(errs, err)...)
		}
		deleted, err := l.SweepRetention(ctx, kbID, policy)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if len(deleted) > 0 {
			out[kbID] = deleted
		}
	}
	return out, errors.Join(errs...)
}

// runRetentionScheduled is the scheduler entry point: it uses the KB's
// injected Clock (via SweepRetention -> clockNow) and default policy knobs,
// never time.Now directly.
func (l *KB) runRetentionScheduled(ctx context.Context) error {
	_, err := l.SweepRetentionAll(ctx, l.retentionPolicy())
	return err
}

func (l *KB) retentionPolicy() RetentionPolicy {
	l.mu.Lock()
	defer l.mu.Unlock()
	policy := RetentionPolicy{KeepLastN: l.RetentionKeepLastN, MaxAge: l.RetentionMaxAge}
	if policy.KeepLastN < 1 {
		policy.KeepLastN = DefaultRetentionPolicy().KeepLastN
	}
	return policy
}
