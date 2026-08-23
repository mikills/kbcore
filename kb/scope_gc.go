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

const (
	scopeGCSchema = "minnow.scope_gc/v1"
	ScopeGCGrace  = time.Hour
)

type scopeGCMarker struct {
	Schema     string               `json:"schema"`
	KBID       string               `json:"kb_id"`
	Candidates map[string]time.Time `json:"candidates"`
}

type loadedScopeGCMarker struct {
	object BlobObjectInfo
	marker scopeGCMarker
}

func (k *KB) ScheduleScopeGC(ctx context.Context, kbID string, ids []string) ([]string, error) {
	ids = normalizeScopeIDs(ids)
	if len(ids) == 0 {
		return []string{}, nil
	}
	mutation, err := k.acquireScopeMutation(ctx, kbID)
	if err != nil {
		return nil, err
	}
	mutationCtx := mutation.Context()
	now := time.Now().UTC()
	if k.Clock != nil {
		now = k.Clock.Now().UTC()
	}
	err = k.scheduleScopeGCLocked(mutationCtx, kbID, ids, now)
	return ids, errors.Join(err, mutation.Close())
}

func (k *KB) scheduleScopeGCLocked(ctx context.Context, kbID string, ids []string, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	key := scopeGCBatchKey(kbID, ids)
	loaded, err := k.loadScopeGCMarker(ctx, key, kbID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		loaded.marker.Candidates[id] = now.Add(ScopeGCGrace)
	}
	return k.saveScopeGCMarker(ctx, key, loaded.marker, loaded.object.Version)
}

func (k *KB) SweepScopeGC(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
		if k.Clock != nil {
			now = k.Clock.Now().UTC()
		}
	}
	objects, err := k.BlobStore.List(ctx, scopeGCPrefix())
	if err != nil {
		return 0, err
	}
	kbIDs := make(map[string]struct{})
	for _, object := range objects {
		loaded, err := k.loadScopeGCMarker(ctx, object.Key, "")
		if err != nil {
			return 0, err
		}
		if loaded.marker.KBID == "" {
			continue
		}
		kbIDs[loaded.marker.KBID] = struct{}{}
	}
	ordered := make([]string, 0, len(kbIDs))
	for kbID := range kbIDs {
		ordered = append(ordered, kbID)
	}
	sort.Strings(ordered)
	deleted := 0
	var errs []error
	for _, kbID := range ordered {
		count, err := k.sweepScopeGCForKB(ctx, kbID, now)
		deleted += count
		if err != nil {
			errs = append(errs, err)
		}
	}
	return deleted, errors.Join(errs...)
}

func (k *KB) sweepScopeGCForKB(ctx context.Context, kbID string, now time.Time) (int, error) {
	mutation, err := k.acquireScopeMutation(ctx, kbID)
	if err != nil {
		return 0, err
	}
	mutationCtx := mutation.Context()
	if session, peekErr := k.IngestSessionsFor().Peek(mutationCtx, kbID); peekErr != nil || session != nil {
		return 0, errors.Join(peekErr, mutation.Close())
	}
	objects, err := k.BlobStore.List(mutationCtx, scopeGCKBPrefix(kbID))
	if err != nil {
		return 0, errors.Join(err, mutation.Close())
	}
	markers := make([]loadedScopeGCMarker, 0, len(objects))
	deadlines := make(map[string]time.Time)
	for _, object := range objects {
		loaded, err := k.loadScopeGCMarker(mutationCtx, object.Key, kbID)
		if err != nil {
			return 0, errors.Join(err, mutation.Close())
		}
		markers = append(markers, loaded)
		for id, deadline := range loaded.marker.Candidates {
			if deadline.After(deadlines[id]) {
				deadlines[id] = deadline
			}
		}
	}
	referenced, err := k.scopeReferences(mutationCtx, kbID)
	if err != nil {
		return 0, errors.Join(err, mutation.Close())
	}
	due := make([]string, 0)
	for id, deadline := range deadlines {
		if _, used := referenced[id]; !used && !now.Before(deadline) {
			due = append(due, id)
		}
	}
	sort.Strings(due)
	if len(due) > 0 {
		format, resolveErr := k.resolveFormat(mutationCtx, kbID)
		if resolveErr != nil {
			return 0, errors.Join(resolveErr, mutation.Close())
		}
		_, err = format.Delete(mutationCtx, IngestDeleteRequest{
			KBID: kbID, DocIDs: due, Upload: true,
			Options: DeleteDocsOptions{HardDelete: true, CleanupGraph: true},
		})
		if err != nil {
			return 0, errors.Join(err, mutation.Close())
		}
	}
	removed := make(map[string]struct{}, len(referenced)+len(due))
	for id := range referenced {
		removed[id] = struct{}{}
	}
	for _, id := range due {
		removed[id] = struct{}{}
	}
	for _, loaded := range markers {
		changed := false
		for id := range loaded.marker.Candidates {
			if _, ok := removed[id]; ok {
				delete(loaded.marker.Candidates, id)
				changed = true
			}
		}
		if !changed {
			continue
		}
		if len(loaded.marker.Candidates) == 0 {
			err = k.BlobStore.Delete(mutationCtx, loaded.object.Key)
		} else {
			err = k.saveScopeGCMarker(
				mutationCtx, loaded.object.Key, loaded.marker, loaded.object.Version,
			)
		}
		if err != nil {
			return len(due), errors.Join(err, mutation.Close())
		}
	}
	return len(due), mutation.Close()
}

func (k *KB) scopeReferences(ctx context.Context, kbID string) (map[string]struct{}, error) {
	scopes, err := k.ListScopes(ctx, kbID)
	if err != nil {
		return nil, err
	}
	referenced := make(map[string]struct{})
	for _, scope := range scopes {
		for _, id := range scope.DocumentIDs {
			referenced[id] = struct{}{}
		}
	}
	return referenced, nil
}

func (k *KB) loadScopeGCMarker(ctx context.Context, key, kbID string) (loadedScopeGCMarker, error) {
	loaded := loadedScopeGCMarker{
		object: BlobObjectInfo{Key: key},
		marker: scopeGCMarker{Schema: scopeGCSchema, KBID: kbID, Candidates: make(map[string]time.Time)},
	}
	info, err := k.BlobStore.Head(ctx, key)
	if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return loaded, nil
	}
	if err != nil {
		return loaded, err
	}
	data, err := k.BlobStore.DownloadBytes(ctx, key)
	if err != nil {
		return loaded, err
	}
	if err := json.Unmarshal(data, &loaded.marker); err != nil {
		return loaded, err
	}
	if loaded.marker.Schema != scopeGCSchema || loaded.marker.KBID == "" ||
		(kbID != "" && loaded.marker.KBID != kbID) {
		return loaded, fmt.Errorf("scope GC marker %s has invalid identity", key)
	}
	if loaded.marker.Candidates == nil {
		loaded.marker.Candidates = make(map[string]time.Time)
	}
	loaded.object = *info
	return loaded, nil
}

func (k *KB) saveScopeGCMarker(
	ctx context.Context,
	key string,
	marker scopeGCMarker,
	revision string,
) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	if revision == "" {
		_, err = uploadScopeIfAbsent(ctx, k.BlobStore, key, data)
	} else {
		_, err = k.BlobStore.UploadBytesIfMatch(ctx, key, data, revision)
	}
	return err
}

func scopeGCBatchKey(kbID string, ids []string) string {
	sum := sha256.Sum256([]byte(strings.Join(ids, "\x00")))
	return scopeGCKBPrefix(kbID) + hex.EncodeToString(sum[:]) + ".json"
}

func scopeGCKBPrefix(kbID string) string { return scopeGCPrefix() + scopeHash(kbID) + "/" }
func scopeGCPrefix() string              { return "scope-gc/" }
