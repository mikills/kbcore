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
	"sync"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

const scopeSchema = "minnow.scope/v1"
const scopeCacheLimit = 16

type Scope struct {
	KBID        string    `json:"kb_id"`
	ScopeID     string    `json:"scope_id"`
	DocumentIDs []string  `json:"document_ids"`
	UpdatedAt   time.Time `json:"updated_at"`
	Revision    string    `json:"revision"`
}

func (k *KB) ReplaceScope(
	ctx context.Context,
	kbID, scopeID string,
	ids []string,
	expectedRevision string,
) (Scope, error) {
	if k == nil || k.BlobStore == nil {
		return Scope{}, fmt.Errorf("blob store is not configured")
	}
	kbID = strings.TrimSpace(kbID)
	scopeID = strings.TrimSpace(scopeID)
	if kbID == "" || scopeID == "" {
		return Scope{}, fmt.Errorf("kb_id and scope_id are required")
	}
	ids = normalizeScopeIDs(ids)
	now := time.Now().UTC()
	if k.Clock != nil {
		now = k.Clock.Now().UTC()
	}
	doc := struct {
		Schema string `json:"schema"`
		Scope
	}{Schema: scopeSchema, Scope: Scope{
		KBID: kbID, ScopeID: scopeID, DocumentIDs: ids, UpdatedAt: now,
	}}
	data, err := json.Marshal(doc)
	if err != nil {
		return Scope{}, err
	}
	mutation, err := k.acquireScopeMutation(ctx, kbID)
	if err != nil {
		return Scope{}, err
	}
	ctx = mutation.Context()
	if err := k.ensureScopeDocumentsExist(ctx, kbID, ids); err != nil {
		return Scope{}, errors.Join(err, mutation.Close())
	}
	previous, previousErr := k.GetScope(ctx, kbID, scopeID)
	if previousErr != nil && !errors.Is(previousErr, ErrScopeNotFound) {
		return Scope{}, errors.Join(previousErr, mutation.Close())
	}
	if err := k.scheduleScopeGCLocked(ctx, kbID, scopeDifference(previous.DocumentIDs, ids), now); err != nil {
		return Scope{}, errors.Join(err, mutation.Close())
	}
	var info *BlobObjectInfo
	if expectedRevision == "" {
		info, err = uploadScopeIfAbsent(ctx, k.BlobStore, scopeKey(kbID, scopeID), data)
	} else {
		info, err = k.BlobStore.UploadBytesIfMatch(
			ctx, scopeKey(kbID, scopeID), data, expectedRevision,
		)
	}
	if err != nil {
		return Scope{}, errors.Join(err, mutation.Close())
	}
	if err := mutation.Close(); err != nil {
		return Scope{}, err
	}
	doc.Revision = info.Version
	k.cacheScope(doc.Scope)
	return doc.Scope, nil
}

func (k *KB) ensureScopeDocumentsExist(ctx context.Context, kbID string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	format, err := k.resolveSearchFormat(ctx, kbID)
	if err != nil {
		return err
	}
	for start := 0; start < len(ids); start += 500 {
		end := min(start+500, len(ids))
		records, err := format.FetchVectors(ctx, kbID, ids[start:end])
		if err != nil {
			return err
		}
		if len(records) != end-start {
			return fmt.Errorf("%w: requested %d, found %d", ErrScopeDocumentsMissing, end-start, len(records))
		}
	}
	return nil
}

type scopeMutation struct {
	ctx     context.Context
	cancel  context.CancelFunc
	manager WriteLeaseManager
	lease   *WriteLease
	ttl     time.Duration
	lock    *sync.Mutex
	done    chan struct{}
	wg      sync.WaitGroup
	errMu   sync.Mutex
	err     error
	once    sync.Once
}

func (k *KB) acquireScopeMutation(ctx context.Context, kbID string) (*scopeMutation, error) {
	var hash uint32 = 2166136261
	for i := 0; i < len(kbID); i++ {
		hash ^= uint32(kbID[i])
		hash *= 16777619
	}
	lock := &k.scopeLocks[hash%uint32(len(k.scopeLocks))]
	lock.Lock()
	manager, ttl := k.writeLeaseManagerAndTTL()
	held, err := manager.Acquire(ctx, "scope:"+kbID, ttl)
	if err != nil {
		lock.Unlock()
		return nil, err
	}
	mutationCtx, cancel := context.WithCancel(ctx)
	mutation := &scopeMutation{
		ctx: mutationCtx, cancel: cancel, manager: manager, lease: held,
		ttl: ttl, lock: lock, done: make(chan struct{}),
	}
	mutation.wg.Add(1)
	go mutation.renew()
	return mutation, nil
}

func (m *scopeMutation) Context() context.Context { return m.ctx }

func (m *scopeMutation) renew() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			renewed, err := m.manager.Renew(m.ctx, m.lease, m.ttl)
			if err != nil {
				// Close cancels this context, and a tick racing it would
				// otherwise fail a mutation that already succeeded.
				if m.ctx.Err() != nil {
					return
				}
				m.errMu.Lock()
				m.err = fmt.Errorf("renew scope mutation lease: %w", err)
				m.errMu.Unlock()
				m.cancel()
				return
			}
			m.lease = renewed
		case <-m.done:
			return
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *scopeMutation) Close() error {
	m.once.Do(func() {
		close(m.done)
		m.cancel()
		m.wg.Wait()
		releaseErr := m.manager.Release(context.Background(), m.lease)
		m.errMu.Lock()
		m.err = errors.Join(m.err, releaseErr)
		m.errMu.Unlock()
		m.lock.Unlock()
	})
	m.errMu.Lock()
	defer m.errMu.Unlock()
	return m.err
}

func (k *KB) ensureDocumentsUnscoped(ctx context.Context, kbID string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	scopes, err := k.ListScopes(ctx, kbID)
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		for _, id := range scope.DocumentIDs {
			if _, ok := wanted[id]; ok {
				return fmt.Errorf("%w: %s is referenced by %s", ErrScopedDocuments, id, scope.ScopeID)
			}
		}
	}
	return nil
}

func uploadScopeIfAbsent(ctx context.Context, store BlobStore, key string, data []byte) (*BlobObjectInfo, error) {
	creator, ok := store.(interface {
		UploadIfNotExists(context.Context, string, string) (*BlobObjectInfo, error)
	})
	if !ok {
		if _, err := store.Head(ctx, key); err == nil {
			return nil, ErrBlobVersionMismatch
		} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrBlobNotFound) {
			return nil, err
		}
		return store.UploadBytesIfMatch(ctx, key, data, "")
	}
	file, err := os.CreateTemp("", "minnow-scope-*")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.Write(data); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return creator.UploadIfNotExists(ctx, key, path)
}

func (k *KB) GetScope(ctx context.Context, kbID, scopeID string) (Scope, error) {
	if k == nil || k.BlobStore == nil {
		return Scope{}, fmt.Errorf("blob store is not configured")
	}
	key := scopeKey(kbID, scopeID)
	info, err := k.BlobStore.Head(ctx, key)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return Scope{}, fmt.Errorf("scope %q: %w", scopeID, ErrScopeNotFound)
		}
		return Scope{}, err
	}
	if cached, ok := k.cachedScope(kbID, scopeID, info.Version); ok {
		return cached, nil
	}
	data, err := k.BlobStore.DownloadBytes(ctx, key)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return Scope{}, fmt.Errorf("scope %q: %w", scopeID, ErrScopeNotFound)
		}
		return Scope{}, err
	}
	var doc struct {
		Schema string `json:"schema"`
		Scope
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return Scope{}, fmt.Errorf("read scope %q: %w", scopeID, err)
	}
	if doc.Schema != scopeSchema || doc.KBID != kbID || doc.ScopeID != scopeID {
		return Scope{}, fmt.Errorf("scope %q has invalid identity", scopeID)
	}
	doc.DocumentIDs = normalizeScopeIDs(doc.DocumentIDs)
	doc.Revision = info.Version
	k.cacheScope(doc.Scope)
	return doc.Scope, nil
}

func (k *KB) cachedScope(kbID, scopeID, revision string) (Scope, bool) {
	k.scopeCacheMu.RLock()
	scope, ok := k.scopeCache[kbID+"\x00"+scopeID]
	k.scopeCacheMu.RUnlock()
	if !ok || scope.Revision != revision {
		return Scope{}, false
	}
	scope.DocumentIDs = append([]string(nil), scope.DocumentIDs...)
	return scope, true
}

func (k *KB) cacheScope(scope Scope) {
	k.scopeCacheMu.Lock()
	if k.scopeCache == nil {
		k.scopeCache = make(map[string]Scope)
	}
	key := scope.KBID + "\x00" + scope.ScopeID
	if len(k.scopeCache) >= scopeCacheLimit {
		for cached := range k.scopeCache {
			if cached != key {
				delete(k.scopeCache, cached)
				break
			}
		}
	}
	scope.DocumentIDs = append([]string(nil), scope.DocumentIDs...)
	k.scopeCache[key] = scope
	k.scopeCacheMu.Unlock()
}

func (k *KB) DeleteScope(ctx context.Context, kbID, scopeID string) error {
	return k.deleteScope(ctx, kbID, scopeID, "")
}

func (k *KB) DeleteScopeIfRevision(ctx context.Context, kbID, scopeID, expectedRevision string) error {
	if strings.TrimSpace(expectedRevision) == "" {
		return fmt.Errorf("scope revision is required")
	}
	return k.deleteScope(ctx, kbID, scopeID, expectedRevision)
}

func (k *KB) deleteScope(ctx context.Context, kbID, scopeID, expectedRevision string) error {
	if k == nil || k.BlobStore == nil {
		return fmt.Errorf("blob store is not configured")
	}
	kbID = strings.TrimSpace(kbID)
	scopeID = strings.TrimSpace(scopeID)
	if kbID == "" || scopeID == "" {
		return fmt.Errorf("kb_id and scope_id are required")
	}
	mutation, err := k.acquireScopeMutation(ctx, kbID)
	if err != nil {
		return err
	}
	mutationCtx := mutation.Context()
	key := scopeKey(kbID, scopeID)
	current, getErr := k.GetScope(mutationCtx, kbID, scopeID)
	switch {
	case errors.Is(getErr, ErrScopeNotFound) && expectedRevision == "":
		return mutation.Close()
	case errors.Is(getErr, ErrScopeNotFound):
		err = ErrScopeNotFound
	case getErr != nil:
		err = getErr
	case expectedRevision != "" && current.Revision != expectedRevision:
		err = ErrBlobVersionMismatch
	}
	if err == nil {
		now := time.Now().UTC()
		if k.Clock != nil {
			now = k.Clock.Now().UTC()
		}
		err = k.scheduleScopeGCLocked(mutationCtx, kbID, current.DocumentIDs, now)
	}
	if err == nil {
		err = k.BlobStore.Delete(mutationCtx, key)
	}
	if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	err = errors.Join(err, mutation.Close())
	if err == nil {
		k.scopeCacheMu.Lock()
		delete(k.scopeCache, kbID+"\x00"+scopeID)
		k.scopeCacheMu.Unlock()
	}
	return err
}

func scopeDifference(previous, next []string) []string {
	nextSet := make(map[string]struct{}, len(next))
	for _, id := range next {
		nextSet[id] = struct{}{}
	}
	removed := make([]string, 0)
	for _, id := range previous {
		if _, kept := nextSet[id]; !kept {
			removed = append(removed, id)
		}
	}
	return removed
}

func (k *KB) ListScopes(ctx context.Context, kbID string) ([]Scope, error) {
	if k == nil || k.BlobStore == nil {
		return nil, fmt.Errorf("blob store is not configured")
	}
	objects, err := k.BlobStore.List(ctx, scopePrefix(kbID))
	if err != nil {
		return nil, err
	}
	scopes := make([]Scope, 0, len(objects))
	for _, object := range objects {
		data, err := k.BlobStore.DownloadBytes(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		var doc struct {
			Schema string `json:"schema"`
			Scope
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("read scope %s: %w", object.Key, err)
		}
		if doc.Schema != scopeSchema || doc.KBID != kbID || doc.ScopeID == "" {
			return nil, fmt.Errorf("scope %s has invalid identity", object.Key)
		}
		doc.DocumentIDs = normalizeScopeIDs(doc.DocumentIDs)
		doc.Revision = object.Version
		scopes = append(scopes, doc.Scope)
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].ScopeID < scopes[j].ScopeID })
	return scopes, nil
}

func normalizeScopeIDs(ids []string) []string {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			set[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func scopeKey(kbID, scopeID string) string {
	return "scopes/" + scopeHash(kbID) + "/" + scopeHash(scopeID) + ".json"
}

func scopePrefix(kbID string) string { return "scopes/" + scopeHash(kbID) + "/" }

func scopeHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (k *KB) deleteKBScopes(ctx context.Context, kbID string) []error {
	objects, err := k.BlobStore.List(ctx, scopePrefix(kbID))
	if err != nil {
		return []error{fmt.Errorf("list scopes: %w", err)}
	}
	errs := make([]error, 0)
	for _, object := range objects {
		if err := k.BlobStore.Delete(ctx, object.Key); err != nil {
			errs = append(errs, fmt.Errorf("delete scope %s: %w", object.Key, err))
		}
	}
	k.scopeCacheMu.Lock()
	for key := range k.scopeCache {
		if strings.HasPrefix(key, kbID+"\x00") {
			delete(k.scopeCache, key)
		}
	}
	k.scopeCacheMu.Unlock()
	markers, err := k.BlobStore.List(ctx, scopeGCKBPrefix(kbID))
	if err != nil {
		errs = append(errs, fmt.Errorf("list scope GC markers: %w", err))
	}
	for _, marker := range markers {
		if err := k.BlobStore.Delete(ctx, marker.Key); err != nil {
			errs = append(errs, fmt.Errorf("delete scope GC marker: %w", err))
		}
	}
	return errs
}
