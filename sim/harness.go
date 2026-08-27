package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
	"github.com/mikills/minnow/kb/search"
)

var (
	osReadDir   = os.ReadDir
	osRemoveAll = os.RemoveAll
)

// Harness is a seeded, deterministic test rig for minnow. It wires the KB and
// its stores against a FakeClock and a FaultableBlobStore so scenarios can
// drive time forward and inject storage failures while checking invariants.
var harnessRootContext = context.Background()

type Harness struct {
	t    *testing.T
	ctx  context.Context
	seed int64
	rng  *rand.Rand

	clock     *kb.FakeClock
	blobRoot  string
	cacheDir  string
	blobStore *FaultableBlobStore
	manifest  *kb.BlobManifestStore
	kb        *kb.KB
	format    *duckdb.DuckDBArtifactFormat

	mu               sync.Mutex
	ingestedDocs     map[string]map[string]kb.Document // kbID → docID → doc
	lastManifestVers map[string]string                 // kbID → last observed version
	sessions         map[string]string                 // kbID → open session handle
	commitSeq        int

	invariants []Invariant
}

// Option customises a Harness at construction time.
type Option func(*config)

type config struct {
	seed       int64
	startTime  time.Time
	faults     BlobFaults
	invariants []Invariant
}

// WithSeed picks the deterministic RNG seed used for RNG-driven choices and
// for blob fault injection.
func WithSeed(seed int64) Option { return func(c *config) { c.seed = seed } }

// WithBlobFaults configures the faultable blob store.
func WithBlobFaults(f BlobFaults) Option { return func(c *config) { c.faults = f } }

// WithStartTime sets the FakeClock's initial time.
func WithStartTime(t time.Time) Option { return func(c *config) { c.startTime = t } }

// WithInvariants attaches invariants evaluated on AssertInvariants.
func WithInvariants(inv ...Invariant) Option {
	return func(c *config) { c.invariants = append(c.invariants, inv...) }
}

// New constructs a Harness and registers t.Cleanup to release resources.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()

	cfg := config{
		seed:      1,
		startTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, o := range opts {
		o(&cfg)
	}

	clock := kb.NewFakeClock(cfg.startTime)
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	cacheDir := filepath.Join(t.TempDir(), "cache")

	inner := &kb.LocalBlobStore{Root: blobRoot}
	blobs := NewFaultableBlobStore(inner, cfg.faults, rand.New(rand.NewSource(cfg.seed)))
	blobs.UseClock(clock)

	manifest := &kb.BlobManifestStore{Store: blobs}

	embedder, err := kb.NewLocalEmbedder(32)
	require.NoError(t, err)

	eventStore := kb.NewInMemoryEventStore()
	eventStore.SetClock(clock)
	eventInbox := kb.NewInMemoryEventInbox()
	eventInbox.SetClock(clock)
	lease := kb.NewInMemoryWriteLeaseManager()
	lease.SetClock(clock)

	loader := kb.NewKB(blobs, cacheDir,
		kb.WithEmbedder(embedder),
		kb.WithManifestStore(manifest),
		kb.WithClock(clock),
	)
	loader.WriteLeaseManager = lease
	loader.EventStore = eventStore
	loader.EventInbox = eventInbox

	format, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
		BlobStore:                  blobs,
		ManifestStore:              manifest,
		CacheDir:                   cacheDir,
		MemoryLimit:                "256MB",
		ShardingPolicy:             loader.ShardingPolicy,
		Embed:                      loader.Embed,
		GraphBuilder:               harnessGraphBuilder(loader),
		EvictCacheIfNeeded:         loader.EvictCacheIfNeeded,
		LockFor:                    loader.LockFor,
		AcquireWriteLease:          loader.AcquireWriteLease,
		EnqueueReplacedShardsForGC: loader.EnqueueReplacedShardsForGC,
		ReconcileShardBlobs: func(ctx context.Context, kbID string, active []kb.SnapshotShardMetadata) error {
			return loader.EnqueueOrphanedShardBlobs(ctx, kbID, active, time.Time{})
		},
		Metrics: loader,
	})
	require.NoError(t, err)
	require.NoError(t, loader.RegisterFormat(format))

	h := &Harness{
		t:                t,
		ctx:              harnessRootContext,
		seed:             cfg.seed,
		rng:              rand.New(rand.NewSource(cfg.seed)),
		clock:            clock,
		blobRoot:         blobRoot,
		cacheDir:         cacheDir,
		blobStore:        blobs,
		manifest:         manifest,
		kb:               loader,
		format:           format,
		ingestedDocs:     make(map[string]map[string]kb.Document),
		lastManifestVers: make(map[string]string),
		sessions:         make(map[string]string),
		invariants:       cfg.invariants,
	}
	t.Cleanup(h.close)
	return h
}

func harnessGraphBuilder(loader *kb.KB) func() *kb.GraphBuilder {
	return func() *kb.GraphBuilder { return loader.GraphBuilder }
}

func (h *Harness) close() {
	if err := h.format.Close(); err != nil {
		h.t.Errorf("close DuckDB format: %v", err)
	}
}

// Seed returns the RNG seed this harness was constructed with. Useful in
// failure messages so a reviewer can reproduce the run.
func (h *Harness) Seed() int64 { return h.seed }

func (h *Harness) Context() context.Context { return h.ctx }

// Clock exposes the FakeClock so scenarios can advance time directly.
func (h *Harness) Clock() *kb.FakeClock { return h.clock }

// SetBlobFaults replaces the active fault config mid-scenario.
func (h *Harness) SetBlobFaults(f BlobFaults) { h.blobStore.SetFaults(f) }

// GenerateDocs returns n deterministic documents seeded from the harness RNG.
// Each doc's text is "<kbID>-<seed>-<i>" so re-runs with the same seed produce
// identical documents and identical embeddings.
func (h *Harness) GenerateDocs(kbID string, n int) []kb.Document {
	out := make([]kb.Document, n)
	for i := range n {
		id := fmt.Sprintf("%s-%d-%05d", kbID, h.seed, i)
		out[i] = kb.Document{ID: id, Text: id + " synthetic content for sim"}
	}
	return out
}

// RandomVec returns a pseudo-random unit-length vector of the requested dim.
// Safe to call from multiple goroutines. rng access is serialised.
func (h *Harness) RandomVec(dim int) []float32 {
	vec := make([]float32, dim)
	h.mu.Lock()
	for i := range vec {
		vec[i] = float32(h.rng.NormFloat64())
	}
	h.mu.Unlock()
	return normalize(vec)
}

func normalize(vec []float32) []float32 {
	var sum float64
	for _, v := range vec {
		sum += float64(v) * float64(v)
	}
	if sum == 0 {
		return vec
	}
	n := float32(1.0 / (sum * 0.5))
	for i := range vec {
		vec[i] *= n
	}
	return vec
}

// Ingest upserts docs into kbID. On success the harness records the
// ingested doc IDs (for the no-doc-loss invariant) and the resulting
// manifest version (for the monotonic-manifest invariant). Scenarios no
// longer need to call RecordManifestVersion manually after a successful
// Ingest. doing so is still safe and idempotent.
func (h *Harness) Ingest(kbID string, docs []kb.Document) error {
	if err := h.kb.UpsertDocsAndUpload(h.ctx, kbID, docs); err != nil {
		return err
	}
	h.recordDocs(kbID, docs)
	h.RecordManifestVersion(kbID)
	return nil
}

func (h *Harness) recordDocs(kbID string, docs []kb.Document) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.ingestedDocs[kbID]
	if !ok {
		m = make(map[string]kb.Document)
		h.ingestedDocs[kbID] = m
	}
	for _, d := range docs {
		m[d.ID] = d
	}
}

// DeferIngest writes a batch that stays out of the manifest until Commit,
// holding the session the way the ingest handler does.
func (h *Harness) DeferIngest(kbID string, docs []kb.Document) error {
	handle, err := h.kb.IngestSessionsFor().Hold(h.ctx, kbID, h.session(kbID))
	if err != nil {
		return err
	}
	h.setSession(kbID, handle)
	embedded := make([]kb.EmbeddedDocument, 0, len(docs))
	for _, d := range docs {
		vector, err := h.Embed(h.ctx, d.Text)
		if err != nil {
			return err
		}
		embedded = append(embedded, kb.EmbeddedDocument{
			ID: d.ID, Text: d.Text, Metadata: d.Metadata, Embedding: vector,
		})
	}
	if err := h.kb.PublishPreparedDocs(
		h.ctx, kbID, embedded, nil, kb.UpsertDocsOptions{DeferPublish: true},
	); err != nil {
		return err
	}
	h.recordDocs(kbID, docs)
	return nil
}

// Commit publishes what a session accumulated, through the same worker the
// queued commit event runs.
func (h *Harness) Commit(kbID string) error {
	sessions := h.kb.IngestSessionsFor()
	handle, err := sessions.Renew(h.ctx, kbID, h.session(kbID))
	if err != nil {
		return err
	}
	payload, err := json.Marshal(kb.SessionCommitPayload{KBID: kbID, SessionID: handle})
	if err != nil {
		return err
	}
	worker := &kb.SessionCommitWorker{
		KB: h.kb, ID: "sim-session-commit", ReleaseSession: sessions.Release,
	}
	event := &kb.KBEvent{
		EventID: h.nextCommitID(kbID), Kind: kb.EventSessionCommit,
		KBID: kbID, Payload: payload, Attempt: 1,
	}
	if _, err := worker.Handle(h.ctx, event); err != nil {
		return err
	}
	h.setSession(kbID, "")
	h.RecordManifestVersion(kbID)
	return nil
}

// nextCommitID gives every commit its own id, or the worker mistakes a retry
// for a redelivery of a finished commit.
func (h *Harness) nextCommitID(kbID string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commitSeq++
	return fmt.Sprintf("sim-commit-%s-%d", kbID, h.commitSeq)
}

func (h *Harness) session(kbID string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessions[kbID]
}

func (h *Harness) setSession(kbID, handle string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if handle == "" {
		delete(h.sessions, kbID)
		return
	}
	h.sessions[kbID] = handle
}

// ReapSessions publishes deferred writes no client still holds. It re-records
// every version, since the reaper does not say which ones it published.
func (h *Harness) ReapSessions() (int, error) {
	reaped, err := h.kb.ReapAbandonedSessions(h.ctx)
	for _, kbID := range h.KBIDs() {
		h.RecordManifestVersion(kbID)
	}
	return reaped, err
}

// HasPendingSession reports whether kbID holds rows that are not in the
// published manifest.
func (h *Harness) HasPendingSession(kbID string) bool {
	return kb.HasPendingSession(filepath.Join(h.cacheDir, kbID))
}

// HoldSession opens a session as a client would, modelling a run in flight.
func (h *Harness) HoldSession(kbID string) (string, error) {
	handle, err := h.kb.IngestSessionsFor().Hold(h.ctx, kbID, h.session(kbID))
	if err != nil {
		return "", err
	}
	h.setSession(kbID, handle)
	return handle, nil
}

// Query runs a top-k vector query against kbID.
func (h *Harness) Query(kbID string, vec []float32, k int) ([]kb.ExpandedResult, error) {
	return h.Search(kbID, vec, k)
}

// Search runs a top-k vector search against kbID.
func (h *Harness) Search(kbID string, vec []float32, k int) ([]kb.ExpandedResult, error) {
	return h.format.QueryRag(h.ctx, kb.RagQueryRequest{
		KBID:     kbID,
		QueryVec: vec,
		Options:  kb.RagQueryOptions{TopK: k},
	})
}

// SearchWithFilter runs a filtered top-k vector search against kbID.
func (h *Harness) SearchWithFilter(kbID string, vec []float32, k int, filter *search.FilterExpr) ([]kb.ExpandedResult, error) {
	return h.format.QueryRag(h.ctx, kb.RagQueryRequest{
		KBID:     kbID,
		QueryVec: vec,
		Options:  kb.RagQueryOptions{TopK: k, Filter: filter},
	})
}

// ManifestVersion returns the current manifest version for kbID, or "" if the
// KB has no manifest yet.
func (h *Harness) ManifestVersion(kbID string) string {
	v, err := h.manifest.HeadVersion(h.ctx, kbID)
	if err != nil {
		return ""
	}
	return v
}

// IngestedDocIDs returns a snapshot of the doc IDs ingested into kbID so far.
func (h *Harness) IngestedDocIDs(kbID string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.ingestedDocs[kbID]))
	for id := range h.ingestedDocs[kbID] {
		ids = append(ids, id)
	}
	return ids
}

// KB returns the underlying KB for scenarios that need deeper access.
func (h *Harness) KB() *kb.KB { return h.kb }

// Format returns the DuckDB artifact format.
func (h *Harness) Format() *duckdb.DuckDBArtifactFormat { return h.format }

// Ctx returns the base context. Scenarios wanting a timeout should derive
// their own via context.WithTimeout.
func (h *Harness) Ctx() context.Context { return h.ctx }

// Fatalf delegates to the underlying *testing.T so scenarios in other
// packages can halt the run on unrecoverable state without reaching into t.
func (h *Harness) Fatalf(format string, args ...any) {
	h.t.Helper()
	h.t.Fatalf(format, args...)
}

// Errorf delegates to the underlying *testing.T for non-fatal failures.
func (h *Harness) Errorf(format string, args ...any) {
	h.t.Helper()
	h.t.Errorf(format, args...)
}

// Embed runs the KB's embedder on text. Provided so scenarios can build
// probe vectors deterministically from known inputs.
func (h *Harness) Embed(ctx context.Context, text string) ([]float32, error) {
	return h.kb.Embed(ctx, text)
}

// RecordManifestVersion captures the current manifest version for kbID so
// the ManifestMonotonic invariant can compare future writes against it.
// Scenarios should call this after any successful ingest.
func (h *Harness) RecordManifestVersion(kbID string) {
	v, err := h.manifest.HeadVersion(h.Ctx(), kbID)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.lastManifestVers[kbID] = v
	h.mu.Unlock()
}

// WipeCache removes every file under the cache directory, forcing the next
// query to re-download shards from the blob store. Mimics a hostile eviction
// or a pod-restart with an ephemeral cache volume.
func (h *Harness) WipeCache() error {
	// Close pooled handles before unlinking their files so disk blocks and
	// database/sql goroutines are released immediately.
	if err := h.format.Close(); err != nil {
		return err
	}
	entries, err := osReadDir(h.cacheDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := osRemoveAll(filepath.Join(h.cacheDir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// BlobKeys lists every blob key whose path starts with prefix. Used by
// invariants that need to compare manifests against on-disk blobs.
func (h *Harness) BlobKeys(prefix string) ([]string, error) {
	infos, err := h.blobStore.List(h.ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(infos))
	for _, info := range infos {
		keys = append(keys, info.Key)
	}
	return keys, nil
}

// KBIDs returns the set of KB IDs the harness has ingested into so far.
func (h *Harness) KBIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.ingestedDocs))
	for id := range h.ingestedDocs {
		ids = append(ids, id)
	}
	return ids
}

// ManifestShards returns the shard metadata for kbID from the current manifest,
// or nil if the KB has no manifest.
func (h *Harness) ManifestShards(ctx context.Context, kbID string) ([]kb.SnapshotShardMetadata, error) {
	doc, err := h.manifest.Get(ctx, kbID)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return doc.Manifest.Shards, nil
}

// AssertInvariants runs all registered invariants and reports failures via
// t.Errorf (non-fatal so the full batch runs). Returns the number of
// violations.
func (h *Harness) AssertInvariants() int {
	h.t.Helper()
	violations := 0
	for _, inv := range h.invariants {
		if err := inv.Check(h); err != nil {
			h.t.Errorf("invariant %q violated (seed=%d): %v", inv.Name(), h.seed, err)
			violations++
		}
	}
	return violations
}
