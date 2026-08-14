package tiered_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/blobstore/localjournal"
	"github.com/mikills/minnow/kb/blobstore/tiered"
	"github.com/stretchr/testify/require"
)

func TestTieredLocalJournalSurvivesRestartAndDrains(t *testing.T) {
	ctx := context.Background()
	journalDir := t.TempDir()
	remote := newMemoryRemote()

	first := newTiered(t, remote, journalDir, tiered.DurabilityLocalJournal)
	require.NoError(t, first.Start(ctx))
	remote.setUnavailable(true)
	info, err := first.UploadBytesIfMatch(ctx, "kb/shard", []byte("durable-local"), "")
	require.NoError(t, err)
	require.NotEmpty(t, info.Version)
	data, err := first.DownloadBytes(ctx, "kb/shard")
	require.NoError(t, err)
	require.Equal(t, []byte("durable-local"), data)
	require.NoError(t, first.Stop(ctx))

	remote.setUnavailable(false)
	second := newTiered(t, remote, journalDir, tiered.DurabilityLocalJournal)
	require.NoError(t, second.Start(ctx))
	require.Eventually(t, func() bool {
		data, err := remote.DownloadBytes(ctx, "kb/shard")
		return err == nil && string(data) == "durable-local"
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		stats, err := second.Stats(ctx)
		return err == nil && stats.PendingEntries == 0
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, second.Stop(ctx))
}

func TestTieredRemoteDurabilityWaitsAndPreservesGlobalOrder(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	store := newTiered(t, remote, t.TempDir(), tiered.DurabilityRemote)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)

	shard, err := store.UploadBytesIfMatch(ctx, "kb/shard", []byte("shard"), "")
	require.NoError(t, err)
	_, err = store.UploadBytesIfMatch(ctx, "kb/manifest", []byte("manifest"), "")
	require.NoError(t, err)
	_, err = store.UploadBytesIfMatch(ctx, "kb/shard", []byte("shard-v2"), shard.Version)
	require.NoError(t, err)
	_, err = store.UploadBytesIfMatch(ctx, "kb/obsolete", []byte("obsolete"), "")
	require.NoError(t, err)
	require.NoError(t, store.Delete(ctx, "kb/obsolete"))

	require.Equal(t, []string{"put:kb/shard", "put:kb/manifest", "put:kb/shard", "put:kb/obsolete", "delete:kb/obsolete"}, remote.operations())
	stats, err := store.Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.PendingEntries)
	metrics, err := store.ReplicationOpenMetrics(ctx)
	require.NoError(t, err)
	require.Contains(t, metrics, "minnow_replication_pending_entries 0")
}

func TestTieredTombstoneHidesRemoteObjectDuringOutage(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	_, err := remote.UploadBytesIfMatch(ctx, "object", []byte("old"), "")
	require.NoError(t, err)
	store := newTiered(t, remote, t.TempDir(), tiered.DurabilityLocalJournal)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)

	_, err = store.Head(ctx, "object") // seed remote version
	require.NoError(t, err)
	remote.setUnavailable(true)
	require.NoError(t, store.Delete(ctx, "object"))
	_, err = store.Head(ctx, "object")
	require.ErrorIs(t, err, blobstore.ErrNotFound)
	_, err = store.DownloadBytes(ctx, "object")
	require.ErrorIs(t, err, blobstore.ErrNotFound)
}

func TestTieredReconcilesRemoteSuccessWithLostResponse(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	remote.mu.Lock()
	remote.succeedThenError = true
	remote.mu.Unlock()
	store := newTiered(t, remote, t.TempDir(), tiered.DurabilityRemote)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)

	_, err := store.UploadBytesIfMatch(ctx, "object", []byte("value"), "")
	require.NoError(t, err)
	stats, err := store.Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.PendingEntries)
	data, err := remote.DownloadBytes(ctx, "object")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), data)
}

func TestTieredRemoteReadsWaitForRemoteApplyToBecomeJournalVisible(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	base := localjournal.New(t.TempDir(), journal.Config{})
	blocking := &blockingMarkJournal{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	store, err := tiered.New(remote, blocking, testConfig(tiered.DurabilityRemote))
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)
	uploadDone := make(chan error, 1)
	go func() {
		_, uploadErr := store.UploadBytesIfMatch(ctx, "object", []byte("value"), "")
		uploadDone <- uploadErr
	}()
	<-blocking.entered
	readDone := make(chan error, 1)
	go func() {
		_, readErr := store.Head(ctx, "object")
		readDone <- readErr
	}()
	select {
	case err := <-readDone:
		require.Failf(t, "remote read passed apply/mark visibility barrier", "error=%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(blocking.release)
	require.NoError(t, <-uploadDone)
	require.NoError(t, <-readDone)
}

func TestTieredRemoteModeDoesNotExposeDefinitiveCASFailure(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	initial, err := remote.UploadBytesIfMatch(ctx, "object", []byte("initial"), "")
	require.NoError(t, err)
	cfg := testConfig(tiered.DurabilityRemote)
	cfg.MaxAttempts = 1
	store, err := tiered.New(remote, localjournal.New(t.TempDir(), journal.Config{}), cfg)
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)

	external, err := remote.UploadBytesIfMatch(ctx, "object", []byte("same-bytes-as-local-attempt"), initial.Version)
	require.NoError(t, err)
	_, err = store.UploadBytesIfMatch(ctx, "object", []byte("same-bytes-as-local-attempt"), initial.Version)
	require.Error(t, err)
	_, err = store.DownloadBytes(ctx, "object")
	require.ErrorContains(t, err, "remote visibility is blocked")
	_, err = store.List(ctx, "")
	require.ErrorContains(t, err, "remote visibility is blocked")
	remoteInfo, err := remote.Head(ctx, "object")
	require.NoError(t, err)
	require.Equal(t, external.Version, remoteInfo.Version)
}

func TestTieredRejectsCorruptJournalPayloadBeforeReplication(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	local := localjournal.New(t.TempDir(), journal.Config{})
	cfg := testConfig(tiered.DurabilityLocalJournal)
	cfg.MaxAttempts = 2
	cfg.RetryBase = 100 * time.Millisecond
	cfg.RetryMax = 100 * time.Millisecond
	store, err := tiered.New(remote, local, cfg)
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)
	remote.setUnavailable(true)
	_, err = store.UploadBytesIfMatch(ctx, "object", []byte("original"), "")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		remote.mu.Lock()
		defer remote.mu.Unlock()
		return remote.replicaAttempts >= 1
	}, time.Second, time.Millisecond)
	entry, err := local.Next(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	reader, err := local.OpenPayload(ctx, entry)
	require.NoError(t, err)
	path := reader.(*os.File).Name()
	require.NoError(t, reader.Close())
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))
	remote.setUnavailable(false)
	require.Eventually(t, func() bool {
		stats, err := store.Stats(ctx)
		return err == nil && stats.FailedEntries == 1
	}, time.Second, 5*time.Millisecond)
	_, err = remote.Head(ctx, "object")
	require.ErrorIs(t, err, blobstore.ErrNotFound)
}

func TestTieredReservesRemoteControlPrefix(t *testing.T) {
	ctx := context.Background()
	store := newTiered(t, newMemoryRemote(), t.TempDir(), tiered.DurabilityLocalJournal)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)
	_, err := store.UploadBytesIfMatch(ctx, "leases/user-data", []byte("value"), "")
	require.ErrorContains(t, err, "reserved control prefix")
	_, err = store.Head(ctx, "leases/user-data")
	require.ErrorIs(t, err, blobstore.ErrNotFound)
}

func TestTieredSupportsCreateOnlyStaging(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	store := newTiered(t, remote, t.TempDir(), tiered.DurabilityRemote)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)

	_, err := store.UploadBytesIfNotExists(ctx, "staging/id", []byte("one"))
	require.NoError(t, err)
	_, err = store.UploadBytesIfNotExists(ctx, "staging/id", []byte("two"))
	require.ErrorIs(t, err, blobstore.ErrVersionMismatch)
}

func TestTieredStopRetriesRemoteOwnershipRelease(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	store, err := tiered.New(remote, localjournal.New(t.TempDir(), journal.Config{}), testConfig(tiered.DurabilityLocalJournal))
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	remote.mu.Lock()
	remote.releaseOwnerErr = errors.New("temporary release failure")
	remote.mu.Unlock()
	require.ErrorContains(t, store.Stop(ctx), "temporary release failure")
	remote.mu.Lock()
	remote.releaseOwnerErr = nil
	remote.mu.Unlock()
	require.NoError(t, store.Stop(ctx))
	remote.mu.Lock()
	require.GreaterOrEqual(t, remote.releaseOwnerCalls, 2)
	remote.mu.Unlock()
}

func TestTieredConcurrentStartsDoNotCloseTheWinningJournal(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	store := newTiered(t, remote, t.TempDir(), tiered.DurabilityLocalJournal)
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errorsSeen <- store.Start(ctx)
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		require.NoError(t, err)
	}
	_, err := store.UploadBytesIfMatch(ctx, "still-open", []byte("value"), "")
	require.NoError(t, err)
	require.NoError(t, store.Stop(ctx))
}

func TestTieredStopWaitsForAdmittedJournalCommitBeforeOwnershipDecision(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	base := localjournal.New(t.TempDir(), journal.Config{})
	blocking := &blockingPutJournal{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	store, err := tiered.New(remote, blocking, testConfig(tiered.DurabilityLocalJournal))
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	remote.setUnavailable(true)
	uploadDone := make(chan error, 1)
	go func() {
		_, uploadErr := store.UploadBytesIfMatch(ctx, "pending", []byte("value"), "")
		uploadDone <- uploadErr
	}()
	<-blocking.entered
	stopDone := make(chan error, 1)
	go func() { stopDone <- store.Stop(ctx) }()
	select {
	case err := <-stopDone:
		require.Failf(t, "Stop returned before admitted enqueue committed", "error=%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(blocking.release)
	require.NoError(t, <-uploadDone)
	require.NoError(t, <-stopDone)
	remote.setUnavailable(false)
	independent := newTiered(t, remote, t.TempDir(), tiered.DurabilityLocalJournal)
	require.ErrorIs(t, independent.Start(ctx), blobstore.ErrVersionMismatch)
}

func TestTieredPendingBacklogKeepsRemoteOwnershipForSameJournalRestart(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	dir := t.TempDir()
	first := newTiered(t, remote, dir, tiered.DurabilityLocalJournal)
	require.NoError(t, first.Start(ctx))
	remote.setUnavailable(true)
	_, err := first.UploadBytesIfMatch(ctx, "pending", []byte("value"), "")
	require.NoError(t, err)
	require.NoError(t, first.Stop(ctx))

	remote.setUnavailable(false)
	independent := newTiered(t, remote, t.TempDir(), tiered.DurabilityLocalJournal)
	require.ErrorIs(t, independent.Start(ctx), blobstore.ErrVersionMismatch)

	restarted := newTiered(t, remote, dir, tiered.DurabilityLocalJournal)
	require.NoError(t, restarted.Start(ctx))
	require.Eventually(t, func() bool {
		stats, err := restarted.Stats(ctx)
		return err == nil && stats.PendingEntries == 0
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, restarted.Stop(ctx))
}

func TestTieredRestartPreservesLogicalCASTokenForUnchangedObject(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	dir := t.TempDir()
	first := newTiered(t, remote, dir, tiered.DurabilityRemote)
	require.NoError(t, first.Start(ctx))
	created, err := first.UploadBytesIfMatch(ctx, "object", []byte("value"), "")
	require.NoError(t, err)
	require.NoError(t, first.Stop(ctx))
	restarted := newTiered(t, remote, dir, tiered.DurabilityRemote)
	require.NoError(t, restarted.Start(ctx))
	head, err := restarted.Head(ctx, "object")
	require.NoError(t, err)
	require.Equal(t, created.Version, head.Version)
	require.NoError(t, restarted.Stop(ctx))
}

func TestTieredStartupReconcilesReplicatedCatalogAfterOwnershipHandoff(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	firstDir := t.TempDir()
	first := newTiered(t, remote, firstDir, tiered.DurabilityRemote)
	require.NoError(t, first.Start(ctx))
	_, err := first.UploadBytesIfMatch(ctx, "object", []byte("first"), "")
	require.NoError(t, err)
	require.NoError(t, first.Stop(ctx))

	second := newTiered(t, remote, t.TempDir(), tiered.DurabilityRemote)
	require.NoError(t, second.Start(ctx))
	current, err := second.Head(ctx, "object")
	require.NoError(t, err)
	_, err = second.UploadBytesIfMatch(ctx, "object", []byte("second"), current.Version)
	require.NoError(t, err)
	require.NoError(t, second.Stop(ctx))

	restarted := newTiered(t, remote, firstDir, tiered.DurabilityRemote)
	require.NoError(t, restarted.Start(ctx))
	data, err := restarted.DownloadBytes(ctx, "object")
	require.NoError(t, err)
	require.Equal(t, []byte("second"), data)
	require.NoError(t, restarted.Stop(ctx))
}

func TestTieredRemoteOwnershipPreventsSecondWriter(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	firstJournal := localjournal.New(t.TempDir(), journal.Config{})
	secondJournal := localjournal.New(t.TempDir(), journal.Config{})
	cfg := testConfig(tiered.DurabilityLocalJournal)
	first, err := tiered.New(remote, firstJournal, cfg)
	require.NoError(t, err)
	second, err := tiered.New(remote, secondJournal, cfg)
	require.NoError(t, err)
	require.NoError(t, first.Start(ctx))
	defer first.Stop(ctx)
	err = second.Start(ctx)
	require.Error(t, err)
}

func newTiered(t *testing.T, remote blobstore.ReplicationStore, dir string, durability tiered.Durability) *tiered.Store {
	t.Helper()
	store, err := tiered.New(remote, localjournal.New(dir, journal.Config{MaxPendingEntries: 100, MaxPendingBytes: 1 << 20}), testConfig(durability))
	require.NoError(t, err)
	return store
}

func testConfig(durability tiered.Durability) tiered.Config {
	return tiered.Config{
		Durability:   durability,
		PollInterval: time.Millisecond,
		RetryBase:    5 * time.Millisecond,
		RetryMax:     20 * time.Millisecond,
		MaxAttempts:  20,
	}
}

type memoryObject struct {
	data        []byte
	version     string
	updated     time.Time
	operationID string
	checksum    string
}

type blockingMarkJournal struct {
	journal.Store
	entered chan struct{}
	release chan struct{}
}

func (b *blockingMarkJournal) MarkReplicated(ctx context.Context, sequence uint64, remoteVersion string) error {
	close(b.entered)
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.Store.MarkReplicated(ctx, sequence, remoteVersion)
}

type blockingPutJournal struct {
	journal.Store
	entered chan struct{}
	release chan struct{}
}

func (b *blockingPutJournal) PutBytes(ctx context.Context, key string, data []byte, expectedVersion string) (journal.Object, journal.Entry, error) {
	close(b.entered)
	select {
	case <-b.release:
	case <-ctx.Done():
		return journal.Object{}, journal.Entry{}, ctx.Err()
	}
	return b.Store.PutBytes(ctx, key, data, expectedVersion)
}

type memoryRemote struct {
	mu                sync.Mutex
	objects           map[string]memoryObject
	versions          int
	unavailable       bool
	succeedThenError  bool
	ops               []string
	ownerID           string
	ownerVersion      string
	releaseOwnerErr   error
	releaseOwnerCalls int
	replicaAttempts   int
}

func newMemoryRemote() *memoryRemote              { return &memoryRemote{objects: map[string]memoryObject{}} }
func (m *memoryRemote) setUnavailable(value bool) { m.mu.Lock(); m.unavailable = value; m.mu.Unlock() }
func (m *memoryRemote) operations() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.ops...)
}
func (m *memoryRemote) check() error {
	if m.unavailable {
		return errors.New("remote unavailable")
	}
	return nil
}
func (m *memoryRemote) Head(_ context.Context, key string) (*blobstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	object, ok := m.objects[key]
	if !ok {
		return nil, blobstore.ErrNotFound
	}
	return &blobstore.ObjectInfo{Key: key, Version: object.version, UpdatedAt: object.updated, Size: int64(len(object.data))}, nil
}
func (m *memoryRemote) DownloadBytes(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	object, ok := m.objects[key]
	if !ok {
		return nil, blobstore.ErrNotFound
	}
	return append([]byte(nil), object.data...), nil
}
func (m *memoryRemote) Download(ctx context.Context, key, dest string) error {
	data, err := m.DownloadBytes(ctx, key)
	if err != nil {
		return err
	}
	return os.WriteFile(dest, data, 0o600)
}
func (m *memoryRemote) UploadBytesIfMatch(ctx context.Context, key string, data []byte, expected string) (*blobstore.ObjectInfo, error) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("memory-remote-%d", time.Now().UnixNano()))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	defer os.Remove(path)
	return m.UploadIfMatch(ctx, key, path, expected)
}
func (m *memoryRemote) UploadIfMatch(_ context.Context, key, src, expected string) (*blobstore.ObjectInfo, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	current, found := m.objects[key]
	if expected != "" && (!found || current.version != expected) {
		return nil, blobstore.ErrVersionMismatch
	}
	return m.putLocked(key, data)
}
func (m *memoryRemote) UploadIfNotExists(_ context.Context, key, src string) (*blobstore.ObjectInfo, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	if _, found := m.objects[key]; found {
		return nil, blobstore.ErrVersionMismatch
	}
	return m.putLocked(key, data)
}
func (m *memoryRemote) PutReplica(_ context.Context, request blobstore.ReplicaPut) (*blobstore.ReplicaInfo, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicaAttempts++
	if err := m.check(); err != nil {
		return nil, err
	}
	current, found := m.objects[request.Key]
	if request.CreateOnly && found {
		return nil, blobstore.ErrVersionMismatch
	}
	if !request.CreateOnly && (!found || current.version != request.ExpectedVersion) {
		return nil, blobstore.ErrVersionMismatch
	}
	info, putErr := m.putLocked(request.Key, data)
	current = m.objects[request.Key]
	current.operationID = request.OperationID
	current.checksum = request.Checksum
	m.objects[request.Key] = current
	if errors.Is(putErr, errLostResponse) {
		return nil, putErr
	}
	if putErr != nil {
		return nil, putErr
	}
	return &blobstore.ReplicaInfo{ObjectInfo: *info, OperationID: request.OperationID, Checksum: request.Checksum}, nil
}

func (m *memoryRemote) HeadReplica(ctx context.Context, key string) (*blobstore.ReplicaInfo, error) {
	info, err := m.Head(ctx, key)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	object := m.objects[key]
	m.mu.Unlock()
	return &blobstore.ReplicaInfo{ObjectInfo: *info, OperationID: object.operationID, Checksum: object.checksum}, nil
}

func (m *memoryRemote) ClaimReplicationOwner(_ context.Context, _ string, ownerID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return "", err
	}
	if m.ownerID == "" {
		m.versions++
		m.ownerID = ownerID
		m.ownerVersion = fmt.Sprintf("owner-v%d", m.versions)
		return m.ownerVersion, nil
	}
	if m.ownerID != ownerID {
		return "", blobstore.ErrVersionMismatch
	}
	return m.ownerVersion, nil
}

func (m *memoryRemote) ReleaseReplicationOwner(_ context.Context, _ string, ownerID, expectedVersion string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseOwnerCalls++
	if m.releaseOwnerErr != nil {
		return m.releaseOwnerErr
	}
	if m.ownerID != ownerID || m.ownerVersion != expectedVersion {
		return nil
	}
	m.ownerID = ""
	m.ownerVersion = ""
	return nil
}

func (m *memoryRemote) DeleteReplica(_ context.Context, key, expectedVersion string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return err
	}
	current, found := m.objects[key]
	if !found {
		return blobstore.ErrNotFound
	}
	if expectedVersion == "" || current.version != expectedVersion {
		return blobstore.ErrVersionMismatch
	}
	delete(m.objects, key)
	m.ops = append(m.ops, "delete:"+key)
	return nil
}

var errLostResponse = errors.New("response lost")

func (m *memoryRemote) putLocked(key string, data []byte) (*blobstore.ObjectInfo, error) {
	m.versions++
	object := memoryObject{data: append([]byte(nil), data...), version: fmt.Sprintf("v%d", m.versions), updated: time.Now().UTC()}
	m.objects[key] = object
	m.ops = append(m.ops, "put:"+key)
	info := &blobstore.ObjectInfo{Key: key, Version: object.version, UpdatedAt: object.updated, Size: int64(len(data))}
	if m.succeedThenError {
		m.succeedThenError = false
		return nil, errLostResponse
	}
	return info, nil
}
func (m *memoryRemote) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return err
	}
	delete(m.objects, key)
	m.ops = append(m.ops, "delete:"+key)
	return nil
}
func (m *memoryRemote) List(_ context.Context, prefix string) ([]blobstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	var result []blobstore.ObjectInfo
	for key, object := range m.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, blobstore.ObjectInfo{Key: key, Version: object.version, UpdatedAt: object.updated, Size: int64(len(object.data))})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
