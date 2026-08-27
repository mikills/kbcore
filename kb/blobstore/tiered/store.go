// Package tiered provides a local-journal/S3-cold blob store. Mutations are
// committed to the journal first and replicated in strict global order.
package tiered

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/mikills/minnow/kb/blobstore/journal"
)

type Durability string

const (
	DurabilityRemote       Durability = "remote"
	DurabilityLocalJournal Durability = "local_journal"
)

type Config struct {
	Durability    Durability
	PollInterval  time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
	MaxAttempts   int
	OwnerKey      string
	ControlPrefix string
}

type Store struct {
	remote  blobstore.ReplicationStore
	journal journal.Store
	config  Config

	lifecycleMu        sync.Mutex
	visibilityMu       sync.RWMutex
	visibilityErr      error
	mu                 sync.Mutex
	started            bool
	accepting          bool
	closed             bool
	cancel             context.CancelFunc
	ownerID            string
	ownerVersion       string
	replicationErr     error
	lastReplicationErr error
	done               chan struct{}
	enqueueDrained     chan struct{}
	enqueues           int
	wg                 sync.WaitGroup
}

func New(remote blobstore.ReplicationStore, localJournal journal.Store, cfg Config) (*Store, error) {
	if remote == nil {
		return nil, errors.New("tiered blob store requires a remote store")
	}
	if localJournal == nil {
		return nil, errors.New("tiered blob store requires a replication journal")
	}
	if cfg.Durability == "" {
		cfg.Durability = DurabilityRemote
	}
	if cfg.Durability != DurabilityRemote && cfg.Durability != DurabilityLocalJournal {
		return nil, fmt.Errorf("unsupported tiered durability %q", cfg.Durability)
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 250 * time.Millisecond
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 30 * time.Second
	}
	if cfg.RetryMax < cfg.RetryBase {
		cfg.RetryMax = cfg.RetryBase
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 20
	}
	cfg.ControlPrefix = strings.TrimSpace(cfg.ControlPrefix)
	if cfg.ControlPrefix == "" {
		cfg.ControlPrefix = "leases/"
	} else if !strings.HasSuffix(cfg.ControlPrefix, "/") {
		cfg.ControlPrefix += "/"
	}
	if cfg.OwnerKey == "" {
		cfg.OwnerKey = cfg.ControlPrefix + "journal/owner.lock"
	}
	return &Store{remote: remote, journal: localJournal, config: cfg}, nil
}

func (s *Store) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	if s.closed {
		s.mu.Unlock()
		return errors.New("tiered blob store is closed")
	}
	s.mu.Unlock()

	if err := s.journal.Open(ctx); err != nil {
		return fmt.Errorf("open replication journal: %w", err)
	}
	ownerID, err := s.journal.Identity(ctx)
	if err != nil {
		_ = s.journal.Close()
		return fmt.Errorf("read replication journal identity: %w", err)
	}
	ownerVersion, err := s.remote.ClaimReplicationOwner(ctx, s.config.OwnerKey, ownerID)
	if err != nil {
		_ = s.journal.Close()
		return fmt.Errorf("claim tiered-store remote prefix: %w", err)
	}
	if err := s.seedRemoteCatalog(ctx); err != nil {
		// Keep the non-expiring claim. The same persistent journal can retry;
		// an independent journal must not take over a prefix with pending work.
		_ = s.journal.Close()
		return fmt.Errorf("seed tiered remote catalog: %w", err)
	}
	if err := s.journal.RetryFailed(ctx, 0); err != nil {
		_ = s.journal.Close()
		return fmt.Errorf("requeue failed replication entries: %w", err)
	}
	stats, err := s.journal.Stats(ctx)
	if err != nil {
		_ = s.journal.Close()
		return fmt.Errorf("inspect replication journal: %w", err)
	}
	if s.config.Durability == DurabilityRemote {
		s.visibilityMu.Lock()
		if stats.PendingEntries > 0 {
			s.visibilityErr = journal.ErrNoneAvailable
		} else {
			s.visibilityErr = nil
		}
		s.visibilityMu.Unlock()
	}

	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		cancel()
		_ = s.journal.Close()
		return errors.New("tiered blob store lifecycle changed during start")
	}
	s.started = true
	s.accepting = true
	s.cancel = cancel
	s.ownerID = ownerID
	s.ownerVersion = ownerVersion
	s.mu.Unlock()

	s.wg.Add(1)
	go s.replicationLoop(workerCtx)
	done := make(chan struct{})
	s.mu.Lock()
	s.done = done
	s.mu.Unlock()
	go func() {
		s.wg.Wait()
		close(done)
	}()
	return nil
}

func (s *Store) BeginStop() {
	s.mu.Lock()
	s.accepting = false
	s.mu.Unlock()
}

func (s *Store) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.BeginStop()
	s.mu.Lock()
	enqueueDrained := s.enqueueDrained
	cancel := s.cancel
	ownerID := s.ownerID
	ownerVersion := s.ownerVersion
	done := s.done
	s.mu.Unlock()
	if enqueueDrained != nil {
		select {
		case <-enqueueDrained:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			// Leave the journal and remote ownership claim open so shutdown can be retried.
			return ctx.Err()
		}
	}
	if ownerID != "" {
		stats, err := s.journal.Stats(ctx)
		if err != nil {
			return err
		}
		if stats.PendingEntries == 0 {
			if err := s.remote.ReleaseReplicationOwner(ctx, s.config.OwnerKey, ownerID, ownerVersion); err != nil {
				return err
			}
		}
	}
	if err := s.journal.Close(); err != nil {
		return err
	}
	s.mu.Lock()
	s.started = false
	s.closed = true
	s.cancel = nil
	s.ownerID = ""
	s.ownerVersion = ""
	s.done = nil
	s.mu.Unlock()
	return nil
}

func (s *Store) Head(ctx context.Context, key string) (*blobstore.ObjectInfo, error) {
	release, err := s.beginRemoteRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if s.reservedKey(key) {
		return nil, blobstore.ErrNotFound
	}
	object, err := s.journal.Get(ctx, key)
	if err == nil {
		if s.config.Durability == DurabilityRemote && !object.Replicated {
			return s.remote.Head(ctx, key)
		}
		if object.Tombstone {
			return nil, blobstore.ErrNotFound
		}
		return objectInfo(object), nil
	}
	if !errors.Is(err, journal.ErrNotFound) {
		return nil, mapJournalError(err)
	}
	remote, err := s.remote.Head(ctx, key)
	if err != nil {
		return nil, err
	}
	seeded, err := s.journal.Seed(ctx, journal.Object{
		Key:           remote.Key,
		Version:       remote.Version,
		RemoteVersion: remote.Version,
		UpdatedAt:     remote.UpdatedAt,
		Size:          remote.Size,
		Replicated:    true,
	})
	if err != nil {
		return nil, mapJournalError(err)
	}
	if s.config.Durability == DurabilityRemote {
		return remote, nil
	}
	if seeded.Tombstone {
		return nil, blobstore.ErrNotFound
	}
	return objectInfo(seeded), nil
}

func (s *Store) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	release, err := s.beginRemoteRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if s.reservedKey(key) {
		return nil, blobstore.ErrNotFound
	}
	object, err := s.journal.Get(ctx, key)
	if err == nil {
		if s.config.Durability == DurabilityRemote && !object.Replicated {
			return s.remote.DownloadBytes(ctx, key)
		}
		if object.Tombstone {
			return nil, blobstore.ErrNotFound
		}
		if object.PayloadID != "" {
			data, readErr := s.readLocalPayload(ctx, object)
			if readErr == nil {
				return data, nil
			}
			return s.fallbackAfterPayloadRace(ctx, key, object, readErr)
		}
	} else if !errors.Is(err, journal.ErrNotFound) {
		return nil, mapJournalError(err)
	}
	return s.remote.DownloadBytes(ctx, key)
}

func (s *Store) Download(ctx context.Context, key, dest string) error {
	release, err := s.beginRemoteRead(ctx)
	if err != nil {
		return err
	}
	defer release()
	if s.reservedKey(key) {
		return blobstore.ErrNotFound
	}
	object, err := s.journal.Get(ctx, key)
	if err == nil {
		if s.config.Durability == DurabilityRemote && !object.Replicated {
			return s.remote.Download(ctx, key, dest)
		}
		if object.Tombstone {
			return blobstore.ErrNotFound
		}
		if object.PayloadID != "" {
			if copyErr := s.copyPayload(ctx, object, dest); copyErr == nil {
				return nil
			} else {
				current, getErr := s.journal.Get(ctx, key)
				if getErr == nil && current.Version == object.Version && current.Replicated {
					return s.remote.Download(ctx, key, dest)
				}
				return copyErr
			}
		}
	} else if !errors.Is(err, journal.ErrNotFound) {
		return mapJournalError(err)
	}
	return s.remote.Download(ctx, key, dest)
}

func (s *Store) UploadBytesIfMatch(ctx context.Context, key string, data []byte, expectedVersion string) (*blobstore.ObjectInfo, error) {
	finish, err := s.beginEnqueue(ctx, key)
	if err != nil {
		return nil, err
	}
	defer finish()
	object, entry, err := s.journal.PutBytes(ctx, key, data, expectedVersion)
	if err == nil {
		err = validateMutationResult(key, expectedVersion, false, object, entry)
	}
	finish()
	if err != nil {
		return nil, mapJournalError(err)
	}
	if err := s.waitIfRemote(ctx, entry.Sequence); err != nil {
		return nil, err
	}
	return objectInfo(object), nil
}

func (s *Store) UploadIfMatch(ctx context.Context, key, src, expectedVersion string) (*blobstore.ObjectInfo, error) {
	finish, err := s.beginEnqueue(ctx, key)
	if err != nil {
		return nil, err
	}
	defer finish()
	object, entry, err := s.journal.PutFile(ctx, key, src, expectedVersion)
	if err == nil {
		err = validateMutationResult(key, expectedVersion, false, object, entry)
	}
	finish()
	if err != nil {
		return nil, mapJournalError(err)
	}
	if err := s.waitIfRemote(ctx, entry.Sequence); err != nil {
		return nil, err
	}
	return objectInfo(object), nil
}

func (s *Store) UploadIfNotExists(ctx context.Context, key, src string) (*blobstore.ObjectInfo, error) {
	finish, err := s.beginEnqueue(ctx, key)
	if err != nil {
		return nil, err
	}
	defer finish()
	object, entry, err := s.journal.CreateFile(ctx, key, src)
	if err == nil {
		err = validateMutationResult(key, "", true, object, entry)
	}
	finish()
	if err != nil {
		return nil, mapJournalError(err)
	}
	if err := s.waitIfRemote(ctx, entry.Sequence); err != nil {
		return nil, err
	}
	return objectInfo(object), nil
}

func (s *Store) UploadBytesIfNotExists(ctx context.Context, key string, data []byte) (*blobstore.ObjectInfo, error) {
	finish, err := s.beginEnqueue(ctx, key)
	if err != nil {
		return nil, err
	}
	defer finish()
	object, entry, err := s.journal.CreateBytes(ctx, key, data)
	if err == nil {
		err = validateMutationResult(key, "", true, object, entry)
	}
	finish()
	if err != nil {
		return nil, mapJournalError(err)
	}
	if err := s.waitIfRemote(ctx, entry.Sequence); err != nil {
		return nil, err
	}
	return objectInfo(object), nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	finish, err := s.beginEnqueue(ctx, key)
	if err != nil {
		return err
	}
	defer finish()
	entry, err := s.journal.Delete(ctx, key)
	if err == nil && entry.Sequence != 0 {
		err = validateDeleteResult(key, entry)
	}
	finish()
	if err != nil {
		return mapJournalError(err)
	}
	return s.waitIfRemote(ctx, entry.Sequence)
}

func (s *Store) List(ctx context.Context, prefix string) ([]blobstore.ObjectInfo, error) {
	release, err := s.beginRemoteRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	remoteObjects, err := s.remote.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]blobstore.ObjectInfo, len(remoteObjects))
	seeds := make([]journal.Object, 0, len(remoteObjects))
	for _, remote := range remoteObjects {
		if s.reservedKey(remote.Key) {
			continue
		}
		seeds = append(seeds, journal.Object{
			Key:           remote.Key,
			Version:       remote.Version,
			RemoteVersion: remote.Version,
			UpdatedAt:     remote.UpdatedAt,
			Size:          remote.Size,
			Replicated:    true,
		})
		merged[remote.Key] = remote
	}
	if err := s.journal.SeedBatch(ctx, seeds); err != nil {
		return nil, mapJournalError(err)
	}
	overlay, err := s.journal.Scan(ctx, prefix)
	if err != nil {
		return nil, mapJournalError(err)
	}
	for _, object := range overlay {
		if s.reservedKey(object.Key) {
			continue
		}
		if s.config.Durability == DurabilityRemote && !object.Replicated {
			continue
		}
		if object.Tombstone {
			delete(merged, object.Key)
			continue
		}
		merged[object.Key] = *objectInfo(object)
	}
	result := make([]blobstore.ObjectInfo, 0, len(merged))
	for _, object := range merged {
		result = append(result, object)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *Store) Stats(ctx context.Context) (journal.Stats, error) {
	return s.journal.Stats(ctx)
}

func (s *Store) ReplicationOpenMetrics(ctx context.Context) (string, error) {
	stats, err := s.Stats(ctx)
	if err != nil {
		return "", err
	}
	oldestAge := 0.0
	if !stats.OldestPendingAt.IsZero() {
		oldestAge = time.Since(stats.OldestPendingAt).Seconds()
		if oldestAge < 0 {
			oldestAge = 0
		}
	}
	lines := []string{
		"# TYPE minnow_replication_pending_entries gauge",
		fmt.Sprintf("minnow_replication_pending_entries %d", stats.PendingEntries),
		"# TYPE minnow_replication_pending_bytes gauge",
		fmt.Sprintf("minnow_replication_pending_bytes %d", stats.PendingBytes),
		"# TYPE minnow_replication_failed_entries gauge",
		fmt.Sprintf("minnow_replication_failed_entries %d", stats.FailedEntries),
		"# TYPE minnow_replication_oldest_pending_seconds gauge",
		fmt.Sprintf("minnow_replication_oldest_pending_seconds %.3f", oldestAge),
		"# TYPE minnow_replication_upload_failures_total counter",
		fmt.Sprintf("minnow_replication_upload_failures_total %d", stats.UploadFailures),
		"# TYPE minnow_replication_journal_cleanup_errors_total counter",
		fmt.Sprintf("minnow_replication_journal_cleanup_errors_total %d", stats.CleanupErrors),
		"# TYPE minnow_replication_journal_disk_capacity_bytes gauge",
		fmt.Sprintf("minnow_replication_journal_disk_capacity_bytes %d", stats.DiskCapacityBytes),
		"# TYPE minnow_replication_journal_disk_available_bytes gauge",
		fmt.Sprintf("minnow_replication_journal_disk_available_bytes %d", stats.DiskAvailableBytes),
		"# TYPE minnow_replication_sequence gauge",
		fmt.Sprintf("minnow_replication_sequence %d", stats.ReplicatedThrough),
	}
	return strings.Join(lines, "\n") + "\n", nil
}

func (s *Store) ReplicationError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replicationErr != nil {
		return s.replicationErr
	}
	return s.lastReplicationErr
}

func (s *Store) RetryFailed(ctx context.Context, sequence uint64) error {
	return s.journal.RetryFailed(ctx, sequence)
}

func (s *Store) waitIfRemote(ctx context.Context, sequence uint64) error {
	if s.config.Durability != DurabilityRemote {
		return nil
	}
	return mapJournalError(s.journal.Wait(ctx, sequence))
}

func (s *Store) refreshRemoteVisibility(ctx context.Context) {
	if s.config.Durability != DurabilityRemote {
		return
	}
	stats, err := s.journal.Stats(ctx)
	s.visibilityMu.Lock()
	defer s.visibilityMu.Unlock()
	switch {
	case err != nil:
		s.visibilityErr = err
	case stats.PendingEntries > 0:
		s.visibilityErr = journal.ErrNoneAvailable
	default:
		s.visibilityErr = nil
	}
}

func (s *Store) beginRemoteRead(ctx context.Context) (func(), error) {
	if s.config.Durability != DurabilityRemote {
		return func() {}, ctx.Err()
	}
	s.visibilityMu.RLock()
	if err := ctx.Err(); err != nil {
		s.visibilityMu.RUnlock()
		return nil, err
	}
	if s.visibilityErr != nil {
		err := fmt.Errorf("remote visibility is blocked by unresolved replication: %w", s.visibilityErr)
		s.visibilityMu.RUnlock()
		return nil, err
	}
	return s.visibilityMu.RUnlock, nil
}

func (s *Store) beginEnqueue(ctx context.Context, key string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.reservedKey(key) {
		return nil, fmt.Errorf("tiered blob key %q uses reserved control prefix %q", key, s.config.ControlPrefix)
	}
	s.mu.Lock()
	if !s.started || !s.accepting || s.closed {
		s.mu.Unlock()
		return nil, errors.New("tiered blob store is not accepting writes")
	}
	if s.replicationErr != nil {
		err := fmt.Errorf("tiered blob replication is blocked: %w", s.replicationErr)
		s.mu.Unlock()
		return nil, err
	}
	if s.enqueues == 0 {
		s.enqueueDrained = make(chan struct{})
	}
	s.enqueues++
	s.mu.Unlock()
	finished := false
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if finished {
			return
		}
		finished = true
		s.enqueues--
		if s.enqueues == 0 {
			close(s.enqueueDrained)
			s.enqueueDrained = nil
		}
	}, nil
}

func (s *Store) reservedKey(key string) bool {
	return s.config.ControlPrefix != "" && strings.HasPrefix(key, s.config.ControlPrefix)
}

func (s *Store) replicationLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		entry, err := s.journal.Next(ctx, time.Now().UTC())
		switch {
		case err == nil:
			s.replicateOne(ctx, entry)
			continue
		case errors.Is(err, journal.ErrNoneAvailable):
			s.setReplicationError(nil)
			s.refreshRemoteVisibility(ctx)
		case errors.Is(err, journal.ErrReplicationFailed):
			s.setReplicationError(err)
		default:
			s.setReplicationError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) replicateOne(ctx context.Context, entry journal.Entry) {
	remoteVisibility := s.config.Durability == DurabilityRemote
	if remoteVisibility {
		s.visibilityMu.Lock()
		defer s.visibilityMu.Unlock()
	}
	var remoteVersion string
	err := validateReplicationEntry(entry)
	if err == nil {
		remoteVersion, err = s.applyRemote(ctx, entry)
	}
	if err == nil {
		if markErr := s.journal.MarkReplicated(ctx, entry.Sequence, remoteVersion); markErr == nil {
			if remoteVisibility {
				stats, statsErr := s.journal.Stats(ctx)
				switch {
				case statsErr != nil:
					s.visibilityErr = statsErr
				case stats.PendingEntries > 0:
					s.visibilityErr = journal.ErrNoneAvailable
				default:
					s.visibilityErr = nil
				}
			}
			s.mu.Lock()
			s.replicationErr = nil
			s.lastReplicationErr = nil
			s.mu.Unlock()
			return
		} else {
			err = fmt.Errorf("mark replicated: %w", markErr)
		}
	}
	if remoteVisibility {
		s.visibilityErr = err
	}
	if ctx.Err() != nil {
		return
	}
	failed := errors.Is(err, journal.ErrCorrupt) || entry.Attempts+1 >= s.config.MaxAttempts
	next := time.Now().UTC().Add(s.retryDelay(entry.Attempts + 1))
	s.mu.Lock()
	s.lastReplicationErr = err
	s.mu.Unlock()
	if markErr := s.journal.MarkRetry(ctx, entry.Sequence, err.Error(), next, failed); markErr != nil {
		s.setReplicationError(errors.Join(err, markErr))
		return
	}
	if failed {
		s.setReplicationError(fmt.Errorf("%w: sequence=%d: %v", journal.ErrReplicationFailed, entry.Sequence, err))
	}
}

func validateMutationResult(key, requestedExpected string, requestedCreateOnly bool, object journal.Object, entry journal.Entry) error {
	if object.Key != key || entry.Key != key || object.Version == "" || object.Version != entry.Version || object.Tombstone || object.Size != entry.Size || object.Checksum != entry.Checksum || object.PayloadID != entry.PayloadID {
		return journal.ErrCorrupt
	}
	if entry.Operation != journal.OperationPut || entry.Sequence == 0 || entry.Size < 0 || entry.PayloadID == "" || entry.PayloadID != entry.Checksum || entry.ExpectedVersion != requestedExpected {
		return journal.ErrCorrupt
	}
	checksum, err := hex.DecodeString(entry.Checksum)
	if err != nil || len(checksum) != sha256.Size {
		return journal.ErrCorrupt
	}
	if requestedCreateOnly && !entry.CreateOnly {
		return journal.ErrCorrupt
	}
	if requestedExpected != "" && (entry.CreateOnly || entry.PreviousVersion != requestedExpected) {
		return journal.ErrCorrupt
	}
	if entry.PreviousVersion != "" && entry.Version == entry.PreviousVersion {
		return journal.ErrCorrupt
	}
	if entry.CreateOnly {
		if entry.ExpectedRemoteVersion != "" {
			return journal.ErrCorrupt
		}
	} else if entry.PreviousVersion == "" {
		return journal.ErrCorrupt
	}
	return nil
}

func validateDeleteResult(key string, entry journal.Entry) error {
	if entry.Sequence == 0 || entry.Operation != journal.OperationDelete || entry.Key != key || entry.Version == "" || entry.PreviousVersion == "" || entry.PayloadID != "" || entry.Size != 0 {
		return journal.ErrCorrupt
	}
	return nil
}

func validReplicaResult(info *blobstore.ReplicaInfo, entry journal.Entry) bool {
	return info != nil && info.Key == entry.Key && info.Version != "" && info.Size == entry.Size && info.OperationID == entry.Version && info.Checksum == entry.Checksum
}

func validateReplicationEntry(entry journal.Entry) error {
	if entry.Sequence == 0 || strings.TrimSpace(entry.Key) == "" || strings.TrimSpace(entry.Version) == "" {
		return journal.ErrCorrupt
	}
	switch entry.Operation {
	case journal.OperationPut:
		if entry.Size < 0 || entry.PayloadID == "" || entry.Checksum == "" || entry.PayloadID != entry.Checksum {
			return journal.ErrCorrupt
		}
		checksum, err := hex.DecodeString(entry.Checksum)
		if err != nil || len(checksum) != sha256.Size {
			return journal.ErrCorrupt
		}
		if entry.CreateOnly {
			if entry.ExpectedRemoteVersion != "" {
				return journal.ErrCorrupt
			}
		} else if entry.PreviousVersion == "" || entry.ExpectedRemoteVersion == "" {
			return journal.ErrCorrupt
		}
	case journal.OperationDelete:
		if entry.PayloadID != "" || entry.Size != 0 || entry.PreviousVersion == "" || entry.ExpectedRemoteVersion == "" {
			return journal.ErrCorrupt
		}
	default:
		return journal.ErrCorrupt
	}
	return nil
}

func (s *Store) applyRemote(ctx context.Context, entry journal.Entry) (string, error) {
	switch entry.Operation {
	case journal.OperationPut:
		reader, err := s.journal.OpenPayload(ctx, entry)
		if err != nil {
			return "", err
		}
		defer reader.Close()
		hash := sha256.New()
		written, checksumErr := copyContext(ctx, hash, reader)
		if checksumErr != nil {
			return "", checksumErr
		}
		if written != entry.Size || hex.EncodeToString(hash.Sum(nil)) != entry.Checksum {
			return "", fmt.Errorf("journal payload integrity mismatch for sequence %d", entry.Sequence)
		}
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return "", fmt.Errorf("rewind journal payload: %w", err)
		}
		if !entry.CreateOnly && entry.ExpectedRemoteVersion == "" {
			return "", fmt.Errorf("replica put sequence %d is missing a fencing precondition", entry.Sequence)
		}
		info, uploadErr := s.remote.PutReplica(ctx, blobstore.ReplicaPut{
			Key:             entry.Key,
			Body:            reader,
			Size:            entry.Size,
			ExpectedVersion: entry.ExpectedRemoteVersion,
			CreateOnly:      entry.CreateOnly,
			OperationID:     entry.Version,
			Checksum:        entry.Checksum,
		})
		if uploadErr == nil {
			if !validReplicaResult(info, entry) {
				return "", journal.ErrCorrupt
			}
			return info.Version, nil
		}
		current, headErr := s.remote.HeadReplica(ctx, entry.Key)
		if headErr == nil && validReplicaResult(current, entry) {
			return current.Version, nil
		}
		if headErr == nil && current == nil {
			return "", journal.ErrCorrupt
		}
		return "", uploadErr
	case journal.OperationDelete:
		if entry.ExpectedRemoteVersion == "" {
			// A delete of a key known absent is already remotely complete.
			if entry.PreviousVersion == "" {
				return "", nil
			}
			return "", fmt.Errorf("replica delete sequence %d is missing a fencing precondition", entry.Sequence)
		}
		err := s.remote.DeleteReplica(ctx, entry.Key, entry.ExpectedRemoteVersion)
		if err == nil || errors.Is(err, blobstore.ErrNotFound) {
			return "", nil
		}
		if _, headErr := s.remote.HeadReplica(ctx, entry.Key); errors.Is(headErr, blobstore.ErrNotFound) {
			return "", nil
		}
		return "", err
	default:
		return "", fmt.Errorf("unsupported replication operation %q", entry.Operation)
	}
}

func (s *Store) retryDelay(attempt int) time.Duration {
	delay := s.config.RetryBase
	for i := 1; i < attempt && delay < s.config.RetryMax; i++ {
		if delay > s.config.RetryMax/2 {
			return s.config.RetryMax
		}
		delay *= 2
	}
	if delay > s.config.RetryMax {
		return s.config.RetryMax
	}
	return delay
}

func (s *Store) seedRemoteCatalog(ctx context.Context) error {
	remoteObjects, err := s.remote.List(ctx, "")
	if err != nil {
		return err
	}
	objects := make([]journal.Object, 0, len(remoteObjects))
	for _, object := range remoteObjects {
		if strings.HasPrefix(object.Key, s.config.ControlPrefix) {
			continue
		}
		replica, err := s.remote.HeadReplica(ctx, object.Key)
		if err != nil {
			return err
		}
		if replica == nil || replica.Key != object.Key || replica.Version == "" || replica.Size < 0 || (replica.OperationID != "" && replica.Checksum == "") {
			return journal.ErrCorrupt
		}
		logicalVersion := replica.OperationID
		if logicalVersion == "" {
			logicalVersion = replica.Version
		}
		objects = append(objects, journal.Object{
			Key:           replica.Key,
			Version:       logicalVersion,
			RemoteVersion: replica.Version,
			UpdatedAt:     replica.UpdatedAt,
			Size:          replica.Size,
			Checksum:      replica.Checksum,
			Replicated:    true,
		})
	}
	return s.journal.ReconcileRemote(ctx, objects)
}

func (s *Store) readLocalPayload(ctx context.Context, object journal.Object) ([]byte, error) {
	reader, err := s.journal.OpenPayloadByID(ctx, object.PayloadID)
	if err != nil {
		return nil, mapJournalError(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	checksum := sha256.Sum256(data)
	if int64(len(data)) != object.Size || hex.EncodeToString(checksum[:]) != object.Checksum {
		return nil, fmt.Errorf("journal payload integrity mismatch for %s", object.Key)
	}
	return data, nil
}

func (s *Store) fallbackAfterPayloadRace(ctx context.Context, key string, observed journal.Object, readErr error) ([]byte, error) {
	current, err := s.journal.Get(ctx, key)
	if err == nil && current.Version == observed.Version && current.Replicated {
		return s.remote.DownloadBytes(ctx, key)
	}
	return nil, readErr
}

func (s *Store) copyPayload(ctx context.Context, object journal.Object, destination string) error {
	reader, err := s.journal.OpenPayloadByID(ctx, object.PayloadID)
	if err != nil {
		return mapJournalError(err)
	}
	defer reader.Close()
	file, err := os.Create(destination)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := copyContext(ctx, io.MultiWriter(file, hash), reader)
	if syncErr := file.Sync(); copyErr == nil {
		copyErr = syncErr
	}
	if closeErr := file.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr == nil && (written != object.Size || hex.EncodeToString(hash.Sum(nil)) != object.Checksum) {
		copyErr = fmt.Errorf("journal payload integrity mismatch for %s", object.Key)
	}
	if copyErr != nil {
		_ = os.Remove(destination)
	}
	return copyErr
}

func (s *Store) setReplicationError(err error) {
	s.mu.Lock()
	s.replicationErr = err
	s.mu.Unlock()
}

func objectInfo(object journal.Object) *blobstore.ObjectInfo {
	return &blobstore.ObjectInfo{Key: object.Key, Version: object.Version, UpdatedAt: object.UpdatedAt, Size: object.Size}
}

func mapJournalError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, journal.ErrNotFound):
		return errors.Join(blobstore.ErrNotFound, err)
	case errors.Is(err, journal.ErrVersionMismatch):
		return errors.Join(blobstore.ErrVersionMismatch, err)
	default:
		return err
	}
}

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 128*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}
