// Package localjournal provides a crash-recoverable bbolt-backed replication
// journal with journal-owned, content-addressed payload files.
package localjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/cacheevict"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketMeta      = []byte("meta")
	bucketObjects   = []byte("objects")
	bucketEntries   = []byte("entries")
	keyInstanceID   = []byte("instance_id")
	keyReplicated   = []byte("replicated_through")
	keyUploadFailed = []byte("upload_failures")
)

type Store struct {
	Dir    string
	Config journal.Config

	mu            sync.Mutex
	mutationMu    sync.Mutex
	db            *bolt.DB
	notify        chan struct{}
	closed        bool
	cleanupErrors atomic.Uint64
	gcPending     map[string]struct{}
}

func New(dir string, cfg journal.Config) *Store {
	return &Store{Dir: dir, Config: cfg, notify: make(chan struct{}), gcPending: make(map[string]struct{})}
}

func (s *Store) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return nil
	}
	if strings.TrimSpace(s.Dir) == "" {
		return errors.New("local journal directory is required")
	}
	if err := mkdirAllDurable(s.payloadDir(), 0o700); err != nil {
		return fmt.Errorf("create journal directory: %w", err)
	}
	db, err := bolt.Open(filepath.Join(s.Dir, "journal.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open journal database: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketObjects); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketEntries); err != nil {
			return err
		}
		if meta.Get(keyInstanceID) == nil {
			return meta.Put(keyInstanceID, []byte(uuid.NewString()))
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return fmt.Errorf("initialize journal database: %w", err)
	}
	s.db = db
	s.closed = false
	if err := syncDir(s.Dir); err != nil {
		s.db = nil
		_ = db.Close()
		return fmt.Errorf("sync journal directory: %w", err)
	}
	if err := s.verifyAndCleanPayloads(ctx); err != nil {
		s.db = nil
		_ = db.Close()
		return err
	}
	return nil
}

func (s *Store) Identity(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	db, err := s.database()
	if err != nil {
		return "", err
	}
	var identity string
	err = db.View(func(tx *bolt.Tx) error {
		identity = string(tx.Bucket(bucketMeta).Get(keyInstanceID))
		if identity == "" {
			return errors.New("journal identity is missing")
		}
		return nil
	})
	return identity, err
}

func (s *Store) PutFile(ctx context.Context, key, src, expectedVersion string) (journal.Object, journal.Entry, error) {
	return s.putFile(ctx, key, src, expectedVersion, false)
}

func (s *Store) PutBytes(ctx context.Context, key string, data []byte, expectedVersion string) (journal.Object, journal.Entry, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.putLocked(ctx, key, bytes.NewReader(data), int64(len(data)), expectedVersion, false)
}

func (s *Store) CreateFile(ctx context.Context, key, src string) (journal.Object, journal.Entry, error) {
	return s.putFile(ctx, key, src, "", true)
}

func (s *Store) CreateBytes(ctx context.Context, key string, data []byte) (journal.Object, journal.Entry, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.putLocked(ctx, key, bytes.NewReader(data), int64(len(data)), "", true)
}

func (s *Store) putFile(ctx context.Context, key, src, expectedVersion string, createOnly bool) (journal.Object, journal.Entry, error) {
	if err := ctx.Err(); err != nil {
		return journal.Object{}, journal.Entry{}, err
	}
	file, err := os.Open(src)
	if err != nil {
		return journal.Object{}, journal.Entry{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return journal.Object{}, journal.Entry{}, err
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.putLocked(ctx, key, file, info.Size(), expectedVersion, createOnly)
}

func (s *Store) putLocked(ctx context.Context, key string, source io.Reader, expectedSize int64, expectedVersion string, createOnly bool) (journal.Object, journal.Entry, error) {
	if err := validateKey(key); err != nil {
		return journal.Object{}, journal.Entry{}, err
	}
	if expectedSize < 0 || expectedSize == math.MaxInt64 {
		return journal.Object{}, journal.Entry{}, journal.ErrBackpressure
	}
	if err := s.preflightPut(key, expectedVersion, createOnly, expectedSize); err != nil {
		return journal.Object{}, journal.Entry{}, err
	}
	payloadID, size, err := s.stagePayload(ctx, io.LimitReader(source, expectedSize+1))
	if err == nil && size > expectedSize {
		err = errors.New("source grew while being journaled")
	}
	if err != nil {
		return journal.Object{}, journal.Entry{}, err
	}

	var object journal.Object
	var entry journal.Entry
	db, err := s.database()
	if err != nil {
		return object, entry, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket(bucketObjects)
		entries := tx.Bucket(bucketEntries)
		current, found, err := getObject(objects, key)
		if err != nil {
			return err
		}
		if createOnly && found && !current.Tombstone {
			return journal.ErrVersionMismatch
		}
		if expectedVersion != "" && (!found || current.Tombstone || current.Version != expectedVersion) {
			return journal.ErrVersionMismatch
		}
		if err := enforceCapacity(entries, s.Config, size); err != nil {
			return err
		}
		sequence, err := entries.NextSequence()
		if err != nil {
			return err
		}
		instanceID := string(tx.Bucket(bucketMeta).Get(keyInstanceID))
		now := time.Now().UTC()
		version := fmt.Sprintf("%s:%020d", instanceID, sequence)
		entry = journal.Entry{
			Sequence:        sequence,
			Operation:       journal.OperationPut,
			Key:             key,
			Version:         version,
			ExpectedVersion: expectedVersion,
			CreateOnly:      createOnly || !found || current.Tombstone,
			PayloadID:       payloadID,
			Checksum:        payloadID,
			Size:            size,
			CreatedAt:       now,
			State:           journal.StatePending,
		}
		if found {
			entry.PreviousVersion = current.Version
			entry.ExpectedRemoteVersion = current.RemoteVersion
		}
		object = journal.Object{
			Key:       key,
			Version:   version,
			UpdatedAt: now,
			Size:      size,
			Checksum:  payloadID,
			PayloadID: payloadID,
		}
		if err := putJSON(entries, sequenceKey(sequence), entry); err != nil {
			return err
		}
		return putJSON(objects, []byte(key), object)
	})
	if err != nil {
		_ = s.removePayloadIfUnreferenced(payloadID)
		return journal.Object{}, journal.Entry{}, err
	}
	s.signal()
	return object, entry, nil
}

func (s *Store) Delete(ctx context.Context, key string) (journal.Entry, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return journal.Entry{}, err
	}
	if err := validateKey(key); err != nil {
		return journal.Entry{}, err
	}
	db, err := s.database()
	if err != nil {
		return journal.Entry{}, err
	}
	var current journal.Object
	var found bool
	if err := db.View(func(tx *bolt.Tx) error {
		var getErr error
		current, found, getErr = getObject(tx.Bucket(bucketObjects), key)
		return getErr
	}); err != nil {
		return journal.Entry{}, err
	}
	if !found || current.Tombstone {
		return journal.Entry{}, nil
	}
	var entry journal.Entry
	err = db.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket(bucketObjects)
		entries := tx.Bucket(bucketEntries)
		current, found, err := getObject(objects, key)
		if err != nil {
			return err
		}
		if err := enforceCapacity(entries, s.Config, 0); err != nil {
			return err
		}
		sequence, err := entries.NextSequence()
		if err != nil {
			return err
		}
		instanceID := string(tx.Bucket(bucketMeta).Get(keyInstanceID))
		now := time.Now().UTC()
		version := fmt.Sprintf("%s:%020d", instanceID, sequence)
		entry = journal.Entry{
			Sequence:  sequence,
			Operation: journal.OperationDelete,
			Key:       key,
			Version:   version,
			CreatedAt: now,
			State:     journal.StatePending,
		}
		if found {
			entry.ExpectedVersion = current.Version
			entry.PreviousVersion = current.Version
			entry.ExpectedRemoteVersion = current.RemoteVersion
		}
		object := journal.Object{Key: key, Version: version, UpdatedAt: now, Tombstone: true}
		if err := putJSON(entries, sequenceKey(sequence), entry); err != nil {
			return err
		}
		return putJSON(objects, []byte(key), object)
	})
	if err == nil {
		s.signal()
	}
	return entry, err
}

func (s *Store) Get(ctx context.Context, key string) (journal.Object, error) {
	if err := ctx.Err(); err != nil {
		return journal.Object{}, err
	}
	db, err := s.database()
	if err != nil {
		return journal.Object{}, err
	}
	var object journal.Object
	var found bool
	err = db.View(func(tx *bolt.Tx) error {
		var getErr error
		object, found, getErr = getObject(tx.Bucket(bucketObjects), key)
		return getErr
	})
	if err != nil {
		return journal.Object{}, err
	}
	if !found {
		return journal.Object{}, journal.ErrNotFound
	}
	return object, nil
}

func (s *Store) Seed(ctx context.Context, object journal.Object) (journal.Object, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return journal.Object{}, err
	}
	if err := validateKey(object.Key); err != nil {
		return journal.Object{}, err
	}
	if object.Version == "" || object.RemoteVersion == "" || object.Size < 0 {
		return journal.Object{}, journal.ErrCorrupt
	}
	object.Replicated = true
	object.PayloadID = ""
	if object.UpdatedAt.IsZero() {
		object.UpdatedAt = time.Now().UTC()
	}
	db, err := s.database()
	if err != nil {
		return journal.Object{}, err
	}
	result := object
	err = db.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket(bucketObjects)
		current, found, err := getObject(objects, object.Key)
		if err != nil {
			return err
		}
		if found {
			result = current
			return nil
		}
		return putJSON(objects, []byte(object.Key), object)
	})
	return result, err
}

// SeedBatch inventories remote objects in one bbolt transaction so startup
// does not fsync once per object. Existing local catalog entries always win.
func (s *Store) SeedBatch(ctx context.Context, input []journal.Object) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	objects := make([]journal.Object, len(input))
	for index, object := range input {
		if err := validateKey(object.Key); err != nil {
			return err
		}
		if object.Version == "" || object.RemoteVersion == "" || object.Size < 0 {
			return journal.ErrCorrupt
		}
		object.Replicated = true
		object.PayloadID = ""
		if object.UpdatedAt.IsZero() {
			object.UpdatedAt = time.Now().UTC()
		}
		objects[index] = object
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketObjects)
		for _, object := range objects {
			if bucket.Get([]byte(object.Key)) != nil {
				continue
			}
			if err := putJSON(bucket, []byte(object.Key), object); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ReconcileRemote(ctx context.Context, input []journal.Object) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	remote := make(map[string]journal.Object, len(input))
	for _, object := range input {
		if err := validateKey(object.Key); err != nil {
			return err
		}
		if object.Version == "" || object.RemoteVersion == "" || object.Size < 0 {
			return journal.ErrCorrupt
		}
		object.Replicated = true
		object.PayloadID = ""
		if object.UpdatedAt.IsZero() {
			object.UpdatedAt = time.Now().UTC()
		}
		if _, duplicate := remote[object.Key]; duplicate {
			return journal.ErrCorrupt
		}
		remote[object.Key] = object
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketObjects)
		type existingObject struct {
			key    string
			object journal.Object
		}
		var existing []existingObject
		if err := bucket.ForEach(func(key, value []byte) error {
			var object journal.Object
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			existing = append(existing, existingObject{key: string(key), object: object})
			return nil
		}); err != nil {
			return err
		}
		for _, current := range existing {
			if !current.object.Replicated {
				delete(remote, current.key)
				continue
			}
			if replacement, found := remote[current.key]; found {
				if err := putJSON(bucket, []byte(current.key), replacement); err != nil {
					return err
				}
				delete(remote, current.key)
				continue
			}
			if err := bucket.Delete([]byte(current.key)); err != nil {
				return err
			}
		}
		for key, object := range remote {
			if err := putJSON(bucket, []byte(key), object); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Scan(ctx context.Context, prefix string) ([]journal.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	var objects []journal.Object
	err = db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketObjects).Cursor()
		for k, v := cursor.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var object journal.Object
			if err := json.Unmarshal(v, &object); err != nil {
				return err
			}
			objects = append(objects, object)
		}
		return nil
	})
	sortObjects(objects)
	return objects, err
}

func (s *Store) OpenPayload(ctx context.Context, entry journal.Entry) (journal.ReadSeekCloser, error) {
	return s.OpenPayloadByID(ctx, entry.PayloadID)
}

func (s *Store) OpenPayloadByID(ctx context.Context, payloadID string) (journal.ReadSeekCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if payloadID == "" {
		return nil, journal.ErrRemoteOnly
	}
	file, err := os.Open(s.payloadPath(payloadID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: payload %s", journal.ErrNotFound, payloadID)
	}
	return file, err
}

func (s *Store) Next(ctx context.Context, now time.Time) (journal.Entry, error) {
	if err := ctx.Err(); err != nil {
		return journal.Entry{}, err
	}
	db, err := s.database()
	if err != nil {
		return journal.Entry{}, err
	}
	var entry journal.Entry
	found := false
	err = db.View(func(tx *bolt.Tx) error {
		_, value := tx.Bucket(bucketEntries).Cursor().First()
		if value == nil {
			return nil
		}
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return journal.Entry{}, err
	}
	if !found {
		return journal.Entry{}, journal.ErrNoneAvailable
	}
	if entry.State == journal.StateFailed {
		return journal.Entry{}, fmt.Errorf("%w: sequence=%d: %s", journal.ErrReplicationFailed, entry.Sequence, entry.LastError)
	}
	if !entry.NextAttemptAt.IsZero() && now.Before(entry.NextAttemptAt) {
		return journal.Entry{}, journal.ErrNoneAvailable
	}
	return entry, nil
}

func (s *Store) MarkReplicated(ctx context.Context, sequence uint64, remoteVersion string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	var payloadID string
	err = db.Update(func(tx *bolt.Tx) error {
		entries := tx.Bucket(bucketEntries)
		cursor := entries.Cursor()
		firstKey, firstValue := cursor.First()
		if firstValue == nil {
			return journal.ErrNotFound
		}
		if binary.BigEndian.Uint64(firstKey) != sequence {
			return fmt.Errorf("journal replication out of order: first=%d completed=%d", binary.BigEndian.Uint64(firstKey), sequence)
		}
		completedKey := append([]byte(nil), firstKey...)
		var completed journal.Entry
		if err := json.Unmarshal(firstValue, &completed); err != nil {
			return err
		}
		payloadID = completed.PayloadID
		objects := tx.Bucket(bucketObjects)
		object, found, err := getObject(objects, completed.Key)
		if err != nil {
			return err
		}
		if found && object.Version == completed.Version {
			object.Replicated = true
			object.RemoteVersion = remoteVersion
			object.PayloadID = ""
			if err := putJSON(objects, []byte(object.Key), object); err != nil {
				return err
			}
		}
		type pendingUpdate struct {
			key   []byte
			entry journal.Entry
		}
		var updates []pendingUpdate
		for key, value := cursor.Next(); key != nil; key, value = cursor.Next() {
			var pending journal.Entry
			if err := json.Unmarshal(value, &pending); err != nil {
				return err
			}
			if pending.PreviousVersion == completed.Version && pending.ExpectedRemoteVersion == "" {
				pending.ExpectedRemoteVersion = remoteVersion
				updates = append(updates, pendingUpdate{key: append([]byte(nil), key...), entry: pending})
			}
		}
		for _, update := range updates {
			if err := putJSON(entries, update.key, update.entry); err != nil {
				return err
			}
		}
		if err := entries.Delete(completedKey); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put(keyReplicated, sequenceKey(sequence))
	})
	if err != nil {
		return err
	}
	if payloadID != "" {
		if err := s.removePayloadIfUnreferenced(payloadID); err != nil {
			s.cleanupErrors.Add(1)
			s.gcPending[payloadID] = struct{}{}
		}
	}
	s.retryPayloadGC()
	s.signal()
	return nil
}

func (s *Store) MarkRetry(ctx context.Context, sequence uint64, cause string, nextAttempt time.Time, failed bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		entries := tx.Bucket(bucketEntries)
		key := sequenceKey(sequence)
		value := entries.Get(key)
		if value == nil {
			return journal.ErrNotFound
		}
		var entry journal.Entry
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		entry.Attempts++
		entry.LastError = cause
		entry.NextAttemptAt = nextAttempt.UTC()
		meta := tx.Bucket(bucketMeta)
		failures := decodeSequence(meta.Get(keyUploadFailed))
		if failures < ^uint64(0) {
			failures++
		}
		if err := meta.Put(keyUploadFailed, sequenceKey(failures)); err != nil {
			return err
		}
		if failed {
			entry.State = journal.StateFailed
		}
		return putJSON(entries, key, entry)
	})
	if err == nil {
		s.signal()
	}
	return err
}

func (s *Store) RetryFailed(ctx context.Context, sequence uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		entries := tx.Bucket(bucketEntries)
		if sequence != 0 {
			key := sequenceKey(sequence)
			value := entries.Get(key)
			if value == nil {
				return journal.ErrNotFound
			}
			return requeueFailedEntry(entries, key, value)
		}
		var updates [][]byte
		if err := entries.ForEach(func(key, value []byte) error {
			var entry journal.Entry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			if entry.State == journal.StateFailed {
				updates = append(updates, append([]byte(nil), key...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range updates {
			if err := requeueFailedEntry(entries, key, entries.Get(key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		s.signal()
	}
	return err
}

func requeueFailedEntry(entries *bolt.Bucket, key, value []byte) error {
	var entry journal.Entry
	if err := json.Unmarshal(value, &entry); err != nil {
		return err
	}
	if entry.State != journal.StateFailed {
		return nil
	}
	entry.State = journal.StatePending
	entry.Attempts = 0
	entry.LastError = ""
	entry.NextAttemptAt = time.Time{}
	return putJSON(entries, key, entry)
}

func (s *Store) Wait(ctx context.Context, sequence uint64) error {
	for {
		// Capture the generation before reading state. A concurrent transition
		// then closes this channel, avoiding a lost wake-up between the read and
		// the select below.
		notify := s.notifyChannel()
		db, err := s.database()
		if err != nil {
			return err
		}
		var through uint64
		var entry *journal.Entry
		err = db.View(func(tx *bolt.Tx) error {
			through = decodeSequence(tx.Bucket(bucketMeta).Get(keyReplicated))
			if value := tx.Bucket(bucketEntries).Get(sequenceKey(sequence)); value != nil {
				var current journal.Entry
				if err := json.Unmarshal(value, &current); err != nil {
					return err
				}
				entry = &current
			}
			return nil
		})
		if err != nil {
			return err
		}
		if sequence <= through {
			return nil
		}
		if entry != nil && entry.State == journal.StateFailed {
			return fmt.Errorf("%w: sequence=%d: %s", journal.ErrReplicationFailed, sequence, entry.LastError)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (s *Store) Stats(ctx context.Context) (journal.Stats, error) {
	if err := ctx.Err(); err != nil {
		return journal.Stats{}, err
	}
	db, err := s.database()
	if err != nil {
		return journal.Stats{}, err
	}
	var stats journal.Stats
	stats.CleanupErrors = s.cleanupErrors.Load()
	if usage, usageErr := cacheevict.MeasureDiskUsage(s.Dir); usageErr == nil {
		stats.DiskCapacityBytes = usage.CapacityBytes
		stats.DiskAvailableBytes = usage.AvailableBytes
	}
	err = db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		stats.ReplicatedThrough = decodeSequence(meta.Get(keyReplicated))
		stats.UploadFailures = decodeSequence(meta.Get(keyUploadFailed))
		return tx.Bucket(bucketEntries).ForEach(func(_, value []byte) error {
			var entry journal.Entry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			stats.PendingEntries++
			if entry.Operation == journal.OperationPut {
				if entry.Size < 0 || stats.PendingBytes > math.MaxInt64-entry.Size {
					return journal.ErrCorrupt
				}
				stats.PendingBytes += entry.Size
			}
			if entry.State == journal.StateFailed {
				stats.FailedEntries++
			}
			if stats.OldestPendingAt.IsZero() || entry.CreatedAt.Before(stats.OldestPendingAt) {
				stats.OldestPendingAt = entry.CreatedAt
			}
			return nil
		})
	})
	return stats, err
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	db := s.db
	s.db = nil
	close(s.notify)
	s.notify = make(chan struct{})
	s.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.Close()
}

func (s *Store) stagePayload(ctx context.Context, source io.Reader) (string, int64, error) {
	if _, err := s.database(); err != nil {
		return "", 0, err
	}
	tmp, err := os.CreateTemp(s.payloadDir(), ".payload-*")
	if err != nil {
		return "", 0, err
	}
	tmpPath := tmp.Name()
	remove := true
	defer func() {
		_ = tmp.Close()
		if remove {
			_ = os.Remove(tmpPath)
		}
	}()
	hash := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(tmp, hash), source)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", 0, err
	}
	payloadID := hex.EncodeToString(hash.Sum(nil))
	destination := s.payloadPath(payloadID)
	if _, statErr := os.Stat(destination); statErr == nil {
		existingChecksum, checksumErr := fileChecksum(ctx, destination)
		if checksumErr != nil {
			return "", 0, checksumErr
		}
		if existingChecksum != payloadID {
			return "", 0, fmt.Errorf("journal payload collision or corruption for %s", payloadID)
		}
		return payloadID, written, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", 0, statErr
	}
	if err := os.Rename(tmpPath, destination); err != nil {
		if _, statErr := os.Stat(destination); statErr != nil {
			return "", 0, err
		}
	} else {
		remove = false
		if err := syncDir(s.payloadDir()); err != nil {
			return "", 0, err
		}
	}
	return payloadID, written, nil
}

func (s *Store) verifyAndCleanPayloads(ctx context.Context) error {
	referenced := map[string]struct{}{}
	err := s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if _, err := uuid.Parse(string(meta.Get(keyInstanceID))); err != nil {
			return fmt.Errorf("invalid journal identity: %w", err)
		}
		for _, key := range [][]byte{keyReplicated, keyUploadFailed} {
			if value := meta.Get(key); value != nil && len(value) != 8 {
				return journal.ErrCorrupt
			}
		}
		if err := tx.Bucket(bucketObjects).ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var object journal.Object
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			if string(key) != object.Key || object.Version == "" || object.Size < 0 {
				return journal.ErrCorrupt
			}
			if object.PayloadID != "" {
				if !validPayloadID(object.PayloadID) {
					return journal.ErrCorrupt
				}
				referenced[object.PayloadID] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
		return tx.Bucket(bucketEntries).ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var entry journal.Entry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			if len(key) != 8 || entry.Sequence == 0 || binary.BigEndian.Uint64(key) != entry.Sequence || entry.Version == "" || entry.Size < 0 {
				return journal.ErrCorrupt
			}
			if entry.Operation != journal.OperationPut && entry.Operation != journal.OperationDelete {
				return journal.ErrCorrupt
			}
			if entry.State != journal.StatePending && entry.State != journal.StateFailed {
				return journal.ErrCorrupt
			}
			if entry.Operation == journal.OperationPut {
				if !validPayloadID(entry.PayloadID) || entry.Checksum != entry.PayloadID {
					return journal.ErrCorrupt
				}
				referenced[entry.PayloadID] = struct{}{}
			} else if entry.PayloadID != "" {
				return journal.ErrCorrupt
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	for payloadID := range referenced {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := s.payloadPath(payloadID)
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("journal payload %s is missing: %w", payloadID, err)
		}
		if info.IsDir() {
			return fmt.Errorf("journal payload %s is not a file", payloadID)
		}
		checksum, err := fileChecksum(ctx, path)
		if err != nil {
			return err
		}
		if checksum != payloadID {
			return fmt.Errorf("journal payload %s checksum mismatch", payloadID)
		}
	}
	items, err := os.ReadDir(s.payloadDir())
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.IsDir() {
			continue
		}
		if _, ok := referenced[item.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(s.payloadDir(), item.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func validPayloadID(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s *Store) retryPayloadGC() {
	for payloadID := range s.gcPending {
		if err := s.removePayloadIfUnreferenced(payloadID); err != nil {
			s.cleanupErrors.Add(1)
			continue
		}
		delete(s.gcPending, payloadID)
	}
}

func (s *Store) removePayloadIfUnreferenced(payloadID string) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	referenced := false
	err = db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketObjects).ForEach(func(_, value []byte) error {
			var object journal.Object
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			if object.PayloadID == payloadID {
				referenced = true
			}
			return nil
		}); err != nil || referenced {
			return err
		}
		return tx.Bucket(bucketEntries).ForEach(func(_, value []byte) error {
			var entry journal.Entry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			if entry.PayloadID == payloadID {
				referenced = true
			}
			return nil
		})
	})
	if err != nil || referenced {
		return err
	}
	if err := os.Remove(s.payloadPath(payloadID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *Store) database() (*bolt.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil || s.closed {
		return nil, journal.ErrClosed
	}
	return s.db, nil
}

func (s *Store) signal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Store) notifyChannel() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

func (s *Store) payloadDir() string           { return filepath.Join(s.Dir, "payloads") }
func (s *Store) payloadPath(id string) string { return filepath.Join(s.payloadDir(), id) }

func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("blob key is required")
	}
	clean := filepath.ToSlash(filepath.Clean(key))
	if filepath.IsAbs(key) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(key, "\\") || strings.ContainsRune(key, '\x00') {
		return fmt.Errorf("invalid blob key %q", key)
	}
	return nil
}

func (s *Store) preflightPut(key, expectedVersion string, createOnly bool, size int64) error {
	if s.Config.MinFreeBytes > 0 {
		usage, err := cacheevict.MeasureDiskUsage(s.Dir)
		if err != nil {
			return err
		}
		if size > usage.AvailableBytes || usage.AvailableBytes-size < s.Config.MinFreeBytes {
			return fmt.Errorf("%w: available=%d payload=%d min_free=%d", journal.ErrBackpressure, usage.AvailableBytes, size, s.Config.MinFreeBytes)
		}
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	return db.View(func(tx *bolt.Tx) error {
		current, found, err := getObject(tx.Bucket(bucketObjects), key)
		if err != nil {
			return err
		}
		if createOnly && found && !current.Tombstone {
			return journal.ErrVersionMismatch
		}
		if expectedVersion != "" && (!found || current.Tombstone || current.Version != expectedVersion) {
			return journal.ErrVersionMismatch
		}
		return enforceCapacity(tx.Bucket(bucketEntries), s.Config, size)
	})
}

func enforceCapacity(entries *bolt.Bucket, cfg journal.Config, additionalBytes int64) error {
	var count int
	var bytes int64
	err := entries.ForEach(func(_, value []byte) error {
		var entry journal.Entry
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		count++
		if entry.Operation == journal.OperationPut {
			if entry.Size < 0 || bytes > math.MaxInt64-entry.Size {
				return journal.ErrCorrupt
			}
			bytes += entry.Size
		}
		return nil
	})
	if err != nil {
		return err
	}
	if cfg.MaxPendingEntries > 0 && count+1 > cfg.MaxPendingEntries {
		return fmt.Errorf("%w: entries=%d max=%d", journal.ErrBackpressure, count, cfg.MaxPendingEntries)
	}
	if additionalBytes < 0 {
		return journal.ErrCorrupt
	}
	if cfg.MaxPendingBytes > 0 && (additionalBytes > cfg.MaxPendingBytes || bytes > cfg.MaxPendingBytes-additionalBytes) {
		return fmt.Errorf("%w: bytes=%d additional=%d max=%d", journal.ErrBackpressure, bytes, additionalBytes, cfg.MaxPendingBytes)
	}
	return nil
}

func getObject(bucket *bolt.Bucket, key string) (journal.Object, bool, error) {
	value := bucket.Get([]byte(key))
	if value == nil {
		return journal.Object{}, false, nil
	}
	var object journal.Object
	if err := json.Unmarshal(value, &object); err != nil {
		return journal.Object{}, false, err
	}
	return object, true, nil
}

func putJSON(bucket *bolt.Bucket, key []byte, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, encoded)
}

func sequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func decodeSequence(value []byte) uint64 {
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func copyWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
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

func fileChecksum(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := copyWithContext(ctx, hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func mkdirAllDurable(path string, mode os.FileMode) error {
	path = filepath.Clean(path)
	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := syncDir(filepath.Dir(missing[index])); err != nil {
			return err
		}
		if err := syncDir(missing[index]); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Stable ordering is useful to callers comparing Scan results.
func sortObjects(objects []journal.Object) {
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
}
