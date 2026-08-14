// Package journal defines the durable catalog/outbox contract used by tiered
// blob storage. Implementations own queued payload bytes until replication is
// acknowledged; retaining caller-owned temporary paths is forbidden.
package journal

import (
	"context"
	"errors"
	"io"
	"time"
)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type Operation string

const (
	OperationPut    Operation = "put"
	OperationDelete Operation = "delete"
)

type EntryState string

const (
	StatePending EntryState = "pending"
	StateFailed  EntryState = "failed"
)

// Object is the journal's authoritative view of one key. Version is a logical
// CAS token. RemoteVersion is the backing store's opaque native version.
type Object struct {
	Key           string
	Version       string
	RemoteVersion string
	UpdatedAt     time.Time
	Size          int64
	Checksum      string
	PayloadID     string
	Tombstone     bool
	Replicated    bool
}

// Entry is one globally ordered replication operation.
type Entry struct {
	Sequence              uint64
	Operation             Operation
	Key                   string
	Version               string
	ExpectedVersion       string
	PreviousVersion       string
	ExpectedRemoteVersion string
	CreateOnly            bool
	PayloadID             string
	Checksum              string
	Size                  int64
	CreatedAt             time.Time
	Attempts              int
	NextAttemptAt         time.Time
	LastError             string
	State                 EntryState
}

type Stats struct {
	PendingEntries     int
	PendingBytes       int64
	FailedEntries      int
	CleanupErrors      uint64
	UploadFailures     uint64
	OldestPendingAt    time.Time
	DiskCapacityBytes  int64
	DiskAvailableBytes int64
	ReplicatedThrough  uint64
}

type Config struct {
	MaxPendingEntries int
	MaxPendingBytes   int64
	MinFreeBytes      int64
}

var (
	ErrNotFound          = errors.New("replication journal: object not found")
	ErrVersionMismatch   = errors.New("replication journal: version mismatch")
	ErrNoneAvailable     = errors.New("replication journal: no entry available")
	ErrRemoteOnly        = errors.New("replication journal: payload is remote-only")
	ErrBackpressure      = errors.New("replication journal: pending capacity exceeded")
	ErrReplicationFailed = errors.New("replication journal: replication failed")
	ErrCorrupt           = errors.New("replication journal: corrupt state")
	ErrClosed            = errors.New("replication journal: closed")
)

// Store combines the local namespace catalog and replication outbox so a
// visible mutation and its enqueue are committed atomically.
type Store interface {
	Open(ctx context.Context) error
	// Identity is stable for the lifetime of the journal directory and is
	// used to claim one non-expiring remote-prefix ownership record.
	Identity(ctx context.Context) (string, error)

	// Mutation methods atomically install Object and append Entry. Object.Key,
	// Entry.Key, and the requested key must match; Object.Version and
	// Entry.Version must be the same globally unique stable logical token.
	// For conditional puts ExpectedVersion and PreviousVersion must both equal
	// the caller's expected token, which is checked atomically against current
	// state; the new Version must differ. PreviousVersion otherwise identifies
	// the replaced logical object. New/after-delete
	// puts set CreateOnly with no remote expectation; other puts and deletes
	// retain or later receive the predecessor's non-empty opaque remote version.
	// Put entries own an immutable seekable payload whose PayloadID and Checksum
	// are the lowercase SHA-256, and whose Size is exact. Delete entries have no
	// payload and size zero. Returned success means this catalog/outbox state and
	// payload ownership are durable.
	PutFile(ctx context.Context, key, src, expectedVersion string) (Object, Entry, error)
	PutBytes(ctx context.Context, key string, data []byte, expectedVersion string) (Object, Entry, error)
	CreateFile(ctx context.Context, key, src string) (Object, Entry, error)
	CreateBytes(ctx context.Context, key string, data []byte) (Object, Entry, error)
	Delete(ctx context.Context, key string) (Entry, error)

	// Next must return the lowest non-completed sequence. Sequences are
	// monotonically increasing and never reused; MarkReplicated must reject
	// out-of-order completion. Wait returns nil only after the requested
	// sequence is durably complete and returns ErrReplicationFailed for a
	// permanently failed entry. Close wakes waiters, and Open after a process
	// restart must expose every acknowledged mutation and pending operation.
	// Capacity checks and each namespace mutation/outbox append are atomic.
	//
	// Get returns tombstones as Object values. It returns ErrNotFound only
	// when the journal has never observed the key.
	Get(ctx context.Context, key string) (Object, error)
	// Seed records an already-remote object only if the key has no local
	// catalog record. It never overwrites a local mutation or tombstone.
	Seed(ctx context.Context, object Object) (Object, error)
	// ReconcileRemote atomically applies a complete remote namespace snapshot:
	// replicated records are replaced or removed to match the snapshot, while
	// unreplicated current objects and their pending chains are retained.
	ReconcileRemote(ctx context.Context, objects []Object) error
	Scan(ctx context.Context, prefix string) ([]Object, error)
	// OpenPayload opens the immutable payload owned by entry.
	OpenPayload(ctx context.Context, entry Entry) (ReadSeekCloser, error)
	// OpenPayloadByID is used by local-overlay reads before replication.
	OpenPayloadByID(ctx context.Context, payloadID string) (ReadSeekCloser, error)

	Next(ctx context.Context, now time.Time) (Entry, error)
	// MarkReplicated atomically completes the lowest sequence, updates the
	// matching current catalog object with remoteVersion, clears its local
	// payload reference, and copies remoteVersion into every pending successor
	// whose PreviousVersion equals the completed logical Version and whose
	// ExpectedRemoteVersion is empty. Payload reclamation may occur only after
	// that transaction and only when no catalog or outbox reference remains.
	MarkReplicated(ctx context.Context, sequence uint64, remoteVersion string) error
	MarkRetry(ctx context.Context, sequence uint64, cause string, nextAttempt time.Time, failed bool) error
	// RetryFailed requeues one failed sequence; sequence 0 requeues all failed
	// entries in order. Implementations must leave non-failed entries unchanged.
	RetryFailed(ctx context.Context, sequence uint64) error
	Wait(ctx context.Context, sequence uint64) error
	Stats(ctx context.Context) (Stats, error)

	Close() error
}
