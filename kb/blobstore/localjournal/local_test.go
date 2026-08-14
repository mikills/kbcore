package localjournal_test

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/blobstore/localjournal"
	"github.com/stretchr/testify/require"
)

func TestLocalJournalAcknowledgedPutSurvivesProcessExit(t *testing.T) {
	if dir := os.Getenv("MINNOW_JOURNAL_CRASH_DIR"); dir != "" {
		store := localjournal.New(dir, journal.Config{})
		if err := store.Open(context.Background()); err != nil {
			os.Exit(2)
		}
		if _, _, err := store.PutBytes(context.Background(), "crash/object", []byte("durable"), ""); err != nil {
			os.Exit(3)
		}
		// Intentionally bypass Close and all test defers.
		os.Exit(0)
	}

	dir := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestLocalJournalAcknowledgedPutSurvivesProcessExit$")
	command.Env = append(os.Environ(), "MINNOW_JOURNAL_CRASH_DIR="+dir)
	output, err := command.CombinedOutput()
	require.NoErrorf(t, err, "child output: %s", output)
	reopened := localjournal.New(dir, journal.Config{})
	require.NoError(t, reopened.Open(context.Background()))
	defer reopened.Close()
	object, err := reopened.Get(context.Background(), "crash/object")
	require.NoError(t, err)
	reader, err := reopened.OpenPayloadByID(context.Background(), object.PayloadID)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, []byte("durable"), data)
}

func TestLocalJournalRestartOwnsPayloadAndPreservesOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(t.TempDir(), "temporary-shard")
	require.NoError(t, os.WriteFile(source, []byte("shard-v1"), 0o600))

	first := localjournal.New(dir, journal.Config{})
	require.NoError(t, first.Open(ctx))
	object, put, err := first.PutFile(ctx, "kb/shard.duckdb", source, "")
	require.NoError(t, err)
	require.NoError(t, os.Remove(source), "the caller-owned source may disappear immediately")
	_, obsoletePut, err := first.PutBytes(ctx, "kb/obsolete.duckdb", []byte("obsolete"), "")
	require.NoError(t, err)
	deletion, err := first.Delete(ctx, "kb/obsolete.duckdb")
	require.NoError(t, err)
	require.Less(t, put.Sequence, obsoletePut.Sequence)
	require.Less(t, obsoletePut.Sequence, deletion.Sequence)
	require.NoError(t, first.Close())

	reopened := localjournal.New(dir, journal.Config{})
	require.NoError(t, reopened.Open(ctx))
	defer reopened.Close()
	got, err := reopened.Get(ctx, "kb/shard.duckdb")
	require.NoError(t, err)
	require.Equal(t, object.Version, got.Version)
	entry, err := reopened.Next(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, put.Sequence, entry.Sequence)
	reader, err := reopened.OpenPayload(ctx, entry)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, []byte("shard-v1"), data)

	require.NoError(t, reopened.MarkReplicated(ctx, entry.Sequence, "remote-v1"))
	next, err := reopened.Next(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, obsoletePut.Sequence, next.Sequence)
	require.NoError(t, reopened.MarkReplicated(ctx, next.Sequence, "remote-obsolete"))
	next, err = reopened.Next(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, deletion.Sequence, next.Sequence)
}

func TestLocalJournalExclusivelyOwnsItsDirectory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first := localjournal.New(dir, journal.Config{})
	require.NoError(t, first.Open(ctx))
	defer first.Close()
	second := localjournal.New(dir, journal.Config{})
	start := time.Now()
	err := second.Open(ctx)
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestLocalJournalCASCreateOnlyAndTombstone(t *testing.T) {
	ctx := context.Background()
	store := openJournal(t, journal.Config{})
	created, first, err := store.CreateBytes(ctx, "object", []byte("one"))
	require.NoError(t, err)
	_, _, err = store.CreateBytes(ctx, "object", []byte("duplicate"))
	require.ErrorIs(t, err, journal.ErrVersionMismatch)
	_, _, err = store.PutBytes(ctx, "object", []byte("stale"), "wrong")
	require.ErrorIs(t, err, journal.ErrVersionMismatch)

	updated, second, err := store.PutBytes(ctx, "object", []byte("two"), created.Version)
	require.NoError(t, err)
	require.NotEqual(t, created.Version, updated.Version)
	require.NoError(t, store.MarkReplicated(ctx, first.Sequence, "etag-one"))
	next, err := store.Next(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, second.Sequence, next.Sequence)
	require.Equal(t, "etag-one", next.ExpectedRemoteVersion)
	require.NoError(t, store.MarkReplicated(ctx, second.Sequence, "etag-two"))

	deletion, err := store.Delete(ctx, "object")
	require.NoError(t, err)
	tombstone, err := store.Get(ctx, "object")
	require.NoError(t, err)
	require.True(t, tombstone.Tombstone)
	require.NoError(t, store.MarkReplicated(ctx, deletion.Sequence, ""))
	_, _, err = store.CreateBytes(ctx, "object", []byte("recreated"))
	require.NoError(t, err, "a remotely replicated tombstone makes the logical key absent")
}

func TestLocalJournalConcurrentSamePayloadCannotBeGarbageCollectedBeforeCommit(t *testing.T) {
	ctx := context.Background()
	store := openJournal(t, journal.Config{})
	for iteration := 0; iteration < 20; iteration++ {
		_, first, err := store.PutBytes(ctx, "first", []byte("shared-payload"), "")
		require.NoError(t, err)
		done := make(chan error, 2)
		go func() { done <- store.MarkReplicated(ctx, first.Sequence, "etag") }()
		go func(index int) {
			_, _, putErr := store.PutBytes(ctx, "second", []byte("shared-payload"), "")
			done <- putErr
		}(iteration)
		require.NoError(t, <-done)
		require.NoError(t, <-done)
		object, err := store.Get(ctx, "second")
		require.NoError(t, err)
		reader, err := store.OpenPayloadByID(ctx, object.PayloadID)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		entry, err := store.Next(ctx, time.Now().Add(time.Minute))
		require.NoError(t, err)
		require.NoError(t, store.MarkReplicated(ctx, entry.Sequence, "etag-second"))
	}
}

func TestLocalJournalBackpressureAndFailedWait(t *testing.T) {
	ctx := context.Background()
	store := openJournal(t, journal.Config{MaxPendingEntries: 2, MaxPendingBytes: 5})
	_, first, err := store.PutBytes(ctx, "one", []byte("12345"), "")
	require.NoError(t, err)
	_, _, err = store.PutBytes(ctx, "two", []byte("x"), "")
	require.ErrorIs(t, err, journal.ErrBackpressure)

	require.NoError(t, store.MarkRetry(ctx, first.Sequence, "s3 unavailable", time.Now(), true))
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = store.Wait(waitCtx, first.Sequence)
	require.ErrorIs(t, err, journal.ErrReplicationFailed)
	stats, err := store.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PendingEntries)
	require.Equal(t, 1, stats.FailedEntries)
	require.EqualValues(t, 5, stats.PendingBytes)
	require.EqualValues(t, 1, stats.UploadFailures)
	require.False(t, stats.OldestPendingAt.IsZero())
	require.Positive(t, stats.DiskCapacityBytes)
	require.Positive(t, stats.DiskAvailableBytes)
	require.NoError(t, store.RetryFailed(ctx, 0))
	_, err = store.Next(ctx, time.Now())
	require.NoError(t, err)
}

func TestLocalJournalRejectsOversizedPayloadBeforeStaging(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := localjournal.New(dir, journal.Config{MaxPendingBytes: 5})
	require.NoError(t, store.Open(ctx))
	defer store.Close()
	_, _, err := store.PutBytes(ctx, "too-large", []byte("123456"), "")
	require.ErrorIs(t, err, journal.ErrBackpressure)
	items, err := os.ReadDir(filepath.Join(dir, "payloads"))
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestLocalJournalPreservesEmergencyFreeSpaceBeforeStaging(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := localjournal.New(dir, journal.Config{MinFreeBytes: math.MaxInt64})
	require.NoError(t, store.Open(ctx))
	defer store.Close()
	_, _, err := store.PutBytes(ctx, "reserved", []byte("payload"), "")
	require.ErrorIs(t, err, journal.ErrBackpressure)
	items, err := os.ReadDir(filepath.Join(dir, "payloads"))
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestLocalJournalRejectsUnsafeKeys(t *testing.T) {
	store := openJournal(t, journal.Config{})
	for _, key := range []string{"", "../escape", "a/../../escape", "/absolute", `a\\escape`} {
		_, _, err := store.PutBytes(context.Background(), key, []byte("x"), "")
		require.Error(t, err, key)
	}
}

func TestLocalJournalRefusesMissingOrCorruptCommittedPayload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := localjournal.New(dir, journal.Config{})
	require.NoError(t, store.Open(ctx))
	_, entry, err := store.PutBytes(ctx, "object", []byte("value"), "")
	require.NoError(t, err)
	reader, err := store.OpenPayload(ctx, entry)
	require.NoError(t, err)
	path := reader.(*os.File).Name()
	require.NoError(t, reader.Close())
	require.NoError(t, store.Close())
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))

	reopened := localjournal.New(dir, journal.Config{})
	err = reopened.Open(ctx)
	require.ErrorContains(t, err, "checksum mismatch")
	require.NoError(t, reopened.Close())
}

func TestLocalJournalWaitWakesAfterReplication(t *testing.T) {
	ctx := context.Background()
	store := openJournal(t, journal.Config{})
	_, entry, err := store.PutBytes(ctx, "object", []byte("value"), "")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- store.Wait(ctx, entry.Sequence) }()
	require.NoError(t, store.MarkReplicated(ctx, entry.Sequence, "etag"))
	require.NoError(t, <-done)
	stats, err := store.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, entry.Sequence, stats.ReplicatedThrough)
	require.Zero(t, stats.PendingEntries)
	_, err = store.OpenPayload(ctx, entry)
	require.True(t, errors.Is(err, journal.ErrNotFound) || errors.Is(err, os.ErrNotExist))
}

func openJournal(t *testing.T, cfg journal.Config) *localjournal.Store {
	t.Helper()
	store := localjournal.New(t.TempDir(), cfg)
	require.NoError(t, store.Open(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}
