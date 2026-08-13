package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestShardConnPoolBoundsEntries(t *testing.T) {
	var pool shardConnPool
	dir := t.TempDir()
	for i := 0; i < maxShardConnPoolEntries+1; i++ {
		path := filepath.Join(dir, fmt.Sprintf("pool-%03d.duckdb", i))
		conn, err := pool.GetOrOpen(context.Background(), path, func(_ context.Context, p string) (*sql.DB, error) {
			return sql.Open("duckdb", p)
		})
		require.NoError(t, err)
		conn.mu.Unlock()
	}
	t.Cleanup(pool.CloseAll)

	pool.mu.Lock()
	defer pool.mu.Unlock()
	require.Len(t, pool.entries, maxShardConnPoolEntries)
	require.Contains(t, pool.entries, filepath.Join(dir, fmt.Sprintf("pool-%03d.duckdb", maxShardConnPoolEntries)))
}

func TestShardConnPoolEvictionBlocksReopen(t *testing.T) {
	var pool shardConnPool
	dir := filepath.Join(t.TempDir(), "kb")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, "shard.duckdb")
	openStarted := make(chan struct{})
	continueOpen := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := pool.GetOrOpen(context.Background(), path, func(_ context.Context, p string) (*sql.DB, error) {
			close(openStarted)
			<-continueOpen
			return sql.Open("duckdb", p)
		})
		result <- err
	}()
	<-openStarted
	release := pool.BeginEviction(filepath.Dir(path))
	close(continueOpen)
	require.ErrorIs(t, <-result, errShardConnPoolEvicting)

	opened := false
	_, err := pool.GetOrOpen(context.Background(), path, func(context.Context, string) (*sql.DB, error) {
		opened = true
		return nil, nil
	})
	require.ErrorIs(t, err, errShardConnPoolEvicting)
	require.False(t, opened)

	release()
	conn, err := pool.GetOrOpen(context.Background(), path, func(_ context.Context, p string) (*sql.DB, error) {
		return sql.Open("duckdb", p)
	})
	require.NoError(t, err)
	conn.mu.Unlock()
	pool.CloseAll()
}

func TestShardConnPoolRejectsOpenThatSpansCompletedEviction(t *testing.T) {
	var pool shardConnPool
	dir := filepath.Join(t.TempDir(), "kb")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, "shard.duckdb")
	openStarted := make(chan struct{})
	continueOpen := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := pool.GetOrOpen(context.Background(), path, func(_ context.Context, p string) (*sql.DB, error) {
			close(openStarted)
			<-continueOpen
			return sql.Open("duckdb", p)
		})
		result <- err
	}()
	<-openStarted
	release := pool.BeginEviction(dir)
	release()
	close(continueOpen)
	require.ErrorIs(t, <-result, errShardConnPoolEvicting)
	pool.mu.Lock()
	require.Empty(t, pool.entries)
	pool.mu.Unlock()
}

func TestShardConnPoolCloseByPrefix(t *testing.T) {
	var pool shardConnPool
	path := filepath.Join(t.TempDir(), "pool-test.duckdb")

	conn, err := pool.GetOrOpen(context.Background(), path, func(_ context.Context, p string) (*sql.DB, error) {
		return sql.Open("duckdb", p)
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		pool.CloseByPrefix(filepath.Dir(path))
		close(done)
	}()

	select {
	case <-done:
		require.Fail(t, "CloseByPrefix returned before borrowed connection was released")
	case <-time.After(50 * time.Millisecond):
	}

	conn.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "CloseByPrefix did not complete after borrowed connection release")
	}
}
