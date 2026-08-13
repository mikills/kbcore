package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// shardConn is a pooled DuckDB connection for a single shard file.
// Callers must hold mu while using db to serialize access.
type shardConn struct {
	db      *sql.DB
	mu      sync.Mutex
	lastUse time.Time
}

// shardConnPool keeps warm DuckDB connections keyed by local file path.
const maxShardConnPoolEntries = 16

var errShardConnPoolEvicting = errors.New("DuckDB shard cache path is being evicted")

type shardConnPool struct {
	mu          sync.Mutex
	entries     map[string]*shardConn
	evicting    map[string]*shardEviction
	generations map[string]uint64
	inFlight    map[string]int
}

type shardEviction struct {
	count int
	done  chan struct{}
}

// GetOrOpen returns a pooled connection for localPath, creating one via openFn
// if not already cached. The returned shardConn is locked. the caller MUST
// unlock shardConn.mu after finishing use of shardConn.db.
func (p *shardConnPool) GetOrOpen(ctx context.Context, localPath string,
	openFn func(ctx context.Context, path string) (*sql.DB, error)) (*shardConn, error) {

	p.mu.Lock()
	generation := p.generationLocked(localPath)
	if p.pathEvictingLocked(localPath) {
		p.mu.Unlock()
		return nil, errShardConnPoolEvicting
	}
	if p.entries == nil {
		p.entries = make(map[string]*shardConn)
	}
	if sc, ok := p.entries[localPath]; ok {
		sc.mu.Lock()
		sc.lastUse = time.Now()
		p.mu.Unlock()
		return sc, nil
	}
	if p.inFlight == nil {
		p.inFlight = make(map[string]int)
	}
	p.inFlight[localPath]++
	p.mu.Unlock()

	db, err := openFn(ctx, localPath)
	if err != nil {
		p.mu.Lock()
		p.finishOpenLocked(localPath)
		p.mu.Unlock()
		return nil, err
	}

	sc := &shardConn{db: db, lastUse: time.Now()}

	p.mu.Lock()
	changed := p.pathEvictingLocked(localPath) || p.generationLocked(localPath) != generation
	p.finishOpenLocked(localPath)
	if changed {
		p.mu.Unlock()
		if closeErr := db.Close(); closeErr != nil {
			slog.Default().Warn("close shard connection opened during eviction failed", logKeyError, closeErr)
		}
		return nil, errShardConnPoolEvicting
	}
	// Another goroutine may have raced and inserted first.
	if existing, ok := p.entries[localPath]; ok {
		existing.mu.Lock()
		existing.lastUse = time.Now()
		p.mu.Unlock()
		if err := db.Close(); err != nil {
			slog.Default().Warn("close duplicate shard connection failed", "path", localPath, logKeyError, err)
		}
		return existing, nil
	}
	sc.mu.Lock()
	p.entries[localPath] = sc
	toClose := p.takeOverflowLocked(localPath)
	p.mu.Unlock()

	for _, stale := range toClose {
		stale.mu.Lock()
		if err := stale.db.Close(); err != nil {
			slog.Default().Warn("close LRU shard connection failed", logKeyError, err)
		}
		stale.mu.Unlock()
	}
	return sc, nil
}

func (p *shardConnPool) takeOverflowLocked(protectedPath string) []*shardConn {
	var toClose []*shardConn
	for len(p.entries) > maxShardConnPoolEntries {
		var oldestPath string
		var oldest *shardConn
		for path, candidate := range p.entries {
			if path == protectedPath {
				continue
			}
			if oldest == nil || candidate.lastUse.Before(oldest.lastUse) {
				oldestPath = path
				oldest = candidate
			}
		}
		if oldest == nil {
			break
		}
		delete(p.entries, oldestPath)
		toClose = append(toClose, oldest)
	}
	return toClose
}

// BeginEviction blocks new opens beneath prefix, closes existing handles, and
// returns a release function. The caller must keep the barrier until files are
// removed from disk.
func (p *shardConnPool) BeginEviction(prefix string) func() {
	prefix = strings.TrimSuffix(prefix, string(os.PathSeparator))
	p.mu.Lock()
	if p.evicting == nil {
		p.evicting = make(map[string]*shardEviction)
	}
	state := p.evicting[prefix]
	if state == nil {
		state = &shardEviction{done: make(chan struct{})}
		p.evicting[prefix] = state
	}
	state.count++
	if p.generations == nil {
		p.generations = make(map[string]uint64)
	}
	p.generations[prefix]++
	var toClose []*shardConn
	for key, sc := range p.entries {
		if pathWithinPrefix(key, prefix) {
			toClose = append(toClose, sc)
			delete(p.entries, key)
		}
	}
	p.mu.Unlock()

	for _, sc := range toClose {
		sc.mu.Lock()
		if err := sc.db.Close(); err != nil {
			slog.Default().Warn("close shard connection failed", logKeyError, err)
		}
		sc.mu.Unlock()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			state := p.evicting[prefix]
			if state != nil {
				state.count--
				if state.count == 0 {
					delete(p.evicting, prefix)
					close(state.done)
					if !p.hasEntriesWithinPrefixLocked(prefix) && !p.hasOpenWithinPrefixLocked(prefix) {
						delete(p.generations, prefix)
					}
				}
			}
			p.mu.Unlock()
		})
	}
}

// CloseByPrefix closes current handles without retaining an eviction barrier.
func (p *shardConnPool) CloseByPrefix(prefix string) {
	p.BeginEviction(prefix)()
}

func (p *shardConnPool) waitForEviction(ctx context.Context, path string) error {
	for {
		p.mu.Lock()
		var done <-chan struct{}
		for prefix, state := range p.evicting {
			if pathWithinPrefix(path, prefix) {
				done = state.done
				break
			}
		}
		p.mu.Unlock()
		if done == nil {
			return nil
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *shardConnPool) finishOpenLocked(path string) {
	if p.inFlight[path] <= 1 {
		delete(p.inFlight, path)
	} else {
		p.inFlight[path]--
	}
	for prefix := range p.generations {
		if pathWithinPrefix(path, prefix) && p.evicting[prefix] == nil &&
			!p.hasEntriesWithinPrefixLocked(prefix) && !p.hasOpenWithinPrefixLocked(prefix) {
			delete(p.generations, prefix)
		}
	}
}

func (p *shardConnPool) hasOpenWithinPrefixLocked(prefix string) bool {
	for path := range p.inFlight {
		if pathWithinPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (p *shardConnPool) hasEntriesWithinPrefixLocked(prefix string) bool {
	for path := range p.entries {
		if pathWithinPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (p *shardConnPool) generationLocked(path string) uint64 {
	var generation uint64
	for prefix, value := range p.generations {
		if pathWithinPrefix(path, prefix) {
			generation += value
		}
	}
	return generation
}

func (p *shardConnPool) pathEvictingLocked(path string) bool {
	for prefix := range p.evicting {
		if pathWithinPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func pathWithinPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+string(os.PathSeparator))
}

// CloseAll closes every pooled connection. Called on shutdown.
func (p *shardConnPool) CloseAll() {
	p.mu.Lock()
	entries := p.entries
	p.entries = nil
	p.mu.Unlock()

	for _, sc := range entries {
		sc.mu.Lock()
		if err := sc.db.Close(); err != nil {
			slog.Default().Warn("close shard connection failed", logKeyError, err)
		}
		sc.mu.Unlock()
	}
}
