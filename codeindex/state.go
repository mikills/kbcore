package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	minnowcode "github.com/mikills/minnow/kb/codeindex"
)

const indexStateSchema = "codeindex.state/v2"

type indexState struct {
	SourcePath    string               `json:"-"`
	Legacy        bool                 `json:"-"`
	SchemaVersion string               `json:"schema_version"`
	KBID          string               `json:"kb_id"`
	ScopeID       string               `json:"scope_id"`
	RepoID        string               `json:"repo_id"`
	Ref           string               `json:"ref,omitempty"`
	Root          string               `json:"root"`
	Pipeline      string               `json:"pipeline"`
	UpdatedAt     time.Time            `json:"updated_at"`
	Files         map[string]stateFile `json:"files"`
}

type stateFile struct {
	Hash      string   `json:"hash"`
	SizeBytes int64    `json:"size_bytes"`
	Language  string   `json:"language,omitempty"`
	ChunkIDs  []string `json:"chunk_ids"`
}

type indexStatus struct {
	KBID        string     `json:"kb_id"`
	ScopeID     string     `json:"scope_id"`
	IndexKey    string     `json:"index_key"`
	Description string     `json:"description"`
	MinnowURL   string     `json:"minnow_url"`
	Root        string     `json:"root"`
	RepoID      string     `json:"repo_id"`
	Ref         string     `json:"ref,omitempty"`
	Indexed     bool       `json:"indexed"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	FileCount   int        `json:"file_count"`
	ChunkCount  int        `json:"chunk_count"`
	StatePath   string     `json:"state_path"`
	Phase       string     `json:"phase,omitempty"`
	Recoverable bool       `json:"recoverable,omitempty"`
}

func loadIndexState(target indexTarget) (indexState, string, bool, error) {
	path := indexStatePath(target)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && target.LegacyIndexKey != "" {
		legacyPath := indexStatePathForKey(target, target.LegacyIndexKey)
		if legacyData, legacyErr := os.ReadFile(legacyPath); legacyErr == nil {
			data, err, path = legacyData, nil, legacyPath
		} else if !errors.Is(legacyErr, os.ErrNotExist) {
			return indexState{}, legacyPath, false, legacyErr
		}
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return emptyIndexState(target), indexStatePath(target), false, nil
		}
		return indexState{}, path, false, err
	}
	var state indexState
	if err := json.Unmarshal(data, &state); err != nil {
		return indexState{}, path, false, err
	}
	if state.SchemaVersion != indexStateSchema && state.SchemaVersion != "codeindex.state/v1" {
		return indexState{}, path, false, fmt.Errorf("unsupported state schema %q", state.SchemaVersion)
	}
	state.Legacy = state.SchemaVersion == "codeindex.state/v1"
	if !kbIDMatchesTarget(state.KBID, target) || state.RepoID != target.RepoID || state.Ref != target.Ref || state.Root != target.Root {
		return indexState{}, path, false, fmt.Errorf("state identity does not match the selected index")
	}
	if state.ScopeID != "" && state.ScopeID != target.ScopeID {
		return indexState{}, path, false, fmt.Errorf("state scope does not match the selected index")
	}
	if state.Files == nil {
		state.Files = map[string]stateFile{}
	}
	state.SourcePath = path
	if state.ScopeID == "" {
		state.ScopeID = target.ScopeID
	}
	state.SchemaVersion = indexStateSchema
	return state, path, true, nil
}

func emptyIndexState(target indexTarget) indexState {
	return indexState{
		SchemaVersion: indexStateSchema, KBID: target.KBID, ScopeID: target.ScopeID, RepoID: target.RepoID,
		Ref: target.Ref, Root: target.Root, Files: map[string]stateFile{},
	}
}

func kbIDMatchesTarget(kbID string, target indexTarget) bool {
	if kbID == "" || target.KBID == "" {
		return false
	}
	return (target.LegacyKBID != "" && (kbID == target.LegacyKBID || validGeneratedKBID(kbID, target.LegacyKBID))) ||
		kbID == target.KBID || validGeneratedKBID(kbID, target.KBID)
}

func validGeneratedKBID(kbID, base string) bool {
	suffix, ok := strings.CutPrefix(kbID, base+"-")
	if !ok || len(suffix) != generationSuffixLen {
		return false
	}
	_, err := hex.DecodeString(suffix)
	return err == nil
}

func saveIndexState(target indexTarget, state indexState) (string, error) {
	path := indexStatePath(target)
	if err := writeIndexStateFile(path, state); err != nil {
		return path, err
	}
	if state.SourcePath != "" && state.SourcePath != path {
		if err := os.Remove(state.SourcePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return path, fmt.Errorf("remove migrated state %s: %w", state.SourcePath, err)
		}
	}
	return path, nil
}

func writeIndexStateFile(path string, state indexState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".state-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func indexStatePath(target indexTarget) string {
	return indexStatePathForKey(target, target.IndexKey)
}

func indexStatePathForKey(target indexTarget, key string) string {
	name := minnowcode.SanitizeKey(key) + "-" + shortHash(target.Root) + ".json"
	return filepath.Join(target.StateRoot, ".minnow", "codeindex", name)
}

type indexLock struct {
	PID       int       `json:"pid"`
	Token     string    `json:"token"`
	CreatedAt time.Time `json:"created_at"`
}

type heldIndexLock struct {
	path    string
	token   string
	done    chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func acquireIndexLock(target indexTarget, staleAfter time.Duration) (func(), error) {
	return acquireLockPath(indexStatePath(target)+".lock", target.IndexKey, staleAfter)
}

func acquireLockPath(path, name string, staleAfter time.Duration) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	for range 3 {
		lock := indexLock{PID: os.Getpid(), Token: newLockToken(), CreatedAt: time.Now().UTC()}
		data, err := json.Marshal(lock)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			if _, writeErr := file.Write(data); writeErr != nil {
				file.Close()
				_ = os.Remove(path)
				return nil, writeErr
			}
			if closeErr := file.Close(); closeErr != nil {
				_ = os.Remove(path)
				return nil, closeErr
			}
			held := startHeldIndexLock(path, lock.Token, staleAfter)
			return held.release, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		active, activeErr := indexLockIsActive(path, staleAfter)
		if activeErr != nil {
			if errors.Is(activeErr, os.ErrNotExist) {
				continue
			}
			return nil, activeErr
		}
		if active {
			return nil, fmt.Errorf("index refresh already running for %s (lock %s)", name, path)
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return nil, removeErr
		}
	}
	return nil, fmt.Errorf("acquire index lock %s", path)
}

func startHeldIndexLock(path, token string, staleAfter time.Duration) *heldIndexLock {
	held := &heldIndexLock{
		path: path, token: token, done: make(chan struct{}), stopped: make(chan struct{}),
	}
	go held.heartbeat(staleAfter)
	return held
}

func indexLockIsActive(path string, staleAfter time.Duration) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if staleAfter <= 0 || time.Since(info.ModTime()) <= staleAfter {
		return true, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var lock indexLock
	if json.Unmarshal(data, &lock) != nil || lock.PID <= 0 {
		return false, nil
	}
	alive, known := processIsAlive(lock.PID)
	if !known {
		// When the platform cannot probe a PID, prefer preserving a possibly
		// active owner over stealing its lock.
		return true, nil
	}
	return alive, nil
}

func newLockToken() string {
	var token [16]byte
	if _, err := rand.Read(token[:]); err == nil {
		return hex.EncodeToString(token[:])
	}
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
}

func (lock *heldIndexLock) release() {
	lock.once.Do(func() {
		close(lock.done)
		<-lock.stopped
		if lock.ownsPath() {
			_ = os.Remove(lock.path)
		}
	})
}

func (lock *heldIndexLock) heartbeat(staleAfter time.Duration) {
	defer close(lock.stopped)
	interval := staleAfter / 3
	if interval <= 0 || interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-lock.done:
			return
		case now := <-ticker.C:
			if !lock.ownsPath() {
				return
			}
			_ = os.Chtimes(lock.path, now, now)
		}
	}
}

func (lock *heldIndexLock) ownsPath() bool {
	data, err := os.ReadFile(lock.path)
	if err != nil {
		return false
	}
	var current indexLock
	return json.Unmarshal(data, &current) == nil && current.Token != "" && current.Token == lock.token
}

func statusFromState(target indexTarget, minnowURL, path string, state indexState) indexStatus {
	status := indexStatus{
		KBID: target.KBID, ScopeID: firstNonEmpty(state.ScopeID, target.ScopeID), IndexKey: target.IndexKey, Description: target.Description,
		MinnowURL: minnowURL, Root: target.Root, RepoID: target.RepoID, Ref: target.Ref,
		StatePath: path,
	}
	status.FileCount = len(state.Files)
	for _, file := range state.Files {
		status.ChunkCount += len(file.ChunkIDs)
	}
	if state.UpdatedAt.IsZero() {
		return status
	}
	status.Indexed = true
	status.UpdatedAt = &state.UpdatedAt
	return status
}
