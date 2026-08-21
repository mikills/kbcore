package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	journalSuffix     = ".journal"
	journalKBPrefix   = "kb "
	journalChunkPfx   = "i "
	journalStalePfx   = "s "
	journalPermission = 0o600
)

type uploadRecorder interface {
	record(ids []string) error
}

type uploadJournal struct {
	path string
	file *os.File
}

func uploadJournalPath(target indexTarget) string {
	return indexStatePath(target) + journalSuffix
}

// The KBID is recorded because a run that never saves state gets a fresh KBID
// next time, and orphans must be deleted from the KB that actually received them.
func openUploadJournal(path, kbID string) (*uploadJournal, error) {
	if kbID == "" {
		return nil, fmt.Errorf("upload journal requires a kb id")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, journalPermission)
	if err != nil {
		return nil, err
	}
	journal := &uploadJournal{path: path, file: file}
	if _, err := file.WriteString(journalKBPrefix + kbID + "\n"); err != nil {
		journal.close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		journal.close()
		return nil, err
	}
	return journal, nil
}

func (j *uploadJournal) record(ids []string) error {
	return j.write(journalChunkPfx, ids)
}

// Paths whose stored chunks are about to be deleted. If the run dies before it
// saves state, state still claims those chunks exist and the file would be
// silently missing from the index until its content changes again.
func (j *uploadJournal) recordStale(paths []string) error {
	return j.write(journalStalePfx, paths)
}

func (j *uploadJournal) write(prefix string, values []string) error {
	if len(values) == 0 {
		return nil
	}
	var buf strings.Builder
	for _, value := range values {
		buf.WriteString(prefix)
		buf.WriteString(value)
		buf.WriteByte('\n')
	}
	if _, err := j.file.WriteString(buf.String()); err != nil {
		return err
	}
	return j.file.Sync()
}

func (j *uploadJournal) close() error {
	if j == nil || j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	return err
}

func (j *uploadJournal) remove() error {
	if err := j.close(); err != nil {
		return err
	}
	return removeIfExists(j.path)
}

type journalContents struct {
	kbID  string
	ids   []string
	stale []string
}

func loadUploadJournal(path string) (journalContents, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return journalContents{}, nil
	}
	if err != nil {
		return journalContents{}, err
	}
	defer file.Close()
	var contents journalContents
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, journalKBPrefix):
			contents.kbID = strings.TrimSpace(strings.TrimPrefix(line, journalKBPrefix))
		case strings.HasPrefix(line, journalChunkPfx):
			if id := strings.TrimSpace(strings.TrimPrefix(line, journalChunkPfx)); id != "" {
				contents.ids = append(contents.ids, id)
			}
		case strings.HasPrefix(line, journalStalePfx):
			if path := strings.TrimPrefix(line, journalStalePfx); path != "" {
				contents.stale = append(contents.stale, path)
			}
		}
	}
	return contents, scanner.Err()
}

// journalRecovery names the run doing the recovering, whose own state file is
// not the one beside the journal while an index key is being migrated.
type journalRecovery struct {
	ownJournal string
	statePath  string
	staleAfter time.Duration
}

// startUploadJournal returns the paths recovery dropped from this target's
// state, which the caller must forget from the copy it read beforehand.
func startUploadJournal(
	ctx context.Context,
	client documentDeleter,
	target indexTarget,
	own journalRecovery,
	progress *progressReporter,
) (*uploadJournal, []string, error) {
	path := own.ownJournal
	invalidated, err := recoverUploadJournals(ctx, client, filepath.Dir(path), own, progress)
	if err != nil {
		return nil, nil, err
	}
	journal, err := openUploadJournal(path, target.KBID)
	if err != nil {
		return nil, nil, err
	}
	return journal, invalidated[path], nil
}

// Chunks ingested but never recorded in state are unreachable by any later
// delete. Reports the paths dropped from each state file, keyed by journal.
func recoverUploadJournals(
	ctx context.Context,
	client documentDeleter,
	dir string,
	own journalRecovery,
	progress *progressReporter,
) (map[string][]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*"+journalSuffix))
	if err != nil {
		return nil, err
	}
	invalidated := make(map[string][]string)
	for _, path := range paths {
		if path != own.ownJournal && journalRunIsLive(path, own.staleAfter) {
			// Another index is uploading these right now, not orphaning them.
			continue
		}
		statePath := strings.TrimSuffix(path, journalSuffix)
		if path == own.ownJournal && own.statePath != "" {
			statePath = own.statePath
		}
		dropped, err := recoverUploadJournal(ctx, client, path, statePath, progress)
		if err != nil {
			if own.ownJournal != "" && path != own.ownJournal {
				// Another index's leftovers, which a later run can retry.
				// Failing costs this run everything.
				progress.recoveryDeferred(path, err)
				continue
			}
			return nil, err
		}
		if len(dropped) > 0 {
			invalidated[path] = dropped
		}
	}
	return invalidated, nil
}

// journalRunIsLive reports whether this journal's index is locked by a live run.
func journalRunIsLive(journalPath string, staleAfter time.Duration) bool {
	active, err := indexLockIsActive(strings.TrimSuffix(journalPath, journalSuffix)+".lock", staleAfter)
	return err == nil && active
}

func recoverUploadJournal(
	ctx context.Context,
	client documentDeleter,
	path, statePath string,
	progress *progressReporter,
) ([]string, error) {
	contents, err := loadUploadJournal(path)
	if err != nil {
		return nil, err
	}
	if contents.kbID == "" || (len(contents.ids) == 0 && len(contents.stale) == 0) {
		return nil, removeIfExists(path)
	}
	state, err := loadStateFile(statePath)
	if err != nil {
		return nil, err
	}
	doomed, invalidated := reconcileJournal(contents, state)
	if err := sendDeletes(ctx, client, contents.kbID, doomed); err != nil {
		return nil, err
	}
	progress.recovered(len(doomed))
	if len(invalidated) > 0 {
		if err := writeIndexStateFile(statePath, state); err != nil {
			return nil, err
		}
	}
	return invalidated, removeIfExists(path)
}

// Returns the chunks to delete and the state entries dropped so their files get
// rebuilt on the next run.
func reconcileJournal(contents journalContents, state indexState) ([]string, []string) {
	known := make(map[string]struct{})
	for _, file := range state.Files {
		for _, id := range file.ChunkIDs {
			known[id] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(contents.ids))
	doomed := make([]string, 0, len(contents.ids))
	add := func(id string) {
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		doomed = append(doomed, id)
	}
	for _, id := range contents.ids {
		if _, ok := known[id]; !ok {
			add(id)
		}
	}
	var invalidated []string
	for _, stale := range contents.stale {
		file, ok := state.Files[stale]
		if !ok {
			continue
		}
		for _, id := range file.ChunkIDs {
			add(id)
		}
		delete(state.Files, stale)
		invalidated = append(invalidated, stale)
	}
	return doomed, invalidated
}

func loadStateFile(statePath string) (indexState, error) {
	data, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return indexState{Files: map[string]stateFile{}}, nil
	}
	if err != nil {
		return indexState{}, err
	}
	var state indexState
	if err := json.Unmarshal(data, &state); err != nil {
		return indexState{}, fmt.Errorf("read %s for journal recovery: %w", statePath, err)
	}
	if state.Files == nil {
		state.Files = map[string]stateFile{}
	}
	return state, nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
