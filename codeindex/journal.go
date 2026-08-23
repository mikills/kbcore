package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	journalSuffix     = ".journal"
	journalKBPrefix   = "kb "
	journalScopePfx   = "scope "
	journalChunkPfx   = "i "
	journalConfirmPfx = "c "
	journalSessionPfx = "session "
	journalFilePfx    = "file "
	journalScopeBase  = "scope-base "
	journalPublished  = "published"
	journalPermission = 0o600
)

type uploadRecorder interface {
	record(ids []string) error
}

type uploadConfirmer interface{ confirm([]string) error }

type journalFile struct {
	Path     string    `json:"path"`
	Pipeline string    `json:"pipeline"`
	State    stateFile `json:"state"`
}

type journalScope struct {
	Revision string `json:"revision,omitempty"`
	Exists   bool   `json:"exists"`
}

type uploadJournal struct {
	path string
	file *os.File
}

func uploadJournalPath(target indexTarget) string {
	return indexStatePath(target) + journalSuffix
}

func resumeUploadJournal(path, kbID, scopeID string) (*uploadJournal, journalContents, error) {
	if kbID == "" {
		return nil, journalContents{}, fmt.Errorf("upload journal requires a kb id")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, journalContents{}, err
	}
	contents, err := loadUploadJournal(path)
	if err != nil {
		return nil, journalContents{}, err
	}
	if contents.kbID != "" && (contents.kbID != kbID || (contents.scopeID != "" && contents.scopeID != scopeID)) {
		return nil, journalContents{}, fmt.Errorf("upload journal belongs to a different index")
	}
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	file, err := os.OpenFile(path, flags, journalPermission)
	if err != nil {
		return nil, journalContents{}, err
	}
	journal := &uploadJournal{path: path, file: file}
	if contents.kbID == "" {
		header := journalKBPrefix + kbID + "\n" + journalScopePfx + scopeID + "\n"
		if _, err := file.WriteString(header); err != nil {
			journal.close()
			return nil, journalContents{}, err
		}
	}
	if err := file.Sync(); err != nil {
		journal.close()
		return nil, journalContents{}, err
	}
	contents.kbID = kbID
	contents.scopeID = scopeID
	return journal, contents, nil
}

func (j *uploadJournal) record(ids []string) error {
	return j.write(journalChunkPfx, ids)
}

func (j *uploadJournal) confirm(ids []string) error {
	return j.write(journalConfirmPfx, ids)
}

func (j *uploadJournal) recordSession(id string) error {
	return j.write(journalSessionPfx, []string{id})
}

func (j *uploadJournal) recordFile(path, pipeline string, state stateFile) error {
	data, err := json.Marshal(journalFile{Path: path, Pipeline: pipeline, State: state})
	if err != nil {
		return err
	}
	if _, err := j.file.WriteString(journalFilePfx + string(data) + "\n"); err != nil {
		return err
	}
	return j.file.Sync()
}

func (j *uploadJournal) recordScope(revision string, exists bool) error {
	data, err := json.Marshal(journalScope{Revision: revision, Exists: exists})
	if err != nil {
		return err
	}
	if _, err := j.file.WriteString(journalScopeBase + string(data) + "\n"); err != nil {
		return err
	}
	return j.file.Sync()
}

func (j *uploadJournal) markPublished() error {
	if _, err := j.file.WriteString(journalPublished + "\n"); err != nil {
		return err
	}
	return j.file.Sync()
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
	kbID               string
	scopeID            string
	ids                []string
	confirmed          []string
	publishedConfirmed []string
	pendingConfirmed   []string
	files              map[string]journalFile
	scopeRevision      string
	scopeExists        bool
	scopeRecorded      bool
	sessionID          string
	published          bool
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
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, journalKBPrefix):
			contents.kbID = strings.TrimSpace(strings.TrimPrefix(line, journalKBPrefix))
		case strings.HasPrefix(line, journalScopePfx):
			contents.scopeID = strings.TrimSpace(strings.TrimPrefix(line, journalScopePfx))
		case strings.HasPrefix(line, journalChunkPfx):
			if id := strings.TrimSpace(strings.TrimPrefix(line, journalChunkPfx)); id != "" {
				contents.ids = append(contents.ids, id)
				contents.published = false
			}
		case strings.HasPrefix(line, journalConfirmPfx):
			if id := strings.TrimSpace(strings.TrimPrefix(line, journalConfirmPfx)); id != "" {
				contents.confirmed = append(contents.confirmed, id)
				contents.pendingConfirmed = append(contents.pendingConfirmed, id)
			}
		case strings.HasPrefix(line, journalSessionPfx):
			contents.sessionID = strings.TrimSpace(strings.TrimPrefix(line, journalSessionPfx))
			contents.published = false
		case strings.HasPrefix(line, journalFilePfx):
			var file journalFile
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, journalFilePfx)), &file); err != nil {
				return journalContents{}, fmt.Errorf("decode journal file checkpoint: %w", err)
			}
			if file.Path != "" {
				if contents.files == nil {
					contents.files = make(map[string]journalFile)
				}
				contents.files[file.Path] = file
			}
		case strings.HasPrefix(line, journalScopeBase):
			var scope journalScope
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, journalScopeBase)), &scope); err != nil {
				return journalContents{}, fmt.Errorf("decode journal scope checkpoint: %w", err)
			}
			contents.scopeRevision = scope.Revision
			contents.scopeExists = scope.Exists
			contents.scopeRecorded = true
		case line == journalPublished:
			contents.published = true
			contents.sessionID = ""
			contents.scopeRevision = ""
			contents.scopeExists = false
			contents.scopeRecorded = false
			contents.publishedConfirmed = append(contents.publishedConfirmed[:0], contents.confirmed...)
			contents.pendingConfirmed = nil
		}
	}
	return contents, scanner.Err()
}

func startUploadJournal(path string, target indexTarget) (*uploadJournal, journalContents, error) {
	journal, contents, err := resumeUploadJournal(path, target.KBID, target.ScopeID)
	if err != nil {
		return nil, journalContents{}, err
	}
	return journal, contents, nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
