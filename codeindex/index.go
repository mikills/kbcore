package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	minnowcode "github.com/mikills/minnow/kb/codeindex"
)

type indexResult struct {
	KBID             string `json:"kb_id"`
	ScopeID          string `json:"scope_id"`
	IndexKey         string `json:"index_key"`
	Description      string `json:"description"`
	Ref              string `json:"ref,omitempty"`
	Root             string `json:"root"`
	ScannedFiles     int    `json:"scanned_files"`
	SkippedFiles     int    `json:"skipped_files"`
	IndexedFiles     int    `json:"indexed_files"`
	DeletedFiles     int    `json:"deleted_files"`
	UnchangedFiles   int    `json:"unchanged_files"`
	ChangedDuringRun int    `json:"changed_during_run,omitempty"`
	ChunksIndexed    int    `json:"chunks_indexed"`
	ChunksReused     int    `json:"chunks_reused"`
	ChunksDeleted    int    `json:"chunks_deleted"`
	ChunksScheduled  int    `json:"chunks_scheduled_for_gc"`
	StatePath        string `json:"state_path"`
}

type removeResult struct {
	KBID      string `json:"kb_id"`
	ScopeID   string `json:"scope_id"`
	IndexKey  string `json:"index_key"`
	Scheduled int    `json:"chunks_scheduled_for_gc"`
}

func removeIndex(ctx context.Context, cfg Config, cli indexCLIOptions) (removeResult, error) {
	target, err := resolveTarget(cli)
	if err != nil {
		return removeResult{}, err
	}
	operationTTL, err := cfg.operationTimeout()
	if err != nil {
		return removeResult{}, err
	}
	release, err := acquireRefreshLocks(target, operationTTL+time.Minute)
	if err != nil {
		return removeResult{}, err
	}
	defer release()
	state, path, exists, err := loadIndexState(target)
	if err != nil {
		return removeResult{}, err
	}
	if state.Legacy {
		return removeResult{}, fmt.Errorf("refresh index %s before removing its legacy state", target.IndexKey)
	}
	journalPath := uploadJournalPath(target)
	journal, err := loadUploadJournal(journalPath)
	if err != nil {
		return removeResult{}, err
	}
	switch {
	case exists:
		target.KBID = state.KBID
		target.ScopeID = state.ScopeID
	case journal.kbID != "" && kbIDMatchesTarget(journal.kbID, target):
		target.KBID = journal.kbID
		target.ScopeID = firstNonEmpty(journal.scopeID, target.ScopeID)
	default:
		return removeResult{}, fmt.Errorf("index %s has no local state or upload journal", target.IndexKey)
	}
	if journal.kbID != "" && (journal.kbID != target.KBID ||
		(journal.scopeID != "" && journal.scopeID != target.ScopeID)) {
		return removeResult{}, fmt.Errorf("upload journal belongs to a different index")
	}
	ids := removalChunkIDs(state, exists, journal)
	client, err := newMinnowClient(cfg)
	if err != nil {
		return removeResult{}, err
	}
	if err := client.check(ctx); err != nil {
		return removeResult{}, err
	}
	remoteIDs, revision, scopeExists, err := client.getScope(ctx, target.KBID, target.ScopeID)
	if err != nil {
		return removeResult{}, err
	}
	ids = mergeChunkIDs(ids, remoteIDs)
	recovery, _, err := resumeUploadJournal(journalPath, target.KBID, target.ScopeID)
	if err != nil {
		return removeResult{}, err
	}
	if err := recovery.record(ids); err != nil {
		recovery.close()
		return removeResult{}, err
	}
	if err := recovery.close(); err != nil {
		return removeResult{}, err
	}
	if scopeExists {
		err = client.deleteScope(ctx, target.KBID, target.ScopeID, revision)
	}
	if err != nil && !isHTTPStatus(err, 404) {
		return removeResult{}, err
	}
	deleted, err := client.scheduleGC(ctx, target.KBID, ids)
	if err != nil {
		return removeResult{}, err
	}
	registry, err := loadCodebaseRegistry(target.StateRoot)
	if err != nil {
		return removeResult{}, err
	}
	delete(registry.Indexes, target.IndexKey)
	if err := saveCodebaseRegistry(target.StateRoot, registry); err != nil {
		return removeResult{}, err
	}
	if err := removeIfExists(journalPath); err != nil {
		return removeResult{}, err
	}
	if err := removeSession(target); err != nil {
		return removeResult{}, err
	}
	if err := removeIfExists(path); err != nil {
		return removeResult{}, err
	}
	return removeResult{
		KBID: target.KBID, ScopeID: target.ScopeID, IndexKey: target.IndexKey, Scheduled: len(deleted),
	}, nil
}

func removalChunkIDs(state indexState, stateExists bool, journal journalContents) []string {
	groups := make([][]string, 0, 3)
	if stateExists {
		groups = append(groups, stateChunkIDs(state))
	}
	groups = append(groups, journal.ids, journal.confirmed)
	return mergeChunkIDs(groups...)
}

func mergeChunkIDs(groups ...[]string) []string {
	set := make(map[string]struct{})
	for _, ids := range groups {
		for _, id := range ids {
			set[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func refreshIndex(ctx context.Context, cfg Config, cli indexCLIOptions) (indexResult, error) {
	target, err := resolveTarget(cli)
	if err != nil {
		return indexResult{}, err
	}
	operationTTL, err := cfg.operationTimeout()
	if err != nil {
		return indexResult{}, err
	}
	staleAfter := operationTTL + time.Minute
	target, previous, releaseLock, err := prepareRefreshTarget(target, staleAfter)
	if err != nil {
		return indexResult{}, err
	}
	defer releaseLock()
	opts, policy, err := indexOptions(cfg, cli, target)
	if err != nil {
		return indexResult{}, err
	}
	if err := policy.Check(ctx); err != nil {
		return indexResult{}, err
	}
	progress := newProgressReporter(cli.quiet)
	if recovered, ok, err := resumeRunCheckpoint(ctx, cfg, target, opts, progress); err != nil {
		return indexResult{}, err
	} else if ok {
		previous = recovered
	}
	files, skipped, err := minnowcode.Scan(ctx, target.Root, opts, minnowcode.DefaultExcludePatterns)
	if err != nil {
		return indexResult{}, err
	}
	progress.scanned(len(files), skipped)
	if err := minnowcode.ValidateConfirmation(opts, len(files)); err != nil {
		return indexResult{}, err
	}
	// Not when the id is minted, since a run that stops at the prompt has
	// uploaded nothing to pin.
	if target.MintedKBID {
		saveReservedKBID(target)
	}
	client, journal, confirmed, err := startUpload(ctx, cfg, target, uploadJournalPath(target), progress)
	if err != nil {
		return indexResult{}, err
	}
	defer journal.close()
	journalContents, err := loadUploadJournal(uploadJournalPath(target))
	if err != nil {
		return indexResult{}, err
	}
	sink := &documentSink{
		client: client, kbID: target.KBID, policy: policy, journal: journal, progress: progress,
		confirmed: confirmed,
	}
	emit := func(ctx context.Context, docs []minnowcode.Document) error {
		progress.fileChunked(len(docs))
		return sink.emit(ctx, docs)
	}
	pipeline := pipelineFingerprint(opts)
	plan, err := buildIndexPlan(ctx, target, opts, pipeline, previous, files, skipped, emit, planRecovery{
		files: confirmedJournalFiles(journalContents, confirmed, pipeline),
		record: func(path string, state stateFile) error {
			return journal.recordFile(path, pipeline, state)
		},
	})
	if err != nil {
		return indexResult{}, err
	}
	if err := sink.close(ctx); err != nil {
		return indexResult{}, err
	}
	plan.result.ChunksIndexed = sink.uploaded
	plan.result.ChunksReused += sink.reused
	checkpoint := newRunCheckpoint(
		plan, opts.IncludeUntracked, client.scopeRevision, client.scopeExists,
	)
	if err := saveRunCheckpoint(target, checkpoint); err != nil {
		return indexResult{}, fmt.Errorf("save run checkpoint: %w", err)
	}
	// State must not record success before the deferred writes are published.
	allIDs := stateChunkIDs(plan.state)
	if _, err := client.scheduleGC(ctx, target.KBID, allIDs); err != nil {
		return indexResult{}, err
	}
	checkpoint.Phase = runPhaseFinalizing
	if err := saveRunCheckpoint(target, checkpoint); err != nil {
		return indexResult{}, fmt.Errorf("save finalizing checkpoint: %w", err)
	}
	progress.phase("publishing and finalizing branch scope")
	client.onOperationPoll = func() { progress.phaseHeartbeat("publishing and finalizing branch scope") }
	client.scopeAttempt = checkpoint.FinalizeAttempt
	client.onScopeAttempt = func(attempt int) error {
		checkpoint.FinalizeAttempt = attempt
		return saveRunCheckpoint(target, checkpoint)
	}
	if err := finalizeRun(
		ctx, client, target.KBID, target.ScopeID, allIDs,
		checkpoint.ScopeRevision, checkpoint.ScopeExists,
	); err != nil {
		return indexResult{}, err
	}
	if _, err := client.scheduleGC(ctx, target.KBID, allIDs); err != nil {
		return indexResult{}, err
	}
	if err := journal.markPublished(); err != nil {
		return indexResult{}, err
	}
	checkpoint.Phase = runPhaseFinalized
	if err := saveRunCheckpoint(target, checkpoint); err != nil {
		return indexResult{}, fmt.Errorf("save finalized checkpoint: %w", err)
	}
	progress.phase("published and finalized branch scope")
	clearSession(target)
	scheduled, err := client.scheduleGC(ctx, target.KBID, plan.deleteIDs)
	if err != nil {
		return indexResult{}, err
	}
	plan.result.ChunksScheduled = len(scheduled)
	plan.state.UpdatedAt = time.Now().UTC()
	savedPath, err := saveIndexState(target, plan.state)
	if err != nil {
		return indexResult{}, err
	}
	if err := saveRegistrySelection(target, opts); err != nil {
		return indexResult{}, err
	}
	if err := removeRunCheckpoint(target); err != nil {
		return indexResult{}, err
	}
	// State now records every emitted chunk, so a leftover journal is orphan-free.
	_ = journal.remove()
	plan.result.StatePath = savedPath
	progress.done(plan.result)
	return plan.result, nil
}

func resumeRunCheckpoint(
	ctx context.Context,
	cfg Config,
	target indexTarget,
	opts minnowcode.Options,
	progress *progressReporter,
) (indexState, bool, error) {
	checkpoint, exists, err := loadRunCheckpoint(target)
	if err != nil || !exists {
		return indexState{}, false, err
	}
	progress.phase("recovering interrupted finalization")
	client, err := newMinnowClient(cfg)
	if err != nil {
		return indexState{}, false, err
	}
	if err := client.check(ctx); err != nil {
		return indexState{}, false, err
	}
	if !client.canCommitScope {
		return indexState{}, false, fmt.Errorf(
			"Minnow at %s does not support atomic session scope commits", cfg.Minnow.URL,
		)
	}
	if _, err := client.scopeMembers(ctx, target.KBID, target.ScopeID); err != nil {
		return indexState{}, false, err
	}
	allIDs := stateChunkIDs(checkpoint.State)
	client.sessionKB = target.KBID
	client.sessionID = loadSession(target)
	if client.sessionID != "" || !client.scopeMatches(allIDs) {
		client.onWait = progress.waitingForSession
		client.onOperationPoll = func() { progress.phaseHeartbeat("recovering interrupted finalization") }
		client.scopeAttempt = checkpoint.FinalizeAttempt
		client.onScopeAttempt = func(attempt int) error {
			checkpoint.FinalizeAttempt = attempt
			return saveRunCheckpoint(target, checkpoint)
		}
		if _, err := client.scheduleGC(ctx, target.KBID, allIDs); err != nil {
			return indexState{}, false, err
		}
		checkpoint.Phase = runPhaseFinalizing
		if err := saveRunCheckpoint(target, checkpoint); err != nil {
			return indexState{}, false, err
		}
		if err := finalizeRun(
			ctx, client, target.KBID, target.ScopeID, allIDs,
			checkpoint.ScopeRevision, checkpoint.ScopeExists,
		); err != nil {
			if errors.Is(err, errPublishedMissing) {
				journal, _, journalErr := resumeUploadJournal(
					uploadJournalPath(target), target.KBID, target.ScopeID,
				)
				if journalErr != nil {
					return indexState{}, false, errors.Join(err, journalErr)
				}
				journalErr = errors.Join(journal.markPublished(), journal.close())
				if journalErr != nil {
					return indexState{}, false, errors.Join(err, journalErr)
				}
				if removeErr := removeRunCheckpoint(target); removeErr != nil {
					return indexState{}, false, errors.Join(err, removeErr)
				}
				clearSession(target)
				progress.phase("re-uploading missing index data")
				return indexState{}, false, nil
			}
			return indexState{}, false, err
		}
	}
	checkpoint.Phase = runPhaseFinalized
	if err := saveRunCheckpoint(target, checkpoint); err != nil {
		return indexState{}, false, err
	}
	if _, err := client.scheduleGC(ctx, target.KBID, checkpoint.DeleteIDs); err != nil {
		return indexState{}, false, err
	}
	checkpoint.State.UpdatedAt = time.Now().UTC()
	if _, err := saveIndexState(target, checkpoint.State); err != nil {
		return indexState{}, false, err
	}
	opts.IncludeUntracked = checkpoint.IncludeUntracked
	if err := saveRegistrySelection(target, opts); err != nil {
		return indexState{}, false, err
	}
	if err := removeRunCheckpoint(target); err != nil {
		return indexState{}, false, err
	}
	if err := removeIfExists(uploadJournalPath(target)); err != nil {
		return indexState{}, false, err
	}
	clearSession(target)
	progress.phase("recovered finalized index")
	return checkpoint.State, true, nil
}

var errPublishedMissing = errors.New("published index data is missing")

func finalizeRun(
	ctx context.Context,
	client *minnowClient,
	kbID, scopeID string,
	ids []string,
	expectedRevision string,
	expectedScope bool,
) error {
	scopeOnly := client.sessionID == ""
	client.scopeRevision = expectedRevision
	client.scopeExists = expectedScope
	commitErr := client.commit(ctx, kbID, scopeID, ids)
	if commitErr == nil {
		return nil
	}
	if !isHTTPConflict(commitErr) && !(scopeOnly && isOperationFailed(commitErr)) {
		return commitErr
	}
	published, err := client.published(ctx, kbID, ids)
	if err != nil {
		return errors.Join(commitErr, err)
	}
	if !allIDsPresent(ids, published) {
		if err := client.refreshScope(ctx, kbID, scopeID); err != nil {
			return errors.Join(commitErr, err)
		}
		if client.scopeRevision != expectedRevision || client.scopeExists != expectedScope {
			return commitErr
		}
		return fmt.Errorf("%w: %v", errPublishedMissing, commitErr)
	}
	if err := client.refreshScope(ctx, kbID, scopeID); err != nil {
		return errors.Join(commitErr, err)
	}
	if client.scopeMatches(ids) {
		return nil
	}
	if client.scopeRevision != expectedRevision || client.scopeExists != expectedScope {
		return commitErr
	}
	client.sessionID = ""
	if scopeOnly && isOperationFailed(commitErr) {
		if err := client.advanceScopeAttempt(); err != nil {
			return errors.Join(commitErr, err)
		}
	}
	if err := client.commit(ctx, kbID, scopeID, ids); err != nil {
		if isOperationFailed(err) {
			if attemptErr := client.advanceScopeAttempt(); attemptErr != nil {
				return errors.Join(commitErr, err, attemptErr)
			}
		}
		return errors.Join(commitErr, err)
	}
	return nil
}

func allIDsPresent(ids []string, present map[string]struct{}) bool {
	for _, id := range ids {
		if _, ok := present[id]; !ok {
			return false
		}
	}
	return true
}

func startUpload(
	ctx context.Context,
	cfg Config,
	target indexTarget,
	journalPath string,
	progress *progressReporter,
) (*minnowClient, *uploadJournal, map[string]struct{}, error) {
	client, err := newMinnowClient(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := client.check(ctx); err != nil {
		return nil, nil, nil, fmt.Errorf("connect to Minnow at %s: %w", cfg.Minnow.URL, err)
	}
	if !client.canScope {
		return nil, nil, nil, fmt.Errorf("Minnow at %s does not support document scopes", cfg.Minnow.URL)
	}
	if !client.canCommitScope {
		return nil, nil, nil, fmt.Errorf("Minnow at %s does not support atomic session scope commits", cfg.Minnow.URL)
	}
	journal, contents, err := startUploadJournal(journalPath, target)
	if err != nil {
		return nil, nil, nil, err
	}
	confirmed := make(map[string]struct{}, len(contents.confirmed))
	if client.canDeferPublish {
		client.sessionKB = target.KBID
		if !contents.published {
			storedSession := loadSession(target)
			client.sessionID = firstNonEmpty(contents.sessionID, storedSession)
			if contents.sessionID == "" && storedSession != "" && len(target.MigrationIDs) > 0 {
				if err := client.commitSession(ctx, target.KBID); err != nil && !isHTTPConflict(err) {
					journal.close()
					return nil, nil, nil, fmt.Errorf("publish legacy ingest session: %w", err)
				}
				clearSession(target)
				client.sessionID = ""
			}
		}
		client.onSession = func(id string) error {
			if err := journal.recordSession(id); err != nil {
				return err
			}
			saveSession(target, id)
			return nil
		}
		client.onWait = progress.waitingForSession
	}
	if client.sessionID != "" {
		for _, id := range contents.pendingConfirmed {
			confirmed[id] = struct{}{}
		}
		if len(contents.publishedConfirmed) > 0 {
			published, err := client.published(ctx, target.KBID, contents.publishedConfirmed)
			if err != nil {
				journal.close()
				return nil, nil, nil, err
			}
			for id := range published {
				confirmed[id] = struct{}{}
			}
		}
	} else if len(contents.confirmed) > 0 {
		published, err := client.published(ctx, target.KBID, contents.confirmed)
		if err != nil {
			journal.close()
			return nil, nil, nil, err
		}
		confirmed = published
	}
	if len(contents.confirmed) == 0 && len(contents.ids) > 0 {
		published, err := client.published(ctx, target.KBID, contents.ids)
		if err != nil {
			journal.close()
			return nil, nil, nil, err
		}
		for id := range published {
			confirmed[id] = struct{}{}
		}
	}
	if len(target.MigrationIDs) > 0 {
		published, err := client.published(ctx, target.KBID, target.MigrationIDs)
		if err != nil {
			journal.close()
			return nil, nil, nil, err
		}
		for id := range published {
			confirmed[id] = struct{}{}
		}
	}
	members, err := client.scopeMembers(ctx, target.KBID, target.ScopeID)
	if err != nil {
		journal.close()
		return nil, nil, nil, err
	}
	for id := range members {
		confirmed[id] = struct{}{}
	}
	if contents.scopeRecorded {
		client.scopeRevision = contents.scopeRevision
		client.scopeExists = contents.scopeExists
	} else if err := journal.recordScope(client.scopeRevision, client.scopeExists); err != nil {
		journal.close()
		return nil, nil, nil, err
	}
	return client, journal, confirmed, nil
}

func prepareRefreshTarget(
	target indexTarget,
	staleAfter time.Duration,
) (indexTarget, indexState, func(), error) {
	releaseLock, err := acquireRefreshLocks(target, staleAfter)
	if err != nil {
		return indexTarget{}, indexState{}, nil, err
	}
	prepared := false
	defer func() {
		if !prepared {
			releaseLock()
		}
	}()
	if target.Git {
		if err := minnowcode.EnsureLocalStateIgnored(target.StateRoot); err != nil {
			return indexTarget{}, indexState{}, nil, err
		}
	}
	previous, _, stateExists, err := loadIndexState(target)
	if err != nil {
		return indexTarget{}, indexState{}, nil, err
	}
	target, err = assignIndexGeneration(target, previous, stateExists)
	if err != nil {
		return indexTarget{}, indexState{}, nil, err
	}
	if err := archiveForeignJournal(target); err != nil {
		return indexTarget{}, indexState{}, nil, err
	}
	if stateExists && previous.KBID != target.KBID {
		previous = emptyIndexState(target)
	}
	prepared = true
	return target, previous, releaseLock, nil
}

func archiveForeignJournal(target indexTarget) error {
	path := uploadJournalPath(target)
	contents, err := loadUploadJournal(path)
	if err != nil || contents.kbID == "" || contents.kbID == target.KBID {
		return err
	}
	archived := path + ".legacy-" + shortHash(contents.kbID)
	if err := os.Rename(path, archived); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("archive journal for %s: %w", contents.kbID, err)
	}
	return nil
}

func acquireRefreshLocks(target indexTarget, staleAfter time.Duration) (func(), error) {
	repositoryLock := filepath.Join(sharedIndexDir(target), "repository-"+target.RepoID+"-"+shortHash(reservationBase(target))+".lock")
	releaseRepository, err := acquireLockPath(repositoryLock, target.RepoID, staleAfter)
	if err != nil {
		return nil, err
	}
	releaseCurrent, err := acquireIndexLock(target, staleAfter)
	if err != nil {
		releaseRepository()
		return nil, err
	}
	if target.LegacyIndexKey == "" {
		return func() { releaseCurrent(); releaseRepository() }, nil
	}
	legacyTarget := target
	legacyTarget.IndexKey = target.LegacyIndexKey
	if indexStatePath(legacyTarget) == indexStatePath(target) {
		return func() { releaseCurrent(); releaseRepository() }, nil
	}
	releaseLegacy, err := acquireIndexLock(legacyTarget, staleAfter)
	if err != nil {
		releaseCurrent()
		releaseRepository()
		return nil, fmt.Errorf("acquire legacy index lock: %w", err)
	}
	return func() {
		releaseLegacy()
		releaseCurrent()
		releaseRepository()
	}, nil
}

type indexPlan struct {
	state      indexState
	deleteIDs  []string
	stalePaths []string
	result     indexResult
}

type planRecovery struct {
	files  map[string]stateFile
	record func(path string, state stateFile) error
}

func confirmedJournalFiles(
	contents journalContents,
	confirmed map[string]struct{},
	pipeline string,
) map[string]stateFile {
	var files map[string]stateFile
	for path, file := range contents.files {
		if file.Pipeline != pipeline || !allChunksConfirmed(file.State.ChunkIDs, confirmed) {
			continue
		}
		if files == nil {
			files = make(map[string]stateFile)
		}
		files[path] = file.State
	}
	return files
}

func allChunksConfirmed(ids []string, confirmed map[string]struct{}) bool {
	for _, id := range ids {
		if _, ok := confirmed[id]; !ok {
			return false
		}
	}
	return true
}

func buildIndexPlan(
	ctx context.Context,
	target indexTarget,
	opts minnowcode.Options,
	pipeline string,
	previous indexState,
	files []minnowcode.ScannedFile,
	skipped int,
	emit emitFunc,
	recovery planRecovery,
) (indexPlan, error) {
	plan := indexPlan{
		state: indexState{
			SourcePath:    previous.SourcePath,
			SchemaVersion: indexStateSchema, KBID: target.KBID, ScopeID: target.ScopeID, RepoID: target.RepoID,
			Ref: target.Ref, Root: target.Root, Pipeline: pipeline, Files: make(map[string]stateFile, len(files)),
		},
		result: indexResult{
			KBID: target.KBID, ScopeID: target.ScopeID, IndexKey: target.IndexKey, Description: target.Description,
			Ref: target.Ref, Root: target.Root, ScannedFiles: len(files), SkippedFiles: skipped,
		},
	}
	current := make(map[string]minnowcode.ScannedFile, len(files))
	for _, file := range files {
		current[file.RelPath] = file
	}
	for path, recovered := range recovery.files {
		file, exists := current[path]
		if !exists || recovered.Hash != file.Hash || recovered.Language != file.Language {
			continue
		}
		plan.state.Files[path] = recovered
		plan.result.UnchangedFiles++
		plan.result.ChunksReused += len(recovered.ChunkIDs)
		delete(current, path)
	}
	superseded := make(map[string]stateFile)
	for path, old := range previous.Files {
		file, exists := current[path]
		if !exists {
			plan.deleteIDs = append(plan.deleteIDs, old.ChunkIDs...)
			plan.stalePaths = append(plan.stalePaths, path)
			plan.result.DeletedFiles++
			continue
		}
		if previous.Pipeline == pipeline && old.Hash == file.Hash && old.Language == file.Language {
			plan.state.Files[path] = old
			plan.result.UnchangedFiles++
			delete(current, path)
			continue
		}
		superseded[path] = old
	}
	paths := make([]string, 0, len(current))
	for path := range current {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	newChunkIDs := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		file := current[path]
		docs, _, err := minnowcode.BuildDocuments(ctx, target.Root, target.RepoID, file, opts)
		if errors.Is(err, minnowcode.ErrFileChanged) {
			if old, indexed := superseded[path]; indexed {
				plan.state.Files[path] = old
				// Retained chunks still belong to the previous pipeline.
				if previous.Pipeline != pipeline {
					plan.state.Pipeline = previous.Pipeline
				}
				delete(superseded, path)
			}
			plan.result.ChangedDuringRun++
			continue
		}
		if err != nil {
			return indexPlan{}, err
		}
		chunkIDs := make([]string, 0, len(docs))
		for _, doc := range docs {
			chunkIDs = append(chunkIDs, doc.ID)
			newChunkIDs[doc.ID] = struct{}{}
		}
		state := stateFile{
			Hash: file.Hash, SizeBytes: file.SizeBytes, Language: file.Language, ChunkIDs: chunkIDs,
		}
		if recovery.record != nil {
			if err := recovery.record(path, state); err != nil {
				return indexPlan{}, err
			}
		}
		if err := emit(ctx, docs); err != nil {
			return indexPlan{}, err
		}
		plan.state.Files[path] = state
		plan.result.IndexedFiles++
		plan.result.ChunksIndexed += len(docs)
	}
	for path, old := range superseded {
		plan.deleteIDs = append(plan.deleteIDs, old.ChunkIDs...)
		plan.stalePaths = append(plan.stalePaths, path)
	}
	filteredDeletes := plan.deleteIDs[:0]
	for _, id := range plan.deleteIDs {
		if _, replaced := newChunkIDs[id]; !replaced {
			filteredDeletes = append(filteredDeletes, id)
		}
	}
	plan.deleteIDs = filteredDeletes
	sort.Strings(plan.deleteIDs)
	plan.result.ChunksScheduled = len(plan.deleteIDs)
	return plan, nil
}

const codeIndexPipelineVersion = "codeindex.pipeline/v2"

func pipelineFingerprint(opts minnowcode.Options) string {
	return pipelineFingerprintForVersion(opts, codeIndexPipelineVersion)
}

func pipelineFingerprintForVersion(opts minnowcode.Options, version string) string {
	data, _ := json.Marshal(struct {
		Version      string   `json:"version"`
		Include      []string `json:"include"`
		Exclude      []string `json:"exclude"`
		MaxFileBytes int64    `json:"max_file_bytes"`
		ChunkSize    int      `json:"chunk_size"`
		Overlap      int      `json:"chunk_overlap"`
	}{version, opts.Include, opts.Exclude, opts.MaxFileBytes, opts.ChunkSize, opts.ChunkOverlap})
	return shortHash(string(data))
}

func indexOptions(cfg Config, cli indexCLIOptions, target indexTarget) (minnowcode.Options, minnowcode.ResourcePolicy, error) {
	requireConfirm := cfg.CodeIndex.RequireConfirm == nil || *cfg.CodeIndex.RequireConfirm
	throttle, err := time.ParseDuration(cfg.CodeIndex.Throttle)
	if err != nil || throttle < 0 {
		return minnowcode.Options{}, minnowcode.ResourcePolicy{}, fmt.Errorf("code_index.throttle must be a non-negative duration")
	}
	opts := minnowcode.Options{
		KBID: target.KBID, IndexKey: target.IndexKey, Description: target.Description, Root: target.Root,
		Include: append([]string(nil), cfg.CodeIndex.Include...), Exclude: append([]string(nil), cfg.CodeIndex.Exclude...),
		MaxFileBytes: cfg.CodeIndex.MaxFileBytes, ChunkSize: cfg.CodeIndex.ChunkSize,
		ChunkOverlap: cfg.CodeIndex.ChunkOverlap, IncludeUntracked: cfg.CodeIndex.IncludeUntracked || cli.includeUntracked,
		EmbedBatchSize: cfg.CodeIndex.RequestBatchSize, MaxBatchBytes: cfg.CodeIndex.MaxBatchBytes,
		Throttle: throttle, MaxHeapBytes: cfg.CodeIndex.MaxHeapBytes, MaxRSSBytes: cfg.CodeIndex.MaxRSSBytes,
		LargeRepoFiles: cfg.CodeIndex.LargeRepoFiles, RequireConfirm: requireConfirm,
		ConfirmedLarge: cli.yes || cli.force,
	}
	applyIndexResourceOverrides(&opts, cli)
	if cli.lowResource {
		applyLowResourceDefaults(&opts, cli)
	}
	opts = minnowcode.NormalizeOptions(opts)
	return opts, minnowcode.ResourcePolicyFromOptions(opts), nil
}

func applyIndexResourceOverrides(opts *minnowcode.Options, cli indexCLIOptions) {
	if cli.requestBatchSize > 0 {
		opts.EmbedBatchSize = cli.requestBatchSize
	}
	if cli.maxBatchBytes > 0 {
		opts.MaxBatchBytes = cli.maxBatchBytes
	}
	if cli.maxHeapBytes > 0 {
		opts.MaxHeapBytes = cli.maxHeapBytes
	}
	if cli.maxRSSBytes > 0 {
		opts.MaxRSSBytes = cli.maxRSSBytes
	}
	if cli.largeRepoFiles > 0 {
		opts.LargeRepoFiles = cli.largeRepoFiles
	}
	if cli.throttle > 0 {
		opts.Throttle = cli.throttle
	}
}

func applyLowResourceDefaults(opts *minnowcode.Options, cli indexCLIOptions) {
	if cli.requestBatchSize == 0 {
		opts.EmbedBatchSize = 16
	}
	if cli.maxBatchBytes == 0 {
		opts.MaxBatchBytes = 128 * 1024
	}
	if cli.throttle == 0 {
		opts.Throttle = 250 * time.Millisecond
	}
}

type emitFunc func(context.Context, []minnowcode.Document) error

type documentIngester interface {
	ingest(ctx context.Context, kbID string, docs []minnowcode.Document) error
}

type documentSink struct {
	client    documentIngester
	kbID      string
	policy    minnowcode.ResourcePolicy
	journal   uploadRecorder
	progress  *progressReporter
	pending   []minnowcode.Document
	lengths   []int
	sent      bool
	confirmed map[string]struct{}
	uploaded  int
	reused    int
}

func (s *documentSink) emit(ctx context.Context, docs []minnowcode.Document) error {
	for _, doc := range docs {
		if _, ok := s.confirmed[doc.ID]; !ok {
			s.pending = append(s.pending, doc)
		} else {
			s.reused++
		}
	}
	return s.drain(ctx, false)
}

func (s *documentSink) close(ctx context.Context) error {
	return s.drain(ctx, true)
}

func (s *documentSink) drain(ctx context.Context, final bool) error {
	for len(s.pending) > 0 {
		s.lengths = s.lengths[:0]
		for _, doc := range s.pending {
			s.lengths = append(s.lengths, len(doc.Text))
		}
		end := s.policy.BatchEndByTextBytes(s.lengths)
		if end <= 0 {
			end = 1
		}
		// More documents may still arrive to fill this batch.
		if !final && end == len(s.pending) {
			return nil
		}
		if err := s.policy.Check(ctx); err != nil {
			return err
		}
		if s.sent {
			if err := s.policy.ThrottleBatch(ctx); err != nil {
				return err
			}
		}
		ids := make([]string, 0, end)
		if s.journal != nil {
			for _, doc := range s.pending[:end] {
				ids = append(ids, doc.ID)
			}
			if err := s.journal.record(ids); err != nil {
				return err
			}
		}
		if err := s.client.ingest(ctx, s.kbID, s.pending[:end]); err != nil {
			return err
		}
		if confirmer, ok := s.journal.(uploadConfirmer); ok {
			if err := confirmer.confirm(ids); err != nil {
				return err
			}
		}
		s.progress.chunksSent(end)
		s.uploaded += end
		s.sent = true
		kept := copy(s.pending, s.pending[end:])
		clear(s.pending[kept:]) // drop sent chunk text so it can be collected
		s.pending = s.pending[:kept]
	}
	return nil
}

func stateChunkIDs(state indexState) []string {
	ids := make([]string, 0)
	for _, file := range state.Files {
		ids = append(ids, file.ChunkIDs...)
	}
	sort.Strings(ids)
	return ids
}

func saveRegistrySelection(target indexTarget, opts minnowcode.Options) error {
	registry, err := loadCodebaseRegistry(target.StateRoot)
	if err != nil {
		return err
	}
	registry.Indexes[target.IndexKey] = codebaseRegistryEntry{
		KBID: target.KBID, ScopeID: target.ScopeID, Root: minnowcode.RelativeRoot(target.StateRoot, target.Root),
		Description: target.Description, IncludeUntracked: opts.IncludeUntracked,
	}
	if target.LegacyIndexKey != "" && target.LegacyIndexKey != target.IndexKey {
		delete(registry.Indexes, target.LegacyIndexKey)
	}
	if err := saveCodebaseRegistry(target.StateRoot, registry); err != nil {
		return err
	}
	saveRepositoryRoot(target)
	return nil
}

// sessionPath holds the handle the server issued, so an interrupted run
// resumes its own session instead of waiting out the lease.
func sessionPath(target indexTarget) string {
	return filepath.Join(sharedIndexDir(target), sessionFileName(target.KBID))
}

func sessionFileName(kbID string) string {
	sum := sha256.Sum256([]byte(kbID))
	name := ".session-" + hex.EncodeToString(sum[:8])
	return name
}

func loadSession(target indexTarget) string {
	paths := []string{sessionPath(target)}
	if target.MigrationDir != "" {
		paths = append(paths, filepath.Join(target.MigrationDir, sessionFileName(target.KBID)))
	}
	paths = append(paths, filepath.Join(filepath.Dir(indexStatePath(target)), sessionFileName(target.KBID)))
	for _, path := range paths {
		if data, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(data))
		}
	}
	return ""
}

// saveSession is best effort, since losing the handle only costs the wait.
func saveSession(target indexTarget, id string) {
	path := sessionPath(target)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(path, []byte(id), 0o600)
}

func clearSession(target indexTarget) {
	_ = removeSession(target)
}

func removeSession(target indexTarget) error {
	paths := []string{sessionPath(target), filepath.Join(filepath.Dir(indexStatePath(target)), sessionFileName(target.KBID))}
	if target.MigrationDir != "" {
		paths = append(paths, filepath.Join(target.MigrationDir, sessionFileName(target.KBID)))
	}
	for _, path := range paths {
		if err := removeIfExists(path); err != nil {
			return err
		}
	}
	return nil
}
