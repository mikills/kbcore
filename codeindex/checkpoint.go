package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const runCheckpointSchema = "codeindex.run/v1"

type runPhase string

const (
	runPhaseUploaded   runPhase = "uploaded"
	runPhaseFinalizing runPhase = "finalizing"
	runPhaseFinalized  runPhase = "finalized"
)

type runCheckpoint struct {
	Schema           string     `json:"schema"`
	Phase            runPhase   `json:"phase"`
	State            indexState `json:"state"`
	DeleteIDs        []string   `json:"delete_ids,omitempty"`
	IncludeUntracked bool       `json:"include_untracked"`
	ScopeRevision    string     `json:"scope_revision,omitempty"`
	ScopeExists      bool       `json:"scope_exists"`
	FinalizeAttempt  int        `json:"finalize_attempt,omitempty"`
}

func newRunCheckpoint(plan indexPlan, includeUntracked bool, scopeRevision string, scopeExists bool) runCheckpoint {
	return runCheckpoint{
		Schema: runCheckpointSchema, Phase: runPhaseUploaded, State: plan.state,
		DeleteIDs:        append([]string(nil), plan.deleteIDs...),
		IncludeUntracked: includeUntracked,
		ScopeRevision:    scopeRevision,
		ScopeExists:      scopeExists,
	}
}

func runCheckpointPath(target indexTarget) string {
	return indexStatePath(target) + ".pending"
}

func loadRunCheckpoint(target indexTarget) (runCheckpoint, bool, error) {
	path := runCheckpointPath(target)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return runCheckpoint{}, false, nil
	}
	if err != nil {
		return runCheckpoint{}, false, err
	}
	var checkpoint runCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return runCheckpoint{}, false, fmt.Errorf("decode run checkpoint: %w", err)
	}
	if checkpoint.Schema != runCheckpointSchema {
		return runCheckpoint{}, false, fmt.Errorf("unsupported run checkpoint schema %q", checkpoint.Schema)
	}
	if !kbIDMatchesTarget(checkpoint.State.KBID, target) || checkpoint.State.ScopeID != target.ScopeID ||
		checkpoint.State.RepoID != target.RepoID || checkpoint.State.Ref != target.Ref || checkpoint.State.Root != target.Root {
		return runCheckpoint{}, false, fmt.Errorf("run checkpoint identity does not match the selected index")
	}
	if checkpoint.State.Files == nil {
		checkpoint.State.Files = map[string]stateFile{}
	}
	return checkpoint, true, nil
}

func saveRunCheckpoint(target indexTarget, checkpoint runCheckpoint) error {
	checkpoint.Schema = runCheckpointSchema
	path := runCheckpointPath(target)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".run-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
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

func removeRunCheckpoint(target indexTarget) error {
	return removeIfExists(runCheckpointPath(target))
}
