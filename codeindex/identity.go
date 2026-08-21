package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	minnowcode "github.com/mikills/minnow/codeindex/indexer"
)

type indexTarget struct {
	Root           string
	StateRoot      string
	Scope          string
	RepoID         string
	Ref            string
	IndexKey       string
	LegacyIndexKey string
	KBID           string
	LegacyKBID     string
	Description    string
	Git            bool
	// MintedKBID marks a run that chose this id itself, the only case worth
	// reserving. A reservation from saved state would outlive that state.
	MintedKBID bool
}

func resolveTarget(opts indexCLIOptions) (indexTarget, error) {
	root, err := minnowcode.ResolveRequestedRoot(opts.root)
	if err != nil {
		return indexTarget{}, err
	}
	gitRoot, gitOK := gitOutput(root, "rev-parse", "--show-toplevel")
	if !gitOK {
		return directoryTarget(root, opts), nil
	}
	gitRoot = filepath.Clean(gitRoot)
	scope := minnowcode.RelativeRoot(gitRoot, root)
	ref, ok := gitOutput(gitRoot, "branch", "--show-current")
	if !ok || strings.TrimSpace(ref) == "" {
		sha, shaOK := gitOutput(gitRoot, "rev-parse", "HEAD")
		if !shaOK {
			return indexTarget{}, fmt.Errorf("resolve current Git branch or revision")
		}
		ref = "detached-" + sha
	}
	repoIdentity, repoName := gitRepositoryIdentity(gitRoot)
	repoID := shortHash(repoIdentity)
	key := strings.TrimSpace(opts.indexKey)
	legacyIndexKey := ""
	if key == "" {
		key = branchIndexKey(ref)
		if scope != "." {
			key += "-" + shortHash(scope)
		}
	} else {
		legacyIndexKey = minnowcode.SanitizeKey(key) + "-" + branchIndexKey(ref)
		key = explicitIdentityPrefix(key) + "-" + branchIndexKey(ref)
		if scope != "." {
			legacyIndexKey += "-" + shortHash(scope)
			key += "-" + shortHash(scope)
		}
	}
	kbID := strings.TrimSpace(opts.kbID)
	legacyKBID := ""
	if kbID == "" {
		kbID = minnowcode.SanitizeKey("code-" + repoName + "-" + key + "-" + repoID + "-" + shortHash(scope))
		if legacyIndexKey != "" {
			legacyKBID = minnowcode.SanitizeKey("code-" + repoName + "-" + legacyIndexKey + "-" + repoID + "-" + shortHash(scope))
		}
	} else {
		legacyKBID = minnowcode.SanitizeKey(kbID + "-" + branchIndexKey(ref) + "-" + shortHash(scope))
		kbID = minnowcode.SanitizeKey(explicitIdentityPrefix(kbID) + "-" + branchIndexKey(ref) + "-" + repoID + "-" + shortHash(scope))
	}
	description := strings.TrimSpace(opts.description)
	if description == "" {
		description = fmt.Sprintf("Code index for %s on %s", repoName, ref)
	}
	return indexTarget{
		Root: root, StateRoot: gitRoot, Scope: scope, RepoID: repoID, Ref: ref, IndexKey: key,
		LegacyIndexKey: legacyIndexKey, KBID: kbID, LegacyKBID: legacyKBID, Description: description, Git: true,
	}, nil
}

func directoryTarget(root string, opts indexCLIOptions) indexTarget {
	identity := filepath.Clean(root)
	repoID := shortHash(identity)
	key := strings.TrimSpace(opts.indexKey)
	legacyIndexKey := ""
	if key == "" {
		key = "directory-" + repoID
	} else {
		legacyIndexKey = minnowcode.SanitizeKey(key)
		key = explicitIdentityPrefix(key)
	}
	name := minnowcode.SanitizeKey(filepath.Base(root))
	kbID := strings.TrimSpace(opts.kbID)
	legacyKBID := ""
	if kbID == "" {
		kbID = minnowcode.SanitizeKey("code-" + name + "-" + repoID)
		if legacyIndexKey != "" {
			legacyKBID = kbID
		}
	} else {
		legacyKBID = kbID
		kbID = minnowcode.SanitizeKey(explicitIdentityPrefix(kbID) + "-" + repoID)
	}
	description := strings.TrimSpace(opts.description)
	if description == "" {
		description = "Code index for " + root
	}
	return indexTarget{
		Root: root, StateRoot: root, Scope: ".", RepoID: repoID, IndexKey: key,
		LegacyIndexKey: legacyIndexKey, KBID: kbID, LegacyKBID: legacyKBID, Description: description,
	}
}

func branchIndexKey(ref string) string {
	return minnowcode.SanitizeKey(ref) + "-" + shortHash(ref)
}

func explicitIdentityPrefix(value string) string {
	value = strings.TrimSpace(value)
	return minnowcode.SanitizeKey(value) + "-" + identityHash(value)
}

func assignIndexGeneration(target indexTarget, state indexState, stateExists bool) (indexTarget, error) {
	if stateExists {
		target.KBID = state.KBID
		return target, nil
	}
	// State is written only after the commit, so an interrupted first index
	// would otherwise abandon everything it uploaded.
	target.MintedKBID = true
	if reserved := loadReservedKBID(target); validReservedKBID(reserved, target) {
		target.KBID = reserved
		return target, nil
	}
	var generation [8]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return indexTarget{}, fmt.Errorf("create index generation: %w", err)
	}
	target.KBID = minnowcode.SanitizeKey(target.KBID + "-" + hex.EncodeToString(generation[:]))
	return target, nil
}

// generationSuffixLen is the hex width of the random generation appended to a
// knowledge base id.
const generationSuffixLen = 16

// validReservedKBID accepts only an id this target could have generated, so a
// reservation from another repository or a crash mid-write cannot capture it.
func validReservedKBID(reserved string, target indexTarget) bool {
	suffix, ok := strings.CutPrefix(reserved, target.KBID+"-")
	if !ok || len(suffix) != generationSuffixLen {
		return false
	}
	_, err := hex.DecodeString(suffix)
	return err == nil
}

func reservedKBIDPath(target indexTarget) string {
	name := minnowcode.SanitizeKey(target.IndexKey) + "-" + shortHash(target.Root) + ".kbid"
	return filepath.Join(target.StateRoot, ".minnow", "codeindex", name)
}

func loadReservedKBID(target indexTarget) string {
	data, err := os.ReadFile(reservedKBIDPath(target))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// saveReservedKBID is best effort, and renames so a crash cannot leave a
// truncated id the next run would trust.
func saveReservedKBID(target indexTarget) {
	path := reservedKBIDPath(target)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".kbid-*")
	if err != nil {
		return
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(target.KBID); err != nil {
		tmp.Close()
		return
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}
	_ = os.Rename(tmp.Name(), path)
}

// clearReservedKBID runs once state records the knowledge base, so deleting
// that file still forces a fresh index.
func clearReservedKBID(target indexTarget) {
	_ = os.Remove(reservedKBIDPath(target))
}

func gitRepositoryIdentity(root string) (string, string) {
	remote, ok := gitOutput(root, "config", "--get", "remote.origin.url")
	if !ok || strings.TrimSpace(remote) == "" {
		return root, filepath.Base(root)
	}
	return remote, repositoryName(remote, filepath.Base(root))
}

func repositoryName(remote, fallback string) string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(remote), ".git")
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Path != "" {
		if name := filepath.Base(parsed.Path); name != "." && name != "/" {
			return minnowcode.SanitizeKey(name)
		}
	}
	if colon := strings.LastIndex(trimmed, ":"); colon >= 0 {
		trimmed = trimmed[colon+1:]
	}
	if slash := strings.LastIndex(trimmed, "/"); slash >= 0 {
		trimmed = trimmed[slash+1:]
	}
	if trimmed == "" {
		trimmed = fallback
	}
	return minnowcode.SanitizeKey(trimmed)
}

func gitOutput(root string, args ...string) (string, bool) {
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).Output()
	if err != nil {
		return "", false
	}
	value := strings.TrimSpace(string(out))
	return value, value != ""
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:4])
}

func identityHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
