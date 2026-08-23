package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	minnowcode "github.com/mikills/minnow/kb/codeindex"
)

type indexTarget struct {
	Root           string
	StateRoot      string
	Scope          string
	ScopeID        string
	RepoID         string
	Ref            string
	IndexKey       string
	LegacyIndexKey string
	KBID           string
	KBBaseID       string
	LegacyKBID     string
	Description    string
	Git            bool
	// MintedKBID marks a run that chose this id itself, the only case worth
	// reserving. A reservation from saved state would outlive that state.
	MintedKBID   bool
	MigrationIDs []string
	MigrationDir string
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
	repoIdentity, repoName := gitRepository(gitRoot)
	repoID := repositoryID(repoIdentity)
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
		identity := "repository"
		if strings.TrimSpace(opts.indexKey) != "" {
			identity = explicitIdentityPrefix(opts.indexKey)
		}
		kbID = minnowcode.SanitizeKey("code-" + repoName + "-" + identity + "-" + repoID + "-" + shortHash(scope))
		legacyKBID = minnowcode.SanitizeKey("code-" + repoName + "-" + key + "-" + repoID + "-" + shortHash(scope))
		if legacyIndexKey != "" {
			legacyKBID = minnowcode.SanitizeKey("code-" + repoName + "-" + legacyIndexKey + "-" + repoID + "-" + shortHash(scope))
		}
	} else {
		legacyKBID = minnowcode.SanitizeKey(kbID + "-" + branchIndexKey(ref) + "-" + shortHash(scope))
		kbID = minnowcode.SanitizeKey(explicitIdentityPrefix(kbID) + "-" + repoID + "-" + shortHash(scope))
	}
	description := strings.TrimSpace(opts.description)
	if description == "" {
		description = fmt.Sprintf("Code index for %s on %s", repoName, ref)
	}
	return indexTarget{
		Root: root, StateRoot: gitRoot, Scope: scope, ScopeID: branchScopeID(ref, scope), RepoID: repoID, Ref: ref, IndexKey: key,
		LegacyIndexKey: legacyIndexKey, KBID: kbID, KBBaseID: kbID, LegacyKBID: legacyKBID, Description: description, Git: true,
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
		Root: root, StateRoot: root, Scope: ".", ScopeID: "directory-" + repoID, RepoID: repoID, IndexKey: key,
		LegacyIndexKey: legacyIndexKey, KBID: kbID, KBBaseID: kbID, LegacyKBID: legacyKBID, Description: description,
	}
}

func branchIndexKey(ref string) string {
	return minnowcode.SanitizeKey(ref) + "-" + shortHash(ref)
}

func branchScopeID(ref, scope string) string {
	return "codeindex-" + identityHash(ref+"\x00"+scope)
}

func explicitIdentityPrefix(value string) string {
	value = strings.TrimSpace(value)
	return minnowcode.SanitizeKey(value) + "-" + identityHash(value)
}

func assignIndexGeneration(target indexTarget, state indexState, stateExists bool) (indexTarget, error) {
	if target.KBBaseID == "" {
		target.KBBaseID = target.KBID
	}
	// State is written only after the commit, so an interrupted first index
	// would otherwise abandon everything it uploaded.
	target.MintedKBID = true
	if reserved, mapped := loadReservedMapping(target); mapped || validReservedKBID(reserved, target) ||
		(target.LegacyKBID != "" && validGeneratedKBID(reserved, target.LegacyKBID)) {
		if !stateExists || state.Legacy || state.KBID != reserved {
			if kbID, ids, dir := selectLegacyGeneration(target, reserved); kbID == reserved {
				target.MigrationIDs = ids
				target.MigrationDir = dir
			}
		}
		target.KBID = reserved
		return target, nil
	}
	if kbID, ids, dir := selectLegacyGeneration(target); kbID != "" {
		target.KBID = kbID
		target.MigrationIDs = ids
		target.MigrationDir = dir
		saveReservedKBID(target)
		return target, nil
	}
	if stateExists {
		target.KBID = state.KBID
		target.ScopeID = firstNonEmpty(state.ScopeID, target.ScopeID)
		saveReservedKBID(target)
		return target, nil
	}
	if contents, err := loadUploadJournal(uploadJournalPath(target)); err == nil &&
		contents.kbID != "" && kbIDMatchesTarget(contents.kbID, target) {
		target.KBID = contents.kbID
		saveReservedKBID(target)
		return target, nil
	}
	target.KBID = target.KBBaseID
	return target, nil
}

func selectLegacyGeneration(target indexTarget, preferred ...string) (string, []string, string) {
	type candidate struct {
		ids map[string]struct{}
		dir string
	}
	candidates := make(map[string]candidate)
	for _, scan := range legacyScanTargets(target) {
		dir := filepath.Dir(indexStatePath(scan))
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		add := func(kbID string, ids []string) {
			if !legacyRepositoryKBID(kbID, scan) {
				return
			}
			item := candidates[kbID]
			if item.ids == nil {
				item = candidate{ids: make(map[string]struct{}), dir: dir}
			}
			for _, id := range ids {
				item.ids[id] = struct{}{}
			}
			candidates[kbID] = item
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			switch {
			case strings.HasSuffix(entry.Name(), journalSuffix):
				contents, err := loadUploadJournal(path)
				if err == nil {
					add(contents.kbID, append(contents.ids, contents.confirmed...))
				}
			case strings.HasSuffix(entry.Name(), ".json"):
				data, err := os.ReadFile(path)
				if err != nil {
					continue
				}
				var state indexState
				if json.Unmarshal(data, &state) != nil || state.RepoID != target.RepoID {
					continue
				}
				ids := make([]string, 0)
				for _, file := range state.Files {
					ids = append(ids, file.ChunkIDs...)
				}
				add(state.KBID, ids)
			}
		}
	}
	best := ""
	if len(preferred) > 0 {
		if _, ok := candidates[preferred[0]]; ok {
			best = preferred[0]
		}
	}
	for kbID, item := range candidates {
		if len(preferred) > 0 && best == preferred[0] {
			break
		}
		if best == "" || len(item.ids) > len(candidates[best].ids) ||
			(len(item.ids) == len(candidates[best].ids) && kbID < best) {
			best = kbID
		}
	}
	ids := make([]string, 0, len(candidates[best].ids))
	for id := range candidates[best].ids {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return best, ids, candidates[best].dir
}

func legacyScanTargets(target indexTarget) []indexTarget {
	if !target.Git || !strings.Contains(target.KBBaseID, "-repository-"+target.RepoID+"-") {
		return []indexTarget{target}
	}
	out, ok := gitOutput(target.StateRoot, "worktree", "list", "--porcelain")
	if !ok {
		return []indexTarget{target}
	}
	refsOut, _ := gitOutput(target.StateRoot, "for-each-ref", "--format=%(refname:short)", "refs/heads")
	refs := strings.Fields(refsOut)
	seenRef := make(map[string]struct{}, len(refs)+1)
	uniqueRefs := make([]string, 0, len(refs)+1)
	for _, ref := range append(refs, target.Ref) {
		if ref == "" {
			continue
		}
		if _, seen := seenRef[ref]; !seen {
			seenRef[ref] = struct{}{}
			uniqueRefs = append(uniqueRefs, ref)
		}
	}
	refs = uniqueRefs
	targets := make([]indexTarget, 0)
	for _, line := range strings.Split(out, "\n") {
		root, found := strings.CutPrefix(line, "worktree ")
		if !found {
			continue
		}
		root = filepath.Join(filepath.Clean(root), filepath.FromSlash(target.Scope))
		scan, err := resolveTarget(indexCLIOptions{root: root})
		if err != nil || scan.RepoID != target.RepoID || scan.Scope != target.Scope {
			continue
		}
		for _, ref := range refs {
			targets = append(targets, legacyTargetForRef(scan, ref))
		}
	}
	if len(targets) == 0 {
		return []indexTarget{target}
	}
	return targets
}

func legacyTargetForRef(target indexTarget, ref string) indexTarget {
	_, repoName := gitRepository(target.StateRoot)
	key := branchIndexKey(ref)
	if target.Scope != "." {
		key += "-" + shortHash(target.Scope)
	}
	target.Ref = ref
	target.IndexKey = key
	target.LegacyIndexKey = ""
	target.KBID = target.KBBaseID
	target.LegacyKBID = minnowcode.SanitizeKey(
		"code-" + repoName + "-" + key + "-" + target.RepoID + "-" + shortHash(target.Scope),
	)
	return target
}

func legacyRepositoryKBID(kbID string, target indexTarget) bool {
	return kbIDMatchesTarget(kbID, target)
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
	name := "repository-" + target.RepoID + "-" + shortHash(target.Scope) + "-" + shortHash(reservationBase(target)) + ".kbid"
	return filepath.Join(sharedIndexDir(target), name)
}

func legacyReservedKBIDPath(target indexTarget) string {
	name := "repository-" + target.RepoID + "-" + shortHash(target.Scope) + ".kbid"
	return filepath.Join(sharedIndexDir(target), name)
}

func sharedIndexDir(target indexTarget) string {
	if target.Git {
		if common, ok := gitOutput(target.StateRoot, "rev-parse", "--git-common-dir"); ok {
			if !filepath.IsAbs(common) {
				common = filepath.Join(target.StateRoot, common)
			}
			return filepath.Join(filepath.Clean(common), "minnow", "codeindex")
		}
	}
	return filepath.Join(target.StateRoot, ".minnow", "codeindex")
}

func loadReservedKBID(target indexTarget) string {
	kbID, _ := loadReservedMapping(target)
	return kbID
}

func loadReservedMapping(target indexTarget) (string, bool) {
	data, err := os.ReadFile(reservedKBIDPath(target))
	if errors.Is(err, os.ErrNotExist) {
		data, err = os.ReadFile(legacyReservedKBIDPath(target))
	}
	if err != nil {
		return "", false
	}
	var reservation struct {
		Schema string `json:"schema"`
		Base   string `json:"base_kb_id"`
		KBID   string `json:"kb_id"`
	}
	if json.Unmarshal(data, &reservation) == nil && reservation.Schema == "codeindex.kbid/v2" {
		if reservation.Base == reservationBase(target) {
			return reservation.KBID, reservation.KBID != ""
		}
		return "", false
	}
	return strings.TrimSpace(string(data)), false
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
	data, err := json.Marshal(struct {
		Schema string `json:"schema"`
		Base   string `json:"base_kb_id"`
		KBID   string `json:"kb_id"`
	}{Schema: "codeindex.kbid/v2", Base: reservationBase(target), KBID: target.KBID})
	if err != nil {
		tmp.Close()
		return
	}
	if _, err := tmp.Write(data); err != nil {
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

func reservationBase(target indexTarget) string {
	if target.KBBaseID != "" {
		return target.KBBaseID
	}
	return target.KBID
}

func gitRepository(root string) (string, string) {
	remote, ok := gitOutput(root, "config", "--get", "remote.origin.url")
	if ok && strings.TrimSpace(remote) != "" {
		return root, repositoryName(remote, filepath.Base(root))
	}
	primary := primaryWorktreeRoot(root)
	return primary, filepath.Base(primary)
}

func primaryWorktreeRoot(root string) string {
	if saved := loadRepositoryRoot(root); saved != "" {
		return saved
	}
	if top, ok := gitOutput(root, "rev-parse", "--show-toplevel"); ok && isPrimaryWorktree(root) {
		if resolved, err := filepath.EvalSymlinks(top); err == nil {
			top = resolved
		}
		top = filepath.Clean(top)
		return top
	}
	if output, ok := gitOutput(root, "worktree", "list", "--porcelain"); ok {
		for _, line := range strings.Split(output, "\n") {
			if candidate, found := strings.CutPrefix(line, "worktree "); found {
				if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
					candidate = resolved
				}
				return filepath.Clean(candidate)
			}
		}
	}
	return root
}

func loadRepositoryRoot(root string) string {
	data, err := os.ReadFile(repositoryRootPath(root))
	if err != nil {
		return ""
	}
	saved := strings.TrimSpace(string(data))
	resolved, err := filepath.EvalSymlinks(saved)
	if err != nil {
		return ""
	}
	return filepath.Clean(resolved)
}

func repositoryRootPath(root string) string {
	common, ok := gitOutput(root, "rev-parse", "--git-common-dir")
	if !ok {
		return ""
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(root, common)
	}
	return filepath.Join(filepath.Clean(common), "minnow", "codeindex", "repository-root")
}

func saveRepositoryRoot(target indexTarget) {
	path := repositoryRootPath(target.StateRoot)
	if path == "" || os.MkdirAll(filepath.Dir(path), 0o755) != nil {
		return
	}
	_ = os.WriteFile(path, []byte(primaryWorktreeRoot(target.StateRoot)+"\n"), 0o600)
}

func isPrimaryWorktree(root string) bool {
	gitDir, gitOK := gitOutput(root, "rev-parse", "--absolute-git-dir")
	common, commonOK := gitOutput(root, "rev-parse", "--git-common-dir")
	if !gitOK || !commonOK {
		return false
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(root, common)
	}
	return filepath.Clean(gitDir) == filepath.Clean(common)
}

func repositoryID(identity string) string {
	if remote, ok := gitOutput(identity, "config", "--get", "remote.origin.url"); ok {
		return shortHash(remote)
	}
	if resolved, err := filepath.EvalSymlinks(identity); err == nil {
		identity = resolved
	}
	return shortHash(filepath.Clean(identity))
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
