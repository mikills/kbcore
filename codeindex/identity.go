package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"

	minnowcode "github.com/mikills/minnow/codeindex/indexer"
)

type indexTarget struct {
	Root        string
	StateRoot   string
	Scope       string
	RepoID      string
	Ref         string
	IndexKey    string
	KBID        string
	Description string
	Git         bool
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
		sha, shaOK := gitOutput(gitRoot, "rev-parse", "--short", "HEAD")
		if !shaOK {
			return indexTarget{}, fmt.Errorf("resolve current Git branch or revision")
		}
		ref = "detached-" + sha
	}
	repoIdentity, repoName := gitRepositoryIdentity(gitRoot)
	repoID := shortHash(repoIdentity)
	key := strings.TrimSpace(opts.indexKey)
	if key == "" {
		key = branchIndexKey(ref)
		if scope != "." {
			key += "-" + shortHash(scope)
		}
	} else {
		key = minnowcode.SanitizeKey(key) + "-" + branchIndexKey(ref)
		if scope != "." {
			key += "-" + shortHash(scope)
		}
	}
	kbID := strings.TrimSpace(opts.kbID)
	if kbID == "" {
		kbID = minnowcode.SanitizeKey("code-" + repoName + "-" + key + "-" + repoID + "-" + shortHash(scope))
	} else {
		kbID = minnowcode.SanitizeKey(kbID + "-" + branchIndexKey(ref) + "-" + shortHash(scope))
	}
	description := strings.TrimSpace(opts.description)
	if description == "" {
		description = fmt.Sprintf("Code index for %s on %s", repoName, ref)
	}
	return indexTarget{
		Root: root, StateRoot: gitRoot, Scope: scope, RepoID: repoID, Ref: ref, IndexKey: key,
		KBID: kbID, Description: description, Git: true,
	}, nil
}

func directoryTarget(root string, opts indexCLIOptions) indexTarget {
	identity := filepath.Clean(root)
	repoID := shortHash(identity)
	key := strings.TrimSpace(opts.indexKey)
	if key == "" {
		key = "directory-" + repoID
	} else {
		key = minnowcode.SanitizeKey(key)
	}
	name := minnowcode.SanitizeKey(filepath.Base(root))
	kbID := strings.TrimSpace(opts.kbID)
	if kbID == "" {
		kbID = minnowcode.SanitizeKey("code-" + name + "-" + repoID)
	}
	description := strings.TrimSpace(opts.description)
	if description == "" {
		description = "Code index for " + root
	}
	return indexTarget{
		Root: root, StateRoot: root, Scope: ".", RepoID: repoID, IndexKey: key,
		KBID: kbID, Description: description,
	}
}

func branchIndexKey(ref string) string {
	return minnowcode.SanitizeKey(ref) + "-" + shortHash(ref)
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
