package codeindex

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const localStateExcludePattern = "/.minnow/"

// EnsureLocalStateIgnored keeps codeindex's generated state out of normal Git
// staging without modifying the repository's shared .gitignore file.
func EnsureLocalStateIgnored(root string) error {
	root, err := ResolveRequestedRoot(root)
	if err != nil {
		return err
	}
	if err := rejectTrackedLocalState(root); err != nil {
		return err
	}
	excludePath, err := gitExcludePath(root)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(excludePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if localStateAlreadyIgnored(data) {
		return nil
	}
	return appendLocalStateExclude(excludePath, data)
}

func rejectTrackedLocalState(root string) error {
	tracked, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", ".minnow").Output()
	if err != nil {
		return fmt.Errorf("check tracked codeindex state: %w", err)
	}
	if len(tracked) != 0 {
		return fmt.Errorf(".minnow contains tracked files; remove them from Git with `git rm -r --cached .minnow` before indexing")
	}
	return nil
}

func gitExcludePath(root string) (string, error) {
	out, err := exec.Command("git", "-C", root, "rev-parse", "--git-path", "info/exclude").Output()
	if err != nil {
		return "", fmt.Errorf("resolve Git exclude path: %w", err)
	}
	excludePath := strings.TrimSpace(string(out))
	if excludePath == "" {
		return "", fmt.Errorf("git exclude path is empty")
	}
	if !filepath.IsAbs(excludePath) {
		excludePath = filepath.Join(root, excludePath)
	}
	return excludePath, nil
}

func localStateAlreadyIgnored(data []byte) bool {
	for line := range strings.SplitSeq(string(data), "\n") {
		switch strings.TrimSpace(line) {
		case localStateExcludePattern, ".minnow/":
			return true
		}
	}
	return false
}

func appendLocalStateExclude(excludePath string, existing []byte) error {
	if err := os.MkdirAll(filepath.Dir(excludePath), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(excludePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	prefix := ""
	if len(existing) > 0 && existing[len(existing)-1] != '\n' {
		prefix = "\n"
	}
	if _, err := file.WriteString(prefix + localStateExcludePattern + "\n"); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
