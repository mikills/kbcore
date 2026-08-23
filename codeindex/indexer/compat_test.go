package codeindex_test

import (
	"context"
	"errors"
	"testing"

	legacy "github.com/mikills/minnow/codeindex/indexer"
	shared "github.com/mikills/minnow/kb/codeindex"
	"github.com/stretchr/testify/require"
)

func TestForwarding(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		opts := legacy.NormalizeOptions(legacy.Options{})
		require.Equal(t, shared.NormalizeOptions(shared.Options{}), opts)
		require.Equal(t, shared.DefaultChunkSize, legacy.DefaultChunkSize)
	})

	t.Run("mutable globals", func(t *testing.T) {
		oldInclude := legacy.DefaultIncludePatterns
		oldLanguages := legacy.LanguageByExt
		t.Cleanup(func() {
			legacy.DefaultIncludePatterns = oldInclude
			legacy.LanguageByExt = oldLanguages
		})
		canonicalPattern := shared.DefaultIncludePatterns[0]
		legacy.DefaultIncludePatterns[0] = "legacy-only"
		require.Equal(t, canonicalPattern, shared.DefaultIncludePatterns[0])
		canonicalLanguage := shared.LanguageByExt[".go"]
		legacy.LanguageByExt[".go"] = "legacy-go"
		require.Equal(t, canonicalLanguage, shared.LanguageByExt[".go"])

		legacy.DefaultIncludePatterns = []string{"**/*.custom"}
		require.Equal(t, []string{"**/*.custom"}, legacy.NormalizeOptions(legacy.Options{}).Include)
		legacy.LanguageByExt = map[string]string{".custom": "custom"}
		require.Equal(t, "custom", legacy.DetectLanguage("file.custom"))
	})

	t.Run("sentinels", func(t *testing.T) {
		oldChanged := legacy.ErrFileChanged
		oldConfirm := legacy.ErrRequiresConfirmation
		t.Cleanup(func() {
			legacy.ErrFileChanged = oldChanged
			legacy.ErrRequiresConfirmation = oldConfirm
		})

		legacy.ErrFileChanged = errors.New("changed")
		_, _, err := legacy.BuildDocuments(
			context.Background(), t.TempDir(), "repo", legacy.ScannedFile{RelPath: "missing.go"}, legacy.Options{},
		)
		require.ErrorIs(t, err, legacy.ErrFileChanged)

		legacy.ErrRequiresConfirmation = errors.New("confirm")
		err = legacy.ValidateConfirmation(legacy.Options{RequireConfirm: true, LargeRepoFiles: 1}, 2)
		require.ErrorIs(t, err, legacy.ErrRequiresConfirmation)
	})
}
