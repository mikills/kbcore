package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.journal")
	journal, _, err := resumeUploadJournal(path, "kb", "main")
	require.NoError(t, err)
	require.NoError(t, journal.record([]string{"a", "b"}))
	require.NoError(t, journal.confirm([]string{"a"}))
	require.NoError(t, journal.recordSession("instance:token"))
	require.NoError(t, journal.markPublished())
	require.NoError(t, journal.close())

	resumed, contents, err := resumeUploadJournal(path, "kb", "main")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, contents.ids)
	require.Equal(t, []string{"a"}, contents.confirmed)
	require.Empty(t, contents.sessionID)
	require.True(t, contents.published)
	require.Equal(t, []string{"a"}, contents.publishedConfirmed)
	require.NoError(t, resumed.remove())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestJournalIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.journal")
	journal, _, err := resumeUploadJournal(path, "kb", "main")
	require.NoError(t, err)
	require.NoError(t, journal.close())

	_, _, err = resumeUploadJournal(path, "kb", "feature")
	require.Error(t, err)
	_, _, err = resumeUploadJournal(path, "other", "main")
	require.Error(t, err)
}
