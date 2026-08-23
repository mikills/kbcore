package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProgress(t *testing.T) {
	started := time.Date(2026, time.August, 23, 10, 0, 0, 0, time.UTC)
	now := started
	var output bytes.Buffer
	reporter := &progressReporter{
		out: &output, interval: 2 * time.Second, now: func() time.Time { return now }, started: started, last: started,
		totalFiles: 100,
	}

	reporter.fileChunked(3)
	require.Empty(t, output.String())
	now = now.Add(12*time.Minute + 3*time.Second)
	reporter.chunksSent(3)
	require.Contains(t, output.String(), "chunked 1/100 files, 3 chunks, 3 uploaded, elapsed 12m3s")

	now = now.Add(7 * time.Second)
	reporter.done(indexResult{IndexedFiles: 1, ChunksIndexed: 3, KBID: "code-repo"})
	require.Contains(t, output.String(), "kb code-repo, elapsed 12m10s")
}

func TestPhaseProgress(t *testing.T) {
	started := time.Date(2026, time.August, 23, 10, 0, 0, 0, time.UTC)
	now := started
	var output bytes.Buffer
	reporter := &progressReporter{out: &output, now: func() time.Time { return now }, started: started}

	reporter.phase("publishing and finalizing branch scope")
	now = now.Add(9 * time.Second)
	reporter.phaseHeartbeat("publishing and finalizing branch scope")
	require.Equal(t, 1, bytes.Count(output.Bytes(), []byte("publishing and finalizing")))
	now = now.Add(time.Second)
	reporter.phaseHeartbeat("publishing and finalizing branch scope")
	require.Equal(t, 2, bytes.Count(output.Bytes(), []byte("publishing and finalizing")))
}
