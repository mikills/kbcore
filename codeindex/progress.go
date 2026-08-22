package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const progressInterval = 2 * time.Second

// Progress goes to stderr so it never mixes with the JSON result on stdout.
type progressReporter struct {
	out        io.Writer
	interval   time.Duration
	last       time.Time
	totalFiles int
	files      int
	chunks     int
	sent       int
}

func newProgressReporter(quiet bool) *progressReporter {
	if quiet {
		return nil
	}
	return &progressReporter{out: os.Stderr, interval: progressInterval, last: time.Now()}
}

func (p *progressReporter) logf(format string, args ...any) {
	if p == nil {
		return
	}
	fmt.Fprintf(p.out, "codeindex: "+format+"\n", args...)
}

func (p *progressReporter) scanned(files, skipped int) {
	if p == nil {
		return
	}
	p.totalFiles = files
	p.logf("scanned %d files (%d skipped)", files, skipped)
}

func (p *progressReporter) recovered(chunks int) {
	if p == nil || chunks == 0 {
		return
	}
	p.logf("removed %d chunks left behind by an interrupted run", chunks)
}

func (p *progressReporter) waitingForSession(d time.Duration) {
	if p == nil {
		return
	}
	p.logf("another run holds this knowledge base, waiting %s for it to lapse", d.Round(time.Second))
}

func (p *progressReporter) recoveryDeferred(journalPath string, err error) {
	if p == nil {
		return
	}
	p.logf("left %s for a later run: %v", filepath.Base(journalPath), err)
}

func (p *progressReporter) fileChunked(chunks int) {
	if p == nil {
		return
	}
	p.files++
	p.chunks += chunks
	p.tick()
}

func (p *progressReporter) chunksSent(chunks int) {
	if p == nil {
		return
	}
	p.sent += chunks
	p.tick()
}

func (p *progressReporter) tick() {
	if p == nil || time.Since(p.last) < p.interval {
		return
	}
	p.last = time.Now()
	p.logf("chunked %d/%d files, %d chunks, %d uploaded", p.files, p.totalFiles, p.chunks, p.sent)
}

func (p *progressReporter) done(result indexResult) {
	if p == nil {
		return
	}
	p.logf(
		"indexed %d files (%d unchanged, %d deleted), %d chunks added, %d removed, kb %s",
		result.IndexedFiles, result.UnchangedFiles, result.DeletedFiles,
		result.ChunksIndexed, result.ChunksDeleted, result.KBID,
	)
	if result.ChangedDuringRun > 0 {
		p.logf("%d files changed while indexing, left for the next run", result.ChangedDuringRun)
	}
}
