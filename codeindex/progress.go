package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const progressInterval = 2 * time.Second
const phaseProgressInterval = 10 * time.Second

// Progress goes to stderr so it never mixes with the JSON result on stdout.
type progressReporter struct {
	out        io.Writer
	interval   time.Duration
	now        func() time.Time
	started    time.Time
	last       time.Time
	phaseLast  time.Time
	totalFiles int
	files      int
	chunks     int
	sent       int
}

func newProgressReporter(quiet bool) *progressReporter {
	if quiet {
		return nil
	}
	now := time.Now
	started := now()
	return &progressReporter{out: os.Stderr, interval: progressInterval, now: now, started: started, last: started}
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

func (p *progressReporter) phase(name string) {
	if p == nil {
		return
	}
	p.logf("%s, elapsed %s", name, p.now().Sub(p.started).Round(time.Second))
	p.phaseLast = p.now()
}

func (p *progressReporter) phaseHeartbeat(name string) {
	if p == nil {
		return
	}
	now := p.now()
	if now.Sub(p.phaseLast) < phaseProgressInterval {
		return
	}
	p.phaseLast = now
	p.logf("%s, elapsed %s", name, now.Sub(p.started).Round(time.Second))
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
	if p == nil {
		return
	}
	now := p.now()
	if now.Sub(p.last) < p.interval {
		return
	}
	p.last = now
	p.logf(
		"chunked %d/%d files, %d chunks, %d uploaded, elapsed %s",
		p.files, p.totalFiles, p.chunks, p.sent, now.Sub(p.started).Round(time.Second),
	)
}

func (p *progressReporter) done(result indexResult) {
	if p == nil {
		return
	}
	p.logf(
		"indexed %d files (%d unchanged, %d deleted), %d chunks added, %d scheduled for cleanup, kb %s, elapsed %s",
		result.IndexedFiles, result.UnchangedFiles, result.DeletedFiles,
		result.ChunksIndexed, result.ChunksScheduled, result.KBID,
		p.now().Sub(p.started).Round(time.Second),
	)
	if result.ChangedDuringRun > 0 {
		p.logf("%d files changed while indexing, left for the next run", result.ChangedDuringRun)
	}
}
