package scheduler

import (
	"context"
	"fmt"
)

type JobRunner func(context.Context) error

type JobSet struct {
	ShardGC          JobRunner
	EventReaper      JobRunner
	EventCleanup     JobRunner
	InboxCleanup     JobRunner
	MediaGCMark      JobRunner
	MediaGCSweep     JobRunner
	MediaUploadAbort JobRunner
	SessionReap      JobRunner
}

type JobIDs struct {
	ShardGC          string
	EventReaper      string
	EventCleanup     string
	InboxCleanup     string
	MediaGCMark      string
	MediaGCSweep     string
	MediaUploadAbort string
	SessionReap      string
}

type JobExpressions struct {
	ShardGC          string
	EventReaper      string
	EventCleanup     string
	InboxCleanup     string
	MediaGCMark      string
	MediaGCSweep     string
	MediaUploadAbort string
	SessionReap      string
}

type EnabledJobs struct {
	ShardGC          bool
	EventJobs        bool
	InboxCleanup     bool
	MediaJobs        bool
	MediaUploadAbort bool
	SessionReap      bool
}

type NamedJobRunner func(context.Context, string) error

func RegisterDefaultNamedJobs(
	s *Scheduler,
	ids JobIDs,
	exprs JobExpressions,
	enabled EnabledJobs,
	run NamedJobRunner,
) error {
	return RegisterDefaultJobs(s, ids, exprs, JobSet{
		ShardGC:          namedJob(enabled.ShardGC, ids.ShardGC, run),
		EventReaper:      namedJob(enabled.EventJobs, ids.EventReaper, run),
		EventCleanup:     namedJob(enabled.EventJobs, ids.EventCleanup, run),
		InboxCleanup:     namedJob(enabled.InboxCleanup, ids.InboxCleanup, run),
		MediaGCMark:      namedJob(enabled.MediaJobs, ids.MediaGCMark, run),
		MediaGCSweep:     namedJob(enabled.MediaJobs, ids.MediaGCSweep, run),
		MediaUploadAbort: namedJob(enabled.MediaUploadAbort, ids.MediaUploadAbort, run),
		SessionReap:      namedJob(enabled.SessionReap, ids.SessionReap, run),
	})
}

func namedJob(enabled bool, id string, run NamedJobRunner) JobRunner {
	if !enabled || run == nil {
		return nil
	}
	return func(ctx context.Context) error { return run(ctx, id) }
}

func RegisterDefaultJobs(s *Scheduler, ids JobIDs, exprs JobExpressions, jobs JobSet) error {
	if s == nil {
		return fmt.Errorf("scheduler: nil")
	}
	if jobs.ShardGC != nil {
		if err := s.Register(ids.ShardGC, exprs.ShardGC, Job(jobs.ShardGC)); err != nil {
			return err
		}
	}
	return firstJobErr(
		registerIfPresent(s, ids.EventReaper, exprs.EventReaper, jobs.EventReaper),
		registerIfPresent(s, ids.EventCleanup, exprs.EventCleanup, jobs.EventCleanup),
		registerIfPresent(s, ids.InboxCleanup, exprs.InboxCleanup, jobs.InboxCleanup),
		registerIfPresent(s, ids.MediaGCMark, exprs.MediaGCMark, jobs.MediaGCMark),
		registerIfPresent(s, ids.MediaGCSweep, exprs.MediaGCSweep, jobs.MediaGCSweep),
		registerIfPresent(s, ids.MediaUploadAbort, exprs.MediaUploadAbort, jobs.MediaUploadAbort),
		registerIfPresent(s, ids.SessionReap, exprs.SessionReap, jobs.SessionReap),
	)
}

func registerIfPresent(s *Scheduler, id string, expr string, run JobRunner) func() error {
	return func() error {
		if run == nil {
			return nil
		}
		return s.Register(id, expr, Job(run))
	}
}

func firstJobErr(steps ...func() error) error {
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}
