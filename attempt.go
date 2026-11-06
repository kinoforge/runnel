package runnel

import (
	"context"
	"errors"
)

var (
	// ErrShutdown is the cause of a job's context when the worker stops before the
	// job finishes. The job is released untouched.
	ErrShutdown = errors.New("runnel: worker shutting down")
	// ErrTimeout is the cause of a job's context when the attempt exceeds the job's
	// timeout.
	ErrTimeout = errors.New("runnel: attempt timed out")
	// ErrLost is the cause of a job's context when the worker lost the lease, for
	// example because the job was cancelled.
	ErrLost = errors.New("runnel: lease lost")
	// ErrDeadline is the cause of a job's context when the job's deadline passes.
	ErrDeadline = errors.New("runnel: deadline passed")
)

// Attempt returns the context an attempt of j runs in: with the job's timeout
// and deadline, and carrying the job for JobFrom and Dependencies.
func Attempt(ctx context.Context, j *Job, src Source) (context.Context, context.CancelFunc) {
	stopTimeout, stopDeadline := func() {}, func() {}
	if j.Timeout > 0 {
		ctx, stopTimeout = context.WithTimeoutCause(ctx, j.Timeout.D(), ErrTimeout)
	}
	if !j.Deadline.IsZero() {
		ctx, stopDeadline = context.WithDeadlineCause(ctx, j.Deadline, ErrDeadline)
	}
	return withJob(ctx, j, src), func() { stopDeadline(); stopTimeout() }
}

// Classify turns the result of an attempt into an outcome, telling shutdowns and
// timeouts apart from errors.
func Classify(ctx context.Context, j *Job, err error) Outcome {
	cause := context.Cause(ctx)
	switch {
	case err != nil && errors.Is(cause, ErrShutdown):
		return Outcome{Kind: OutcomeRelease}
	case err != nil && errors.Is(cause, ErrTimeout):
		return Outcome{Kind: OutcomeRetry, Error: "timed out after " + j.Timeout.String()}
	case err != nil && errors.Is(cause, ErrDeadline):
		return Outcome{Kind: OutcomeFail, Error: deadlinePassed}
	}
	return OutcomeOf(err, j.Result)
}

type jobKey struct{}

type jobValue struct {
	job *Job
	src Source
}

func withJob(ctx context.Context, j *Job, src Source) context.Context {
	return context.WithValue(ctx, jobKey{}, jobValue{job: j, src: src})
}

// JobFrom returns the job an attempt's context carries.
func JobFrom(ctx context.Context) *Job {
	v, _ := ctx.Value(jobKey{}).(jobValue)
	return v.job
}

// Dependencies returns the jobs the current job depended on, with their results.
func Dependencies(ctx context.Context) ([]*Job, error) {
	v, ok := ctx.Value(jobKey{}).(jobValue)
	if !ok || len(v.job.DependsOn) == 0 {
		return nil, nil
	}
	return v.src.Jobs(ctx, v.job.DependsOn)
}

// Cancelled reports whether the attempt's context ended because the job was
// taken away, as opposed to a timeout or shutdown.
func Cancelled(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrLost)
}
