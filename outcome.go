package runnel

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	maxOutcomeDays  = 30
	maxHandleLen    = 1024
	maxOutcomeDelay = maxOutcomeDays * 24 * time.Hour
)

// OutcomeKind is what an attempt asks to happen next.
type OutcomeKind string

const (
	// OutcomeDone completes the job.
	OutcomeDone OutcomeKind = "done"
	// OutcomeRetry tries again after the backoff, or after Delay, until attempts
	// run out.
	OutcomeRetry OutcomeKind = "retry"
	// OutcomeFail stops retrying; the job dies.
	OutcomeFail OutcomeKind = "fail"
	// OutcomeBusy puts the job back for Delay without using an attempt, for a
	// pool that is full.
	OutcomeBusy OutcomeKind = "busy"
	// OutcomePending parks the job while an outside task runs; its poller is called
	// with Handle after Delay.
	OutcomePending OutcomeKind = "pending"
	// OutcomeRelease gives the job back untouched, for shutdowns.
	OutcomeRelease OutcomeKind = "release"
)

var OutcomeKinds = []OutcomeKind{OutcomeDone, OutcomeRetry, OutcomeFail, OutcomeBusy, OutcomePending, OutcomeRelease}

// Outcome is the result of an attempt, as a worker reports it.
type Outcome struct {
	Kind   OutcomeKind     `json:"kind"`
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Delay  Duration        `json:"delay,omitempty"`
	Handle string          `json:"handle,omitempty"`
}

const (
	// DefaultBusyDelay is the wait after Busy when the handler gives none.
	DefaultBusyDelay = 15 * time.Second
	// DefaultPollDelay is the wait between polls when the handler gives none.
	DefaultPollDelay = 5 * time.Second
)

type controlError struct {
	kind   OutcomeKind
	delay  time.Duration
	handle string
	err    error
}

func (e *controlError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.kind, e.err)
	}
	return string(e.kind)
}

func (e *controlError) Unwrap() error { return e.err }

// Busy returns an error that puts the job back for delay without using an
// attempt.
func Busy(delay time.Duration) error {
	return &controlError{kind: OutcomeBusy, delay: delay}
}

// Pending returns an error that parks the job while an outside task runs. The
// job's poller is called with handle after delay.
func Pending(handle string, delay time.Duration) error {
	return &controlError{kind: OutcomePending, handle: handle, delay: delay}
}

// Fail returns an error that stops retrying the job.
func Fail(err error) error { return &controlError{kind: OutcomeFail, err: err} }

// RetryIn returns an error that retries after exactly delay.
func RetryIn(delay time.Duration, err error) error {
	return &controlError{kind: OutcomeRetry, delay: delay, err: err}
}

// OutcomeOf turns a handler's error into an outcome: nil completes with result,
// the errors of this package pick their outcome, and any other error retries.
func OutcomeOf(err error, result json.RawMessage) Outcome {
	if err == nil {
		return Outcome{Kind: OutcomeDone, Result: result}
	}
	var c *controlError
	if errors.As(err, &c) {
		o := Outcome{Kind: c.kind, Delay: Duration(c.delay), Handle: c.handle}
		if c.err != nil {
			o.Error = c.err.Error()
		}
		return o
	}
	return Outcome{Kind: OutcomeRetry, Error: err.Error()}
}

func (o Outcome) Validate() error {
	switch o.Kind {
	case OutcomeDone, OutcomeRetry, OutcomeFail, OutcomeBusy, OutcomeRelease:
	case OutcomePending:
		if len(o.Handle) > maxHandleLen {
			return invalid("handle too long")
		}
	default:
		return invalid("unknown outcome %q", o.Kind)
	}
	if o.Delay < 0 || o.Delay.D() > maxOutcomeDelay {
		return invalid("delay must be between 0 and %d days", maxOutcomeDays)
	}
	if len(o.Result) > 0 && !json.Valid(o.Result) {
		return invalid("result must be valid JSON")
	}
	return nil
}

const deadlinePassed = "deadline passed"

// Decide returns the job as it should be stored after outcome o at now. It
// changes nothing in place and does no I/O. A job that has not completed by its
// deadline, or would run again only after it, ends as expired.
func Decide(j *Job, o Outcome, now time.Time) *Job {
	n := decide(j, o, now)
	stopped := o.Kind == OutcomeFail && o.Error == deadlinePassed && !j.Deadline.IsZero()
	if o.Kind == OutcomeDone || (!stopped && !j.late(now) && !j.late(n.RunAt)) {
		return n
	}
	if !n.State.Final() {
		n.addError(deadlinePassed, now)
	}
	end(n, StateExpired, now)
	return n
}

func decide(j *Job, o Outcome, now time.Time) *Job {
	n := j.Clone()
	polling := j.Polling()
	n.Worker, n.LeaseUntil, n.UpdatedAt = "", time.Time{}, now
	delay := o.Delay.D()
	switch o.Kind {
	case OutcomeDone:
		n.State, n.Result, n.Handle, n.FinishedAt, n.StartBy = StateCompleted, o.Result, "", now, time.Time{}
	case OutcomePending:
		if delay <= 0 {
			delay = DefaultPollDelay
		}
		if o.Handle != "" {
			n.Handle = o.Handle
		}
		if n.Handle == "" {
			n.addError("pending without a handle", now)
			end(n, StateDead, now)
			return n
		}
		n.State, n.RunAt, n.StartBy = StatePolling, now.Add(delay), time.Time{}
	case OutcomeBusy, OutcomeRelease:
		if o.Kind == OutcomeBusy && delay <= 0 {
			delay = DefaultBusyDelay
		}
		if polling {
			n.State, n.RunAt = StatePolling, now.Add(delay)
			return n
		}
		n.Attempt = max(n.Attempt-1, 0)
		n.State, n.RunAt = StateWaiting, now.Add(delay)
	case OutcomeRetry:
		n.addError(orDefault(o.Error, "retry"), now)
		n.Handle = ""
		if n.Attempt < n.MaxAttempts {
			if delay <= 0 {
				delay = n.Backoff.Delay(n.Attempt)
			}
			n.State, n.RunAt, n.StartBy = StateWaiting, now.Add(delay), time.Time{}
			return n
		}
		end(n, StateDead, now)
	case OutcomeFail:
		n.addError(orDefault(o.Error, string(o.Kind)), now)
		end(n, StateDead, now)
	}
	return n
}

func end(n *Job, st State, now time.Time) {
	n.Handle = ""
	n.State, n.FinishedAt, n.StartBy = st, now, time.Time{}
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
