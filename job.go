package runnel

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"
)

const (
	defaultBackoffBase   = time.Second
	defaultBackoffMax    = 10 * time.Minute
	defaultBackoffFactor = 2
	defaultBackoffJitter = 0.2
	maxErrorLen          = 4096
)

// State is where a job is in its life.
type State string

const (
	// StateBlocked jobs wait for the jobs they depend on.
	StateBlocked State = "blocked"
	// StateWaiting jobs are ready to claim once RunAt passes.
	StateWaiting State = "waiting"
	// StateRunning jobs are leased to a worker.
	StateRunning State = "running"
	// StatePolling jobs wait for an outside task and are polled at RunAt.
	StatePolling State = "polling"
	// StateCompleted jobs finished successfully.
	StateCompleted State = "completed"
	// StateDead jobs ran out of attempts and fallbacks, or failed for good.
	StateDead State = "dead"
	// StateCancelled jobs were cancelled, or a job they depended on died.
	StateCancelled State = "cancelled"
)

var States = []State{StateBlocked, StateWaiting, StateRunning, StatePolling, StateCompleted, StateDead, StateCancelled}

// Final reports whether the job can no longer change state on its own.
func (s State) Final() bool {
	return s == StateCompleted || s == StateDead || s == StateCancelled
}

// Active reports whether the job holds a slot: running or polling.
func (s State) Active() bool {
	return s == StateRunning || s == StatePolling
}

func (s State) Valid() bool {
	return slices.Contains(States, s)
}

// Duration is a time.Duration that reads JSON as "30s" or as seconds, and writes
// it as "30s".
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	var secs float64
	if err := json.Unmarshal(b, &secs); err != nil {
		return errors.New("duration must be a string like \"30s\" or seconds")
	}
	if math.IsNaN(secs) || math.IsInf(secs, 0) || math.Abs(secs) > math.MaxInt64/float64(time.Second) {
		return errors.New("duration out of range")
	}
	*d = Duration(secs * float64(time.Second))
	return nil
}

// Backoff spaces retries: Base times Factor to the power of the attempt, capped
// at Max, with Jitter as a fraction either way. Zero fields mean 1s, 10m, 2 and
// 0.2.
type Backoff struct {
	Base   Duration `json:"base,omitempty"`
	Max    Duration `json:"max,omitempty"`
	Factor float64  `json:"factor,omitempty"`
	Jitter float64  `json:"jitter,omitempty"`
}

func (b Backoff) IsZero() bool { return b == Backoff{} }

func (b Backoff) Delay(attempt int) time.Duration {
	base, maxd, factor, jitter := b.Base.D(), b.Max.D(), b.Factor, b.Jitter
	if base <= 0 {
		base = defaultBackoffBase
	}
	if maxd <= 0 {
		maxd = defaultBackoffMax
	}
	if factor < 1 {
		factor = defaultBackoffFactor
	}
	if jitter == 0 {
		jitter = defaultBackoffJitter
	}
	if jitter < 0 || jitter > 1 {
		jitter = 0
	}
	d := float64(base) * math.Pow(factor, float64(max(attempt-1, 0)))
	if d > float64(maxd) || math.IsInf(d, 0) {
		d = float64(maxd)
	}
	if jitter > 0 {
		d *= 1 - jitter + rand.Float64()*2*jitter
	}
	return time.Duration(d)
}

// Target is where a job goes when it falls back. Empty fields keep the job's
// current queue, kind or pool.
type Target struct {
	Queue string `json:"queue,omitempty"`
	Kind  string `json:"kind,omitempty"`
	Pool  string `json:"pool,omitempty"`
}

// AttemptError records why an attempt did not complete.
type AttemptError struct {
	Attempt int       `json:"attempt"`
	Queue   string    `json:"queue"`
	Kind    string    `json:"kind"`
	At      time.Time `json:"at"`
	Error   string    `json:"error"`
}

const maxErrors = 20

// Job is a unit of work. Kind picks the handler; Payload is its input and Result
// its output. Producers set the fields up to Meta; the store keeps the rest.
type Job struct {
	ID           string            `json:"id"`
	Namespace    string            `json:"namespace"`
	Queue        string            `json:"queue"`
	Kind         string            `json:"kind"`
	Payload      json.RawMessage   `json:"payload,omitempty"`
	Priority     int               `json:"priority"`
	Tenant       string            `json:"tenant,omitempty"`
	Pool         string            `json:"pool,omitempty"`
	State        State             `json:"state"`
	Attempt      int               `json:"attempt"`
	MaxAttempts  int               `json:"max_attempts"`
	RunAt        time.Time         `json:"run_at"`
	WaitingSince time.Time         `json:"waiting_since"`
	StartWithin  Duration          `json:"start_within,omitempty"`
	StartBy      time.Time         `json:"start_by,omitzero"`
	Timeout      Duration          `json:"timeout,omitempty"`
	Backoff      Backoff           `json:"backoff,omitzero"`
	Worker       string            `json:"worker,omitempty"`
	LeaseUntil   time.Time         `json:"lease_until,omitzero"`
	Version      int64             `json:"version"`
	Handle       string            `json:"handle,omitempty"`
	Fallbacks    []Target          `json:"fallbacks,omitempty"`
	Fallback     int               `json:"fallback,omitempty"`
	UniqueKey    string            `json:"unique_key,omitempty"`
	UniqueFor    Duration          `json:"unique_for,omitempty"`
	DependsOn    []string          `json:"depends_on,omitempty"`
	Pending      int               `json:"pending,omitempty"`
	Webhook      string            `json:"webhook,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
	Result       json.RawMessage   `json:"result,omitempty"`
	Errors       []AttemptError    `json:"errors,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	StartedAt    time.Time         `json:"started_at,omitzero"`
	FinishedAt   time.Time         `json:"finished_at,omitzero"`

	delay time.Duration
}

type jobData struct {
	Payload     json.RawMessage   `json:"p,omitempty"`
	StartWithin Duration          `json:"sw,omitempty"`
	Timeout     Duration          `json:"to,omitempty"`
	Backoff     Backoff           `json:"bo,omitzero"`
	Handle      string            `json:"h,omitempty"`
	Fallbacks   []Target          `json:"fb,omitempty"`
	Fallback    int               `json:"fi,omitempty"`
	UniqueFor   Duration          `json:"uf,omitempty"`
	DependsOn   []string          `json:"d,omitempty"`
	Webhook     string            `json:"w,omitempty"`
	Meta        map[string]string `json:"m,omitempty"`
	Result      json.RawMessage   `json:"r,omitempty"`
	Errors      []AttemptError    `json:"e,omitempty"`
}

// EncodeData packs the fields stores keep in one blob.
func (j *Job) EncodeData() ([]byte, error) {
	return json.Marshal(jobData{
		Payload: j.Payload, StartWithin: j.StartWithin, Timeout: j.Timeout, Backoff: j.Backoff,
		Handle: j.Handle, Fallbacks: j.Fallbacks, Fallback: j.Fallback, UniqueFor: j.UniqueFor,
		DependsOn: j.DependsOn, Webhook: j.Webhook, Meta: j.Meta, Result: j.Result, Errors: j.Errors,
	})
}

// DecodeData unpacks a blob written by EncodeData.
func (j *Job) DecodeData(b []byte) error {
	var d jobData
	if len(b) > 0 {
		if err := json.Unmarshal(b, &d); err != nil {
			return fmt.Errorf("job %s data: %w", j.ID, err)
		}
	}
	j.Payload, j.StartWithin, j.Timeout, j.Backoff = d.Payload, d.StartWithin, d.Timeout, d.Backoff
	j.Handle, j.Fallbacks, j.Fallback, j.UniqueFor = d.Handle, d.Fallbacks, d.Fallback, d.UniqueFor
	j.DependsOn, j.Webhook, j.Meta, j.Result, j.Errors = d.DependsOn, d.Webhook, d.Meta, d.Result, d.Errors
	return nil
}

// StartWindow is how long the job may wait before it moves to its next fallback,
// or zero when it has none left.
func (j *Job) StartWindow() time.Duration {
	if j.StartWithin > 0 && j.Fallback < len(j.Fallbacks) {
		return j.StartWithin.D()
	}
	return 0
}

func (j *Job) computeStartBy() {
	if w := j.StartWindow(); w > 0 && !j.WaitingSince.IsZero() {
		j.StartBy = j.WaitingSince.Add(w)
		return
	}
	j.StartBy = time.Time{}
}

// Prepare fills defaults, resets the fields stores own and validates the job
// against l. Clients call it before Store.Insert.
func (j *Job) Prepare(now time.Time, l Limits) error {
	if !ValidID(j.ID) {
		j.ID = NewID()
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = DefaultMaxAttempts
	}
	if j.delay > 0 {
		j.RunAt, j.delay = now.Add(j.delay), 0
	}
	if j.RunAt.IsZero() || j.RunAt.Before(now) {
		j.RunAt = now
	}
	j.RunAt = j.RunAt.UTC().Truncate(time.Millisecond)
	j.State, j.Attempt, j.Version, j.Worker, j.LeaseUntil = StateWaiting, 0, 0, "", time.Time{}
	j.Handle, j.Fallback, j.Pending, j.Result, j.Errors = "", 0, 0, nil, nil
	j.WaitingSince, j.CreatedAt, j.UpdatedAt = j.RunAt, now, now
	j.StartedAt, j.FinishedAt = time.Time{}, time.Time{}
	j.computeStartBy()
	return j.validate(l)
}

// Polling reports whether the job is waiting on an outside task.
func (j *Job) Polling() bool { return j.Handle != "" }

func (j *Job) Bind(v any) error {
	if len(j.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(j.Payload, v)
}

func (j *Job) SetResult(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	j.Result = b
	return nil
}

// Clone returns a deep copy.
func (j *Job) Clone() *Job {
	c := *j
	c.Payload = slices.Clone(j.Payload)
	c.Result = slices.Clone(j.Result)
	c.Fallbacks = slices.Clone(j.Fallbacks)
	c.DependsOn = slices.Clone(j.DependsOn)
	c.Errors = slices.Clone(j.Errors)
	c.Meta = maps.Clone(j.Meta)
	return &c
}

func (j *Job) addError(msg string, now time.Time) {
	if len(msg) > maxErrorLen {
		msg = msg[:maxErrorLen]
	}
	j.Errors = append(j.Errors, AttemptError{Attempt: j.Attempt, Queue: j.Queue, Kind: j.Kind, At: now, Error: msg})
	if len(j.Errors) > maxErrors {
		j.Errors = slices.Clone(j.Errors[len(j.Errors)-maxErrors:])
	}
}

// Millis returns t as Unix milliseconds, with the zero time as 0.
func Millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// FromMillis reverses Millis.
func FromMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func MillisString(t time.Time) string { return strconv.FormatInt(Millis(t), 10) }
