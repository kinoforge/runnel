package runnel

import (
	"encoding/json"
	"maps"
	"time"
)

// Option sets a field of a new job.
type Option func(*Job)

// Priority orders jobs within a queue, from -1000 to 1000; higher runs first.
func Priority(p int) Option { return func(j *Job) { j.Priority = p } }

// Group names who the job is for, so group limits apply to it.
func Group(t string) Option { return func(j *Job) { j.Group = t } }

// Pool makes the job count against a shared pool.
func Pool(r string) Option { return func(j *Job) { j.Pool = r } }

// At delays the job until t.
func At(t time.Time) Option { return func(j *Job) { j.RunAt = t } }

// In delays the job by d, measured on the store's clock when it is enqueued.
func In(d time.Duration) Option { return func(j *Job) { j.RunAt, j.delay = time.Time{}, d } }

// MaxAttempts caps attempts before the job falls back or dies.
func MaxAttempts(n int) Option { return func(j *Job) { j.MaxAttempts = n } }

// Timeout ends an attempt that runs longer than d.
func Timeout(d time.Duration) Option { return func(j *Job) { j.Timeout = Duration(d) } }

// StartTimeout ends the job as expired when it waits longer than d to start.
func StartTimeout(d time.Duration) Option { return func(j *Job) { j.StartTimeout = Duration(d) } }

// WithBackoff spaces the job's retries.
func WithBackoff(b Backoff) Option { return func(j *Job) { j.Backoff = b } }

// Unique refuses a second job with the same key while the first is held: until
// it finishes when window is zero, or for window after it is enqueued.
func Unique(key string, window time.Duration) Option {
	return func(j *Job) { j.UniqueKey, j.UniqueFor = key, Duration(window) }
}

// After makes the job wait until the jobs with these ids complete.
func After(ids ...string) Option { return func(j *Job) { j.DependsOn = append(j.DependsOn, ids...) } }

// Callback posts the job's summary to url when it finishes.
func Callback(url string) Option { return func(j *Job) { j.CallbackURL = url } }

// Meta adds string labels to the job.
func Meta(kv map[string]string) Option {
	return func(j *Job) {
		if j.Meta == nil {
			j.Meta = map[string]string{}
		}
		maps.Copy(j.Meta, kv)
	}
}

// NewJob builds a job. A payload that is not raw JSON or bytes is encoded as
// JSON.
func NewJob(queue, kind string, payload any, opts ...Option) (*Job, error) {
	j := &Job{Queue: queue, Kind: kind}
	if payload != nil {
		switch p := payload.(type) {
		case json.RawMessage:
			j.Payload = p
		case []byte:
			j.Payload = p
		default:
			b, err := json.Marshal(payload)
			if err != nil {
				return nil, invalid("payload: %v", err)
			}
			j.Payload = b
		}
	}
	for _, o := range opts {
		o(j)
	}
	return j, nil
}
