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

// Tenant names who the job is for, so tenant limits apply to it.
func Tenant(t string) Option { return func(j *Job) { j.Tenant = t } }

// Resource makes the job count against a shared resource.
func Resource(r string) Option { return func(j *Job) { j.Resource = r } }

// At delays the job until t.
func At(t time.Time) Option { return func(j *Job) { j.RunAt = t } }

// In delays the job by d, measured on the store's clock when it is enqueued.
func In(d time.Duration) Option { return func(j *Job) { j.RunAt, j.delay = time.Time{}, d } }

// MaxAttempts caps attempts before the job falls back or dies.
func MaxAttempts(n int) Option { return func(j *Job) { j.MaxAttempts = n } }

// Timeout ends an attempt that runs longer than d.
func Timeout(d time.Duration) Option { return func(j *Job) { j.Timeout = Duration(d) } }

// StartWithin moves the job to its next fallback when it waits longer than d to
// start.
func StartWithin(d time.Duration) Option { return func(j *Job) { j.StartWithin = Duration(d) } }

// WithBackoff spaces the job's retries.
func WithBackoff(b Backoff) Option { return func(j *Job) { j.Backoff = b } }

// Unique refuses a second job with the same key while the first is held: until
// it finishes when window is zero, or for window after it is enqueued.
func Unique(key string, window time.Duration) Option {
	return func(j *Job) { j.UniqueKey, j.UniqueFor = key, Duration(window) }
}

// Fallbacks lists where the job goes, in order, when it fails, runs out of
// attempts or waits too long to start.
func Fallbacks(t ...Target) Option { return func(j *Job) { j.Fallbacks = append(j.Fallbacks, t...) } }

// After makes the job wait until the jobs with these ids complete.
func After(ids ...string) Option { return func(j *Job) { j.DependsOn = append(j.DependsOn, ids...) } }

// Webhook posts the job's summary to url when it finishes.
func Webhook(url string) Option { return func(j *Job) { j.Webhook = url } }

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
