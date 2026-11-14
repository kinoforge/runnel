package runnel

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// QueueConfig sets the limits and defaults of a queue. Zero values mean no
// limit, or the default.
//
// MaxRunning caps running jobs across all workers; GroupMaxRunning caps them
// per group. MaxQueued and GroupMaxQueued refuse new jobs past that many queued.
// Rate and GroupRate limit starts per second with token buckets of Burst and
// GroupBurst. MaxAttempts, Backoff, Timeout, StartTimeout, TTL and CallbackURL
// are defaults for jobs that do not set them. Wait is how long an HTTP enqueue waits for the
// result when the caller sends no Prefer header. Retention and DeadRetention keep finished jobs that
// long before they are pruned.
type QueueConfig struct {
	Name            string   `json:"name"`
	Paused          bool     `json:"paused,omitempty"`
	MaxRunning      int      `json:"max_running,omitempty"`
	GroupMaxRunning int      `json:"group_max_running,omitempty"`
	GroupMaxQueued  int      `json:"group_max_queued,omitempty"`
	MaxQueued       int      `json:"max_queued,omitempty"`
	Rate            float64  `json:"rate,omitempty"`
	Burst           int      `json:"burst,omitempty"`
	GroupRate       float64  `json:"group_rate,omitempty"`
	GroupBurst      int      `json:"group_burst,omitempty"`
	MaxAttempts     int      `json:"max_attempts,omitempty"`
	Backoff         Backoff  `json:"backoff,omitzero"`
	Timeout         Duration `json:"timeout,omitempty"`
	StartTimeout    Duration `json:"start_timeout,omitempty"`
	TTL             Duration `json:"ttl,omitempty"`
	Wait            Duration `json:"wait,omitempty"`
	Retention       Duration `json:"retention,omitempty"`
	DeadRetention   Duration `json:"dead_retention,omitempty"`
	CallbackURL     string   `json:"callback_url,omitempty"`
}

// PoolConfig is a number of slots shared by every job that names the pool,
// whatever its queue, such as the calls a provider account allows at once.
// Slots caps the pool's running jobs; GroupSlots caps them per group. Rate
// limits the pool's starts per second with a token bucket of Burst. A paused
// pool starts no jobs, and neither does one before PausedUntil; jobs already
// running or polling go on.
type PoolConfig struct {
	Name        string    `json:"name"`
	Slots       int       `json:"slots,omitempty"`
	GroupSlots  int       `json:"group_slots,omitempty"`
	Rate        float64   `json:"rate,omitempty"`
	Burst       int       `json:"burst,omitempty"`
	Paused      bool      `json:"paused,omitempty"`
	PausedUntil time.Time `json:"paused_until,omitzero"`
}

func (q QueueConfig) Validate() error {
	if err := checkName("queue", q.Name, false); err != nil {
		return err
	}
	if q.MaxRunning < 0 || q.GroupMaxRunning < 0 || q.GroupMaxQueued < 0 || q.MaxQueued < 0 || q.Burst < 0 || q.GroupBurst < 0 {
		return invalid("limits must not be negative")
	}
	if q.Rate < 0 || q.GroupRate < 0 || q.Rate > 1e6 || q.GroupRate > 1e6 {
		return invalid("rates must be between 0 and 1e6 per second")
	}
	if q.MaxAttempts < 0 || q.MaxAttempts > maxAttemptsLimit {
		return invalid("max_attempts must be between 0 and %d", maxAttemptsLimit)
	}
	if q.Timeout < 0 || q.StartTimeout < 0 || q.TTL < 0 || q.Wait < 0 || q.Retention < 0 || q.DeadRetention < 0 {
		return invalid("durations must not be negative")
	}
	if q.CallbackURL != "" {
		return ValidCallbackURL(q.CallbackURL, DefaultLimits.MaxCallbackURL)
	}
	return nil
}

func (p PoolConfig) Validate() error {
	if err := checkName("pool", p.Name, false); err != nil {
		return err
	}
	if p.Slots < 0 || p.GroupSlots < 0 || p.Burst < 0 {
		return invalid("limits must not be negative")
	}
	if p.Rate < 0 || p.Rate > 1e6 {
		return invalid("rate must be between 0 and 1e6 per second")
	}
	return nil
}

// Limited reports whether the pool caps or holds back any start.
func (p PoolConfig) Limited() bool {
	return p.Slots > 0 || p.GroupSlots > 0 || p.Rate > 0 || p.Paused || !p.PausedUntil.IsZero()
}

// Stopped reports whether the pool starts no jobs at now, in Unix milliseconds.
func (p PoolConfig) Stopped(nowMs int64) bool {
	return p.Paused || (!p.PausedUntil.IsZero() && nowMs < p.PausedUntil.UnixMilli())
}

// Limits bounds what a single job may carry.
type Limits struct {
	MaxPayload     int
	MaxDependsOn   int
	MaxMeta        int
	MaxMetaKey     int
	MaxMetaValue   int
	MaxUniqueKey   int
	MaxCallbackURL int
}

// DefaultLimits are the limits used unless a client or server sets others: 1 MiB
// payloads, 256 dependencies, 32 meta entries.
var DefaultLimits = Limits{MaxPayload: 1 << 20, MaxDependsOn: 256, MaxMeta: 32, MaxMetaKey: 64, MaxMetaValue: 512, MaxUniqueKey: 255, MaxCallbackURL: 2048}

// PutTyped stores v as JSON under a configuration kind and name.
func PutTyped(ctx context.Context, s Store, ns string, kind ConfigKind, name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.PutConfig(ctx, ns, kind, name, b)
}

// LoadTyped reads every value of a configuration kind into T.
func LoadTyped[T any](ctx context.Context, s Store, ns string, kind ConfigKind) (map[string]T, error) {
	raw, err := s.Configs(ctx, ns, kind)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(raw))
	for name, b := range raw {
		var v T
		if err := json.Unmarshal(b, &v); err != nil {
			return nil, fmt.Errorf("%s config %q: %w", kind, name, err)
		}
		out[name] = v
	}
	return out, nil
}
