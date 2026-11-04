package runnel

import (
	"context"
	"encoding/json"
	"fmt"
)

// QueueConfig sets the limits and defaults of a queue. Zero values mean no
// limit, or the default.
//
// MaxRunning caps running jobs across all workers; TenantMaxRunning caps them
// per tenant. MaxQueued and TenantMaxQueued refuse new jobs past that many queued.
// Rate and TenantRate limit starts per second with token buckets of Burst and
// TenantBurst. MaxAttempts, Backoff, Timeout and StartWithin are defaults for
// jobs that do not set them. Retention and DeadRetention keep finished jobs that
// long before they are pruned.
type QueueConfig struct {
	Name             string   `json:"name"`
	Paused           bool     `json:"paused,omitempty"`
	MaxRunning       int      `json:"max_running,omitempty"`
	TenantMaxRunning int      `json:"tenant_max_running,omitempty"`
	TenantMaxQueued  int      `json:"tenant_max_queued,omitempty"`
	MaxQueued        int      `json:"max_queued,omitempty"`
	Rate             float64  `json:"rate,omitempty"`
	Burst            int      `json:"burst,omitempty"`
	TenantRate       float64  `json:"tenant_rate,omitempty"`
	TenantBurst      int      `json:"tenant_burst,omitempty"`
	MaxAttempts      int      `json:"max_attempts,omitempty"`
	Backoff          Backoff  `json:"backoff,omitzero"`
	Timeout          Duration `json:"timeout,omitempty"`
	StartWithin      Duration `json:"start_within,omitempty"`
	Retention        Duration `json:"retention,omitempty"`
	DeadRetention    Duration `json:"dead_retention,omitempty"`
}

// ResourceConfig is a limit shared by every queue whose jobs name the resource,
// such as the connections a provider account allows. MaxRunning caps running jobs
// on the resource; TenantMaxRunning caps them per tenant.
type ResourceConfig struct {
	Name             string `json:"name"`
	MaxRunning       int    `json:"max_running,omitempty"`
	TenantMaxRunning int    `json:"tenant_max_running,omitempty"`
}

func (q QueueConfig) Validate() error {
	if err := checkName("queue", q.Name, false); err != nil {
		return err
	}
	if q.MaxRunning < 0 || q.TenantMaxRunning < 0 || q.TenantMaxQueued < 0 || q.MaxQueued < 0 || q.Burst < 0 || q.TenantBurst < 0 {
		return invalid("limits must not be negative")
	}
	if q.Rate < 0 || q.TenantRate < 0 || q.Rate > 1e6 || q.TenantRate > 1e6 {
		return invalid("rates must be between 0 and 1e6 per second")
	}
	if q.MaxAttempts < 0 || q.MaxAttempts > maxAttemptsLimit {
		return invalid("max_attempts must be between 0 and %d", maxAttemptsLimit)
	}
	if q.Timeout < 0 || q.StartWithin < 0 || q.Retention < 0 || q.DeadRetention < 0 {
		return invalid("durations must not be negative")
	}
	return nil
}

func (p ResourceConfig) Validate() error {
	if err := checkName("resource", p.Name, false); err != nil {
		return err
	}
	if p.MaxRunning < 0 || p.TenantMaxRunning < 0 {
		return invalid("limits must not be negative")
	}
	return nil
}

// Limits bounds what a single job may carry.
type Limits struct {
	MaxPayload    int
	MaxFallbacks  int
	MaxDependsOn  int
	MaxMeta       int
	MaxUniqueKey  int
	MaxWebhookURL int
}

// DefaultLimits are the limits used unless a client or server sets others: 1 MiB
// payloads, 16 fallbacks, 256 dependencies, 32 meta entries.
var DefaultLimits = Limits{MaxPayload: 1 << 20, MaxFallbacks: 16, MaxDependsOn: 256, MaxMeta: 32, MaxUniqueKey: 255, MaxWebhookURL: 2048}

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
