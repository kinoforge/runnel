package runnel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

// QueueConfig sets the limits and defaults of a queue. Zero values mean no
// limit, or the default.
//
// Concurrency caps running jobs across all workers; TenantSlots caps them per
// tenant. TenantWaiting and MaxWaiting refuse new jobs past that many waiting.
// Rate and TenantRate limit starts per second with token buckets of Burst and
// TenantBurst. MaxAttempts, Backoff, Timeout and StartWithin are defaults for
// jobs that do not set them. Retention and DeadRetention keep finished jobs that
// long before they are pruned.
type QueueConfig struct {
	Name          string   `json:"name"`
	Paused        bool     `json:"paused,omitempty"`
	Concurrency   int      `json:"concurrency,omitempty"`
	TenantSlots   int      `json:"tenant_slots,omitempty"`
	TenantWaiting int      `json:"tenant_waiting,omitempty"`
	MaxWaiting    int      `json:"max_waiting,omitempty"`
	Rate          float64  `json:"rate,omitempty"`
	Burst         int      `json:"burst,omitempty"`
	TenantRate    float64  `json:"tenant_rate,omitempty"`
	TenantBurst   int      `json:"tenant_burst,omitempty"`
	MaxAttempts   int      `json:"max_attempts,omitempty"`
	Backoff       Backoff  `json:"backoff,omitzero"`
	Timeout       Duration `json:"timeout,omitempty"`
	StartWithin   Duration `json:"start_within,omitempty"`
	Retention     Duration `json:"retention,omitempty"`
	DeadRetention Duration `json:"dead_retention,omitempty"`
}

// PoolConfig is a limit shared by every queue whose jobs name the pool, such as
// the connections a provider account allows. Slots caps running jobs in the
// pool; TenantSlots caps them per tenant.
type PoolConfig struct {
	Name        string `json:"name"`
	Slots       int    `json:"slots,omitempty"`
	TenantSlots int    `json:"tenant_slots,omitempty"`
}

// Schedule enqueues a job on a cron spec. Each firing is enqueued once, even
// with many maintainers running.
type Schedule struct {
	Name        string            `json:"name"`
	Spec        string            `json:"spec"`
	Timezone    string            `json:"timezone,omitempty"`
	Paused      bool              `json:"paused,omitempty"`
	Queue       string            `json:"queue"`
	Kind        string            `json:"kind"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Tenant      string            `json:"tenant,omitempty"`
	Pool        string            `json:"pool,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Timeout     Duration          `json:"timeout,omitempty"`
	Fallbacks   []Target          `json:"fallbacks,omitempty"`
	Webhook     string            `json:"webhook,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// APIKey grants roles in one namespace. Only the SHA-256 hash of its secret is
// stored.
type APIKey struct {
	ID        string    `json:"id"`
	Namespace string    `json:"namespace"`
	Name      string    `json:"name,omitempty"`
	Hash      string    `json:"hash"`
	Roles     []string  `json:"roles"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

// Namespace is one isolated queue system, usually one per service. WebhookSecret
// signs its webhooks.
type Namespace struct {
	Name          string    `json:"name"`
	WebhookSecret string    `json:"webhook_secret,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

const (
	// RoleAdmin may do everything in its namespace, including changing
	// configuration and retrying dead jobs.
	RoleAdmin = "admin"
	// RoleProducer may enqueue and cancel jobs.
	RoleProducer = "producer"
	// RoleWorker may claim, heartbeat and finish jobs.
	RoleWorker = "worker"
	// RoleReader may read jobs, events and stats.
	RoleReader = "reader"
)

// Roles lists every role.
var Roles = []string{RoleAdmin, RoleProducer, RoleWorker, RoleReader}

var (
	namePattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	namespacePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	tenantPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)
)

// ValidName reports whether s can name a queue, kind, pool or schedule: 1 to 128
// letters, digits, dots, dashes or underscores, starting with a letter or digit.
func ValidName(s string) bool { return namePattern.MatchString(s) }

// ValidNamespace reports whether s can name a namespace: 1 to 63 lowercase
// letters, digits, dashes or underscores.
func ValidNamespace(s string) bool { return namespacePattern.MatchString(s) }

// ValidTenant reports whether s can name a tenant.
func ValidTenant(s string) bool { return tenantPattern.MatchString(s) }

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func checkName(field, v string, optional bool) error {
	if v == "" && optional {
		return nil
	}
	if !ValidName(v) {
		return invalid("%s %q must match %s", field, v, namePattern)
	}
	return nil
}

// Validate checks names, limits and durations.
func (q QueueConfig) Validate() error {
	if err := checkName("queue", q.Name, false); err != nil {
		return err
	}
	if q.Concurrency < 0 || q.TenantSlots < 0 || q.TenantWaiting < 0 || q.MaxWaiting < 0 || q.Burst < 0 || q.TenantBurst < 0 {
		return invalid("limits must not be negative")
	}
	if q.Rate < 0 || q.TenantRate < 0 || q.Rate > 1e6 || q.TenantRate > 1e6 {
		return invalid("rates must be between 0 and 1e6 per second")
	}
	if q.MaxAttempts < 0 || q.MaxAttempts > 1000 {
		return invalid("max_attempts must be between 0 and 1000")
	}
	if q.Timeout < 0 || q.StartWithin < 0 || q.Retention < 0 || q.DeadRetention < 0 {
		return invalid("durations must not be negative")
	}
	return nil
}

// Validate checks the name and slots.
func (p PoolConfig) Validate() error {
	if err := checkName("pool", p.Name, false); err != nil {
		return err
	}
	if p.Slots < 0 || p.TenantSlots < 0 {
		return invalid("slots must not be negative")
	}
	return nil
}

// Validate checks the spec, timezone and the job the schedule makes.
func (s Schedule) Validate() error {
	if err := checkName("schedule", s.Name, false); err != nil {
		return err
	}
	if _, err := parseSpec(s.Spec, s.Timezone); err != nil {
		return invalid("schedule spec: %v", err)
	}
	probe := &Job{Queue: s.Queue, Kind: s.Kind, Payload: s.Payload, Priority: s.Priority, Tenant: s.Tenant,
		Pool: s.Pool, MaxAttempts: max(s.MaxAttempts, 1), Timeout: s.Timeout, Fallbacks: s.Fallbacks, Webhook: s.Webhook, Meta: s.Meta}
	return probe.validate(DefaultLimits)
}

// Validate checks the name and secret length.
func (n Namespace) Validate() error {
	if !ValidNamespace(n.Name) {
		return invalid("namespace %q must match %s", n.Name, namespacePattern)
	}
	if len(n.WebhookSecret) > 256 {
		return invalid("webhook secret too long")
	}
	return nil
}

// Validate checks the namespace, id, hash and roles.
func (k APIKey) Validate() error {
	if !ValidNamespace(k.Namespace) {
		return invalid("namespace %q must match %s", k.Namespace, namespacePattern)
	}
	if len(k.ID) != 12 || k.Hash == "" {
		return invalid("key id and hash are required")
	}
	if len(k.Roles) == 0 {
		return invalid("a key needs at least one role")
	}
	for _, r := range k.Roles {
		if !slices.Contains(Roles, r) {
			return invalid("unknown role %q", r)
		}
	}
	if len(k.Name) > 100 {
		return invalid("key name too long")
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

func (j *Job) validate(l Limits) error {
	if err := checkName("queue", j.Queue, false); err != nil {
		return err
	}
	if err := checkName("kind", j.Kind, false); err != nil {
		return err
	}
	if err := checkName("pool", j.Pool, true); err != nil {
		return err
	}
	if j.Tenant != "" && !ValidTenant(j.Tenant) {
		return invalid("tenant %q must match %s", j.Tenant, tenantPattern)
	}
	if len(j.Payload) > l.MaxPayload {
		return invalid("payload is %d bytes, limit %d", len(j.Payload), l.MaxPayload)
	}
	if len(j.Payload) > 0 && !json.Valid(j.Payload) {
		return invalid("payload must be valid JSON")
	}
	if j.Priority < -1000 || j.Priority > 1000 {
		return invalid("priority must be between -1000 and 1000")
	}
	if j.MaxAttempts < 1 || j.MaxAttempts > 1000 {
		return invalid("max_attempts must be between 1 and 1000")
	}
	if j.Timeout < 0 || j.StartWithin < 0 || j.UniqueFor < 0 {
		return invalid("durations must not be negative")
	}
	if len(j.Fallbacks) > l.MaxFallbacks {
		return invalid("at most %d fallbacks", l.MaxFallbacks)
	}
	for _, t := range j.Fallbacks {
		if t == (Target{}) {
			return invalid("fallback target is empty")
		}
		if err := checkName("fallback queue", t.Queue, true); err != nil {
			return err
		}
		if err := checkName("fallback kind", t.Kind, true); err != nil {
			return err
		}
		if err := checkName("fallback pool", t.Pool, true); err != nil {
			return err
		}
	}
	if len(j.DependsOn) > l.MaxDependsOn {
		return invalid("at most %d dependencies", l.MaxDependsOn)
	}
	for _, d := range j.DependsOn {
		if !ValidID(d) {
			return invalid("dependency %q is not a job id", d)
		}
	}
	if len(j.UniqueKey) > l.MaxUniqueKey || strings.ContainsAny(j.UniqueKey, "|\x00") {
		return invalid("unique key must be at most %d bytes without '|'", l.MaxUniqueKey)
	}
	if err := ValidMeta(j.Meta, l); err != nil {
		return err
	}
	if j.Webhook != "" {
		if err := ValidWebhookURL(j.Webhook, l.MaxWebhookURL); err != nil {
			return err
		}
	}
	return nil
}

// ValidMeta checks meta against the limits: at most MaxMeta entries, keys of
// 1 to 64 bytes and values of at most 512.
func ValidMeta(meta map[string]string, l Limits) error {
	if len(meta) > l.MaxMeta {
		return invalid("at most %d meta entries", l.MaxMeta)
	}
	for k, v := range meta {
		if len(k) == 0 || len(k) > 64 || len(v) > 512 {
			return invalid("meta keys must be 1-64 bytes and values at most 512")
		}
	}
	return nil
}

// ValidWebhookURL checks that raw is an absolute http or https URL without
// credentials, at most maxLen bytes.
func ValidWebhookURL(raw string, maxLen int) error {
	if len(raw) > maxLen {
		return invalid("webhook url too long")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil {
		return invalid("webhook must be an absolute http(s) url without credentials")
	}
	return nil
}

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

// PutNamespace creates or replaces a namespace.
func PutNamespace(ctx context.Context, s Store, n Namespace) error {
	if err := n.Validate(); err != nil {
		return err
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	return PutTyped(ctx, s, "", ConfigNamespace, n.Name, n)
}

// Namespaces returns every namespace by name.
func Namespaces(ctx context.Context, s Store) (map[string]Namespace, error) {
	return LoadTyped[Namespace](ctx, s, "", ConfigNamespace)
}

// DeleteNamespace removes a namespace and revokes its keys. Its jobs stay until
// they are pruned.
func DeleteNamespace(ctx context.Context, s Store, name string) error {
	if err := s.DeleteConfig(ctx, "", ConfigNamespace, name); err != nil {
		return err
	}
	keys, err := NamespaceKeys(ctx, s, name)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.DeleteConfig(ctx, "", ConfigKey, k.ID); err != nil {
			return err
		}
	}
	return nil
}

// PutKey stores an API key.
func PutKey(ctx context.Context, s Store, k APIKey) error {
	if err := k.Validate(); err != nil {
		return err
	}
	return PutTyped(ctx, s, "", ConfigKey, k.ID, k)
}

// Keys returns every API key by id.
func Keys(ctx context.Context, s Store) (map[string]APIKey, error) {
	return LoadTyped[APIKey](ctx, s, "", ConfigKey)
}

// NamespaceKeys returns the keys of one namespace, oldest first.
func NamespaceKeys(ctx context.Context, s Store, ns string) ([]APIKey, error) {
	all, err := Keys(ctx, s)
	if err != nil {
		return nil, err
	}
	out := []APIKey{}
	for _, k := range all {
		if k.Namespace == ns {
			out = append(out, k)
		}
	}
	slices.SortFunc(out, func(a, b APIKey) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out, nil
}

// DeleteKey revokes a key of the namespace.
func DeleteKey(ctx context.Context, s Store, ns, id string) error {
	all, err := Keys(ctx, s)
	if err != nil {
		return err
	}
	if k, ok := all[id]; !ok || k.Namespace != ns {
		return ErrNotFound
	}
	return s.DeleteConfig(ctx, "", ConfigKey, id)
}
