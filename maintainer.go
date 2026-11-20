package runnel

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	defaultMaintainInterval = time.Second
	defaultPruneEvery       = time.Minute
	defaultMaintainBatch    = 500
)

// Retention says how long finished jobs and events are kept. Undelivered bounds
// how long events wait for callback delivery before they are dropped and logged.
type Retention struct {
	Completed   time.Duration
	Cancelled   time.Duration
	Dead        time.Duration
	Events      time.Duration
	Undelivered time.Duration
}

// DefaultRetention is used for every Retention field left at zero.
var DefaultRetention = Retention{
	Completed:   24 * time.Hour,
	Cancelled:   24 * time.Hour,
	Dead:        7 * 24 * time.Hour,
	Events:      24 * time.Hour,
	Undelivered: 7 * 24 * time.Hour,
}

// MaintainerConfig configures a Maintainer. Namespaces limits it to those
// namespaces; empty means all.
type MaintainerConfig struct {
	Namespaces []string
	Interval   time.Duration
	PruneEvery time.Duration
	Retention  Retention
	Archive    bool
	Batch      int
	Callbacks  CallbackConfig
	Limits     Limits
	Logger     *slog.Logger
}

// Maintainer does the housekeeping of a store: it returns jobs whose worker
// died, expires jobs past their start timeout, fires schedules,
// turns events into callback jobs and prunes old jobs and events. Run as many as
// you like; their work does not overlap.
type Maintainer struct {
	store Store
	cfg   MaintainerConfig
	log   *slog.Logger
	clock Clock

	mu        sync.Mutex
	wg        sync.WaitGroup
	clients   map[string]*Client
	hookCli   map[string]*Client
	lastPrune map[string]time.Time
	specs     map[string]cron.Schedule
	hooks     map[string]context.CancelFunc
}

func NewMaintainer(s Store, cfg MaintainerConfig) *Maintainer {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultMaintainInterval
	}
	if cfg.PruneEvery <= 0 {
		cfg.PruneEvery = defaultPruneEvery
	}
	cfg.Retention.Completed = pick(cfg.Retention.Completed, DefaultRetention.Completed)
	cfg.Retention.Cancelled = pick(cfg.Retention.Cancelled, DefaultRetention.Cancelled)
	cfg.Retention.Dead = pick(cfg.Retention.Dead, DefaultRetention.Dead)
	cfg.Retention.Events = pick(cfg.Retention.Events, DefaultRetention.Events)
	cfg.Retention.Undelivered = pick(cfg.Retention.Undelivered, DefaultRetention.Undelivered)
	cfg.Retention.Undelivered = max(cfg.Retention.Undelivered, cfg.Retention.Events)
	if cfg.Batch <= 0 {
		cfg.Batch = defaultMaintainBatch
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Limits == (Limits{}) {
		cfg.Limits = DefaultLimits
	}
	cfg.Callbacks = cfg.Callbacks.withDefaults()
	if cfg.Callbacks.MaxPayload <= 0 {
		cfg.Callbacks.MaxPayload = cfg.Limits.MaxPayload + defaultCallbackRoom
	}
	return &Maintainer{store: s, cfg: cfg, log: cfg.Logger, clock: ClockOf(s), clients: map[string]*Client{}, hookCli: map[string]*Client{}, lastPrune: map[string]time.Time{},
		specs: map[string]cron.Schedule{}, hooks: map[string]context.CancelFunc{}}
}

func (m *Maintainer) client(ns string) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.clients[ns]
	if c == nil {
		c = NewClient(m.store, ns).WithLimits(m.cfg.Limits)
		m.clients[ns] = c
	}
	return c
}

func (m *Maintainer) namespaces(ctx context.Context) ([]string, error) {
	if len(m.cfg.Namespaces) > 0 {
		return m.cfg.Namespaces, nil
	}
	all, err := Namespaces(ctx, m.store)
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(all)), nil
}

// Run does a Tick for every namespace each Interval until ctx ends. With
// callbacks enabled it also runs their delivery workers.
func (m *Maintainer) Run(ctx context.Context) error {
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	defer m.stopHooks()
	for {
		nss, err := m.namespaces(ctx)
		if err != nil && ctx.Err() == nil {
			m.log.Error("runnel: listing namespaces failed", "err", err)
		} else if err == nil && len(m.cfg.Namespaces) == 0 {
			m.dropNamespaces(nss)
		}
		for _, ns := range nss {
			if ctx.Err() != nil {
				break
			}
			if m.cfg.Callbacks.Enabled {
				m.ensureHookWorker(ctx, ns)
			}
			if err := m.Tick(ctx, ns); err != nil && ctx.Err() == nil {
				m.log.Error("runnel: maintenance failed", "namespace", ns, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (m *Maintainer) dropNamespaces(active []string) {
	live := make(map[string]bool, len(active))
	for _, ns := range active {
		live[ns] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for ns, cancel := range m.hooks {
		if !live[ns] {
			cancel()
			delete(m.hooks, ns)
		}
	}
	for ns := range m.clients {
		if !live[ns] {
			delete(m.clients, ns)
		}
	}
	for ns := range m.hookCli {
		if !live[ns] {
			delete(m.hookCli, ns)
		}
	}
	for ns := range m.lastPrune {
		if !live[ns] {
			delete(m.lastPrune, ns)
		}
	}
}

// Tick does one round of housekeeping for one namespace.
func (m *Maintainer) Tick(ctx context.Context, ns string) error {
	now := m.clock.Now()
	steps := []func(context.Context, string, time.Time) error{m.reap, m.startWaits, m.deadlines, m.schedules}
	if m.cfg.Callbacks.Enabled {
		steps = append(steps, m.callbacks)
	}
	var first error
	for _, step := range steps {
		if err := step(ctx, ns, now); err != nil && first == nil {
			first = err
		}
	}
	m.mu.Lock()
	due := now.Sub(m.lastPrune[ns]) >= m.cfg.PruneEvery
	if due {
		m.lastPrune[ns] = now
	}
	m.mu.Unlock()
	if due {
		if err := m.prune(ctx, ns, now); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Maintainer) reap(ctx context.Context, ns string, now time.Time) error {
	jobs, err := m.store.Due(ctx, ns, DueLease, now, m.cfg.Batch)
	if err != nil || len(jobs) == 0 {
		return err
	}
	changes := make([]Change, 0, len(jobs))
	for _, j := range jobs {
		o := Outcome{Kind: OutcomeRetry, Error: "lease expired on worker " + j.Worker}
		if j.Polling() {
			o = Outcome{Kind: OutcomeRelease}
		}
		changes = append(changes, Change{Job: Decide(j, o, now), From: StateRunning, LeaseBefore: now})
	}
	_, err = m.store.Apply(ctx, ns, changes, now)
	return err
}

func (m *Maintainer) startWaits(ctx context.Context, ns string, now time.Time) error {
	jobs, err := m.store.Due(ctx, ns, DueStart, now, m.cfg.Batch)
	if err != nil || len(jobs) == 0 {
		return err
	}
	changes := make([]Change, 0, len(jobs))
	for _, j := range jobs {
		if j.State != StateWaiting {
			continue
		}
		n := j.Clone()
		n.addError("not started within "+j.StartTimeout.String(), now)
		n.UpdatedAt = now
		end(n, StateExpired, now)
		changes = append(changes, Change{Job: n, From: StateWaiting})
	}
	if len(changes) == 0 {
		return nil
	}
	_, err = m.store.Apply(ctx, ns, changes, now)
	return err
}

func (m *Maintainer) deadlines(ctx context.Context, ns string, now time.Time) error {
	jobs, err := m.store.Due(ctx, ns, DueDeadline, now, m.cfg.Batch)
	if err != nil || len(jobs) == 0 {
		return err
	}
	changes := make([]Change, 0, len(jobs))
	for _, j := range jobs {
		if j.State.Final() || !j.late(now) {
			continue
		}
		n := j.Clone()
		n.addError(deadlinePassed, now)
		n.Pending, n.Worker, n.LeaseUntil, n.UpdatedAt = 0, "", time.Time{}, now
		end(n, StateExpired, now)
		changes = append(changes, Change{Job: n, From: j.State})
	}
	if len(changes) == 0 {
		return nil
	}
	_, err = m.store.Apply(ctx, ns, changes, now)
	return err
}

func pick(override, fallback time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	return fallback
}
