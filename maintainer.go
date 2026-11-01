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
// how long events wait for webhook delivery before they are dropped and logged.
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
	Webhooks   WebhookConfig
	Logger     *slog.Logger
}

// Maintainer does the housekeeping of a store: it returns jobs whose worker
// died, moves jobs past their start deadline to their fallback, fires schedules,
// turns events into webhook jobs and prunes old jobs and events. Run as many as
// you like; their work does not overlap.
type Maintainer struct {
	store Store
	cfg   MaintainerConfig
	log   *slog.Logger
	clock Clock

	mu        sync.Mutex
	clients   map[string]*Client
	lastPrune map[string]time.Time
	specs     map[string]cron.Schedule
	hooks     map[string]context.CancelFunc
}

// NewMaintainer returns a maintainer for s.
func NewMaintainer(s Store, cfg MaintainerConfig) *Maintainer {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultMaintainInterval
	}
	if cfg.PruneEvery <= 0 {
		cfg.PruneEvery = defaultPruneEvery
	}
	if cfg.Retention.Completed == 0 && cfg.Retention.Cancelled == 0 && cfg.Retention.Dead == 0 {
		cfg.Retention.Completed, cfg.Retention.Cancelled, cfg.Retention.Dead = DefaultRetention.Completed, DefaultRetention.Cancelled, DefaultRetention.Dead
	}
	if cfg.Retention.Events <= 0 {
		cfg.Retention.Events = DefaultRetention.Events
	}
	if cfg.Retention.Undelivered <= 0 {
		cfg.Retention.Undelivered = DefaultRetention.Undelivered
	}
	cfg.Retention.Undelivered = max(cfg.Retention.Undelivered, cfg.Retention.Events)
	if cfg.Batch <= 0 {
		cfg.Batch = defaultMaintainBatch
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cfg.Webhooks = cfg.Webhooks.withDefaults()
	return &Maintainer{store: s, cfg: cfg, log: cfg.Logger, clock: ClockOf(s), clients: map[string]*Client{}, lastPrune: map[string]time.Time{},
		specs: map[string]cron.Schedule{}, hooks: map[string]context.CancelFunc{}}
}

func (m *Maintainer) client(ns string) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.clients[ns]
	if c == nil {
		c = NewClient(m.store, ns)
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
// webhooks enabled it also runs their delivery workers.
func (m *Maintainer) Run(ctx context.Context) error {
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	defer m.stopHooks()
	for {
		nss, err := m.namespaces(ctx)
		if err != nil && ctx.Err() == nil {
			m.log.Error("runnel: listing namespaces failed", "err", err)
		}
		for _, ns := range nss {
			if ctx.Err() != nil {
				break
			}
			if m.cfg.Webhooks.Enabled {
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

func (m *Maintainer) ensureHookWorker(ctx context.Context, ns string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.hooks[ns]; ok {
		return
	}
	hctx, cancel := context.WithCancel(ctx)
	m.hooks[ns] = cancel
	w := NewWorker(m.store, ns, WorkerConfig{Queues: []string{m.cfg.Webhooks.Queue}, Concurrency: m.cfg.Webhooks.Concurrency, Logger: m.log})
	w.Handle(WebhookKind, NewWebhookHandler(m.store, m.cfg.Webhooks))
	go func() {
		if err := w.Run(hctx); err != nil {
			m.log.Error("runnel: webhook worker stopped", "namespace", ns, "err", err)
		}
	}()
}

func (m *Maintainer) stopHooks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ns, cancel := range m.hooks {
		cancel()
		delete(m.hooks, ns)
	}
}

// Tick does one round of housekeeping for one namespace.
func (m *Maintainer) Tick(ctx context.Context, ns string) error {
	now := m.clock.Now()
	steps := []func(context.Context, string, time.Time) error{m.reap, m.startWaits, m.schedules}
	if m.cfg.Webhooks.Enabled {
		steps = append(steps, m.webhooks)
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
		if j.State != StateWaiting || j.Fallback >= len(j.Fallbacks) {
			continue
		}
		n := j.Clone()
		n.addError("not started within "+j.StartWithin.String(), now)
		n.UpdatedAt = now
		moveOn(n, now)
		changes = append(changes, Change{Job: n, From: StateWaiting})
	}
	if len(changes) == 0 {
		return nil
	}
	_, err = m.store.Apply(ctx, ns, changes, now)
	return err
}

func (m *Maintainer) webhooks(ctx context.Context, ns string, now time.Time) error {
	cur, err := m.store.Cursor(ctx, ns, "webhooks")
	if err != nil {
		return err
	}
	events, err := m.store.Events(ctx, ns, cur, m.cfg.Batch)
	if err != nil || len(events) == 0 {
		return err
	}
	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.JobID)
	}
	found, err := m.store.Get(ctx, ns, ids)
	if err != nil {
		return err
	}
	byID := make(map[string]*Job, len(found))
	for _, j := range found {
		byID[j.ID] = j
	}
	var hooks []*Job
	for _, e := range events {
		j := byID[e.JobID]
		if j == nil || j.Webhook == "" || j.Queue == m.cfg.Webhooks.Queue {
			continue
		}
		hook, err := NewWebhookJob(j, "job."+string(e.State), e.Seq, m.cfg.Webhooks.Queue)
		if err != nil {
			return err
		}
		hooks = append(hooks, hook)
	}
	if len(hooks) > 0 {
		if _, err := m.client(ns).EnqueueJobs(ctx, hooks); err != nil {
			return err
		}
	}
	_, err = m.store.SwapCursor(ctx, ns, "webhooks", cur, events[len(events)-1].Seq)
	return err
}

func pick(override, fallback time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	return fallback
}
