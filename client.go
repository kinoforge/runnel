package runnel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// DefaultMaxAttempts is the attempt cap of jobs whose queue and options set
// none.
const DefaultMaxAttempts = 5

const maxBatch = 1000

// Client enqueues and manages the jobs of one namespace on a store. It is safe
// for concurrent use.
type Client struct {
	store  Store
	ns     string
	limits Limits
	now    func() time.Time

	mu     sync.Mutex
	queues map[string]QueueConfig
	loaded time.Time
	hub    *endHub
}

// NewClient returns a client for namespace ns on store s.
func NewClient(s Store, ns string) *Client {
	return &Client{store: s, ns: ns, limits: DefaultLimits, now: ClockOf(s).Now}
}

// WithLimits replaces the limits jobs are validated against.
func (c *Client) WithLimits(l Limits) *Client {
	c.limits = l
	return c
}

// Namespace returns the client's namespace.
func (c *Client) Namespace() string { return c.ns }

// Store returns the client's store.
func (c *Client) Store() Store { return c.store }

func (c *Client) queueConfig(ctx context.Context, name string) (QueueConfig, error) {
	return c.queueConfigFrom(name, func() (map[string][]byte, error) { return c.store.Configs(ctx, c.ns, ConfigQueue) })
}

func (c *Client) queueConfigFrom(name string, load func() (map[string][]byte, error)) (QueueConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queues == nil || time.Since(c.loaded) > 5*time.Second {
		raw, err := load()
		if err != nil {
			return QueueConfig{}, err
		}
		q := make(map[string]QueueConfig, len(raw))
		for n, b := range raw {
			var v QueueConfig
			if err := json.Unmarshal(b, &v); err != nil {
				return QueueConfig{}, fmt.Errorf("queue config %q: %w", n, err)
			}
			q[n] = v
		}
		c.queues, c.loaded = q, time.Now()
	}
	return c.queues[name], nil
}

func (c *Client) forget() {
	c.mu.Lock()
	c.queues = nil
	c.mu.Unlock()
}

func (c *Client) prepare(ctx context.Context, j *Job, now time.Time) error {
	if !ValidNamespace(c.ns) {
		return invalid("namespace %q is not valid", c.ns)
	}
	cfg, err := c.queueConfig(ctx, j.Queue)
	if err != nil {
		return err
	}
	j.Namespace = c.ns
	if j.MaxAttempts == 0 {
		j.MaxAttempts = cfg.MaxAttempts
	}
	if j.Backoff.IsZero() {
		j.Backoff = cfg.Backoff
	}
	if j.Timeout == 0 {
		j.Timeout = cfg.Timeout
	}
	if j.StartWithin == 0 {
		j.StartWithin = cfg.StartWithin
	}
	return j.Prepare(now, c.limits)
}

// Enqueue adds one job and returns its id. The queue's settings fill the options
// the job does not set.
func (c *Client) Enqueue(ctx context.Context, queue, kind string, payload any, opts ...Option) (Inserted, error) {
	j, err := NewJob(queue, kind, payload, opts...)
	if err != nil {
		return Inserted{}, err
	}
	res, err := c.EnqueueJobs(ctx, []*Job{j})
	if err != nil {
		return Inserted{}, err
	}
	return res[0], res[0].Err
}

// EnqueueJobs adds up to 1000 jobs in one atomic call and returns one result per
// job.
func (c *Client) EnqueueJobs(ctx context.Context, jobs []*Job) ([]Inserted, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if len(jobs) > maxBatch {
		return nil, invalid("at most %d jobs per call", maxBatch)
	}
	now, err := c.prepareAll(ctx, jobs)
	if err != nil {
		return nil, err
	}
	return c.store.Insert(ctx, c.ns, jobs, now)
}

func (c *Client) prepareAll(ctx context.Context, jobs []*Job) (time.Time, error) {
	now := c.now().Truncate(time.Millisecond)
	for _, j := range jobs {
		if err := c.prepare(ctx, j, now); err != nil {
			return time.Time{}, err
		}
	}
	return now, nil
}

// EnqueueTx enqueues jobs inside tx, a transaction on the database that holds
// the store's tables, so the jobs exist only if tx commits. It needs a SQL
// store.
func (c *Client) EnqueueTx(ctx context.Context, tx *sql.Tx, jobs ...*Job) ([]Inserted, error) {
	ti, ok := c.store.(TxInserter)
	if !ok {
		return nil, invalid("store cannot enqueue inside a transaction")
	}
	if len(jobs) == 0 || len(jobs) > maxBatch {
		return nil, invalid("send 1 to %d jobs", maxBatch)
	}
	if _, err := c.queueConfigFrom("", func() (map[string][]byte, error) { return ti.ConfigsTx(ctx, tx, c.ns, ConfigQueue) }); err != nil {
		return nil, err
	}
	now, err := c.prepareAll(ctx, jobs)
	if err != nil {
		return nil, err
	}
	return ti.InsertTx(ctx, tx, c.ns, jobs, now)
}

// Get returns a job, or ErrNotFound.
func (c *Client) Get(ctx context.Context, id string) (*Job, error) {
	if !ValidID(id) {
		return nil, ErrNotFound
	}
	jobs, err := c.store.Get(ctx, c.ns, []string{id})
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, ErrNotFound
	}
	return jobs[0], nil
}

// List returns a page of jobs, newest first, and the cursor of the next page.
func (c *Client) List(ctx context.Context, q Query) ([]*Job, string, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	q.Limit = min(q.Limit, 500)
	if q.State != "" && !q.State.Valid() {
		return nil, "", invalid("unknown state %q", q.State)
	}
	if q.Cursor != "" && !ValidID(q.Cursor) {
		return nil, "", invalid("bad cursor")
	}
	return c.store.List(ctx, c.ns, q)
}

func (c *Client) mutate(ctx context.Context, id string, fn func(j *Job, now time.Time) (*Job, error)) (*Job, error) {
	for range 8 {
		j, err := c.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		now := c.now()
		n, err := fn(j, now)
		if err != nil || n == nil {
			return j, err
		}
		ok, err := c.store.Apply(ctx, c.ns, []Change{{Job: n, From: j.State}}, now)
		if err != nil {
			return nil, err
		}
		if ok[0] {
			return n, nil
		}
	}
	return nil, ErrConflict
}

// Cancel cancels a job that has not finished. A running job learns it at its
// next heartbeat; jobs that depend on it are cancelled too.
func (c *Client) Cancel(ctx context.Context, id string) (*Job, error) {
	return c.mutate(ctx, id, func(j *Job, now time.Time) (*Job, error) {
		if j.State == StateCancelled {
			return nil, nil
		}
		if j.State.Final() {
			return nil, ErrFinal
		}
		n := j.Clone()
		n.State, n.FinishedAt, n.UpdatedAt, n.Worker, n.LeaseUntil, n.StartBy = StateCancelled, now, now, "", time.Time{}, time.Time{}
		return n, nil
	})
}

// Retry runs a dead or cancelled job again from its first attempt.
func (c *Client) Retry(ctx context.Context, id string) (*Job, error) {
	return c.mutate(ctx, id, func(j *Job, now time.Time) (*Job, error) {
		if j.State != StateDead && j.State != StateCancelled {
			return nil, ErrNotFinal
		}
		if j.Pending > 0 {
			return nil, ErrDependency
		}
		n := j.Clone()
		n.State, n.Attempt, n.RunAt, n.WaitingSince, n.UpdatedAt = StateWaiting, 0, now, now, now
		n.FinishedAt, n.Result, n.Handle = time.Time{}, nil, ""
		n.computeStartBy()
		return n, nil
	})
}

// Stats counts the namespace's jobs per queue and state.
func (c *Client) Stats(ctx context.Context) ([]QueueStats, error) { return c.store.Stats(ctx, c.ns) }

// Events returns up to limit final-state events after the sequence number after.
func (c *Client) Events(ctx context.Context, after int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	return c.store.Events(ctx, c.ns, after, limit)
}

// SetQueue stores a queue's settings. Other processes see them within a few
// seconds.
func (c *Client) SetQueue(ctx context.Context, q QueueConfig) error {
	if err := q.Validate(); err != nil {
		return err
	}
	defer c.forget()
	return PutTyped(ctx, c.store, c.ns, ConfigQueue, q.Name, q)
}

// Queues returns every queue's settings.
func (c *Client) Queues(ctx context.Context) (map[string]QueueConfig, error) {
	return LoadTyped[QueueConfig](ctx, c.store, c.ns, ConfigQueue)
}

// DeleteQueue removes a queue's settings; its jobs stay.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	defer c.forget()
	return c.store.DeleteConfig(ctx, c.ns, ConfigQueue, name)
}

// SetPool stores a pool.
func (c *Client) SetPool(ctx context.Context, p PoolConfig) error {
	if err := p.Validate(); err != nil {
		return err
	}
	return PutTyped(ctx, c.store, c.ns, ConfigPool, p.Name, p)
}

// Pools returns every pool.
func (c *Client) Pools(ctx context.Context) (map[string]PoolConfig, error) {
	return LoadTyped[PoolConfig](ctx, c.store, c.ns, ConfigPool)
}

// DeletePool removes a pool.
func (c *Client) DeletePool(ctx context.Context, name string) error {
	return c.store.DeleteConfig(ctx, c.ns, ConfigPool, name)
}

// SetSchedule stores a schedule.
func (c *Client) SetSchedule(ctx context.Context, s Schedule) error {
	if err := s.Validate(); err != nil {
		return err
	}
	return PutTyped(ctx, c.store, c.ns, ConfigSchedule, s.Name, s)
}

// Schedules returns every schedule.
func (c *Client) Schedules(ctx context.Context) (map[string]Schedule, error) {
	return LoadTyped[Schedule](ctx, c.store, c.ns, ConfigSchedule)
}

// DeleteSchedule removes a schedule.
func (c *Client) DeleteSchedule(ctx context.Context, name string) error {
	return c.store.DeleteConfig(ctx, c.ns, ConfigSchedule, name)
}

// Flow is a set of jobs with dependencies between them, enqueued together. A
// step runs once the steps it waits for complete, and reads their results
// through Dependencies.
type Flow struct {
	steps []flowStep
	names map[string]int
	err   error
}

type flowStep struct {
	name  string
	job   *Job
	after []string
}

// NewFlow returns an empty flow.
func NewFlow() *Flow { return &Flow{names: map[string]int{}} }

// Add adds a step named name that waits for the steps in after. Errors are
// reported by EnqueueFlow.
func (f *Flow) Add(name string, j *Job, after ...string) *Flow {
	if f.err != nil {
		return f
	}
	if !ValidName(name) {
		f.err = invalid("step name %q is not valid", name)
		return f
	}
	if _, dup := f.names[name]; dup {
		f.err = invalid("step %q added twice", name)
		return f
	}
	if j == nil {
		f.err = invalid("step %q has no job", name)
		return f
	}
	f.names[name] = len(f.steps)
	f.steps = append(f.steps, flowStep{name: name, job: j, after: after})
	return f
}

func (f *Flow) order() ([]int, error) {
	if f.err != nil {
		return nil, f.err
	}
	if len(f.steps) == 0 || len(f.steps) > maxBatch {
		return nil, invalid("a flow needs 1 to %d steps", maxBatch)
	}
	indeg := make([]int, len(f.steps))
	next := make([][]int, len(f.steps))
	for i, s := range f.steps {
		for _, a := range s.after {
			p, ok := f.names[a]
			if !ok {
				return nil, invalid("step %q waits on unknown step %q", s.name, a)
			}
			indeg[i]++
			next[p] = append(next[p], i)
		}
	}
	var queue, out []int
	for i, d := range indeg {
		if d == 0 {
			queue = append(queue, i)
		}
	}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		out = append(out, i)
		for _, n := range next[i] {
			if indeg[n]--; indeg[n] == 0 {
				queue = append(queue, n)
			}
		}
	}
	if len(out) != len(f.steps) {
		return nil, invalid("flow has a cycle")
	}
	return out, nil
}

// EnqueueFlow enqueues every step of a flow in one atomic call and returns each
// step's result by name. It refuses unknown steps and cycles.
func (c *Client) EnqueueFlow(ctx context.Context, f *Flow) (map[string]Inserted, error) {
	order, err := f.order()
	if err != nil {
		return nil, err
	}
	ids := make(map[string]string, len(f.steps))
	for _, s := range f.steps {
		s.job.ID = NewID()
		ids[s.name] = s.job.ID
	}
	jobs := make([]*Job, 0, len(order))
	for _, i := range order {
		s := f.steps[i]
		for _, a := range s.after {
			s.job.DependsOn = append(s.job.DependsOn, ids[a])
		}
		jobs = append(jobs, s.job)
	}
	res, err := c.EnqueueJobs(ctx, jobs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Inserted, len(res))
	for k, i := range order {
		out[f.steps[i].name] = res[k]
	}
	return out, nil
}

// String names the client and its namespace.
func (c *Client) String() string { return fmt.Sprintf("runnel.Client(%s)", c.ns) }
