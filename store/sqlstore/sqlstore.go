package sqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kinoforge/runnel"
)

// Store keeps jobs in a SQL database, in the rn_ tables its migrations create.
// Claims of queues without limits take rows with SKIP LOCKED; claims under
// limits serialize per namespace so counts stay exact.
type Store struct {
	db *sql.DB
	d  Dialect

	mu      sync.Mutex
	cfgs    map[string]*nsConfig
	wakes   map[string]bool
	ends    map[string][]string
	kick    chan struct{}
	stopped chan struct{}
	closed  bool

	clockOnce sync.Once
	clock     runnel.Clock
}

type nsConfig struct {
	queues map[string]runnel.QueueConfig
	pools  map[string]runnel.PoolConfig
	at     time.Time
}

func (c *nsConfig) poolLimits() bool {
	for _, p := range c.pools {
		if p.Slots > 0 || p.TenantSlots > 0 {
			return true
		}
	}
	return false
}

func limited(q runnel.QueueConfig) bool {
	return q.Concurrency > 0 || q.TenantSlots > 0 || q.Rate > 0 || q.TenantRate > 0
}

// New returns a store over db speaking dialect d.
func New(db *sql.DB, d Dialect) *Store {
	return &Store{db: db, d: d, cfgs: map[string]*nsConfig{}, wakes: map[string]bool{}, ends: map[string][]string{}}
}

func (s *Store) configs(ctx context.Context, q querier, ns string) (*nsConfig, error) {
	s.mu.Lock()
	c := s.cfgs[ns]
	s.mu.Unlock()
	if c != nil && time.Since(c.at) < time.Second {
		return c, nil
	}
	queues, err := s.queueConfigs(ctx, q, ns)
	if err != nil {
		return nil, err
	}
	pools, err := s.poolConfigs(ctx, q, ns)
	if err != nil {
		return nil, err
	}
	c = &nsConfig{queues: queues, pools: pools, at: time.Now()}
	s.mu.Lock()
	s.cfgs[ns] = c
	s.mu.Unlock()
	return c, nil
}

func (s *Store) forget(ns string) {
	s.mu.Lock()
	delete(s.cfgs, ns)
	s.mu.Unlock()
}

// DB returns the connection pool, for tuning or for transactions to pass to
// InsertTx.
func (s *Store) DB() *sql.DB { return s.db }

// Dialect returns the store's dialect.
func (s *Store) Dialect() Dialect { return s.d }

func (s *Store) q(query string) string { return s.d.rebind(query) }

// Time returns the database's clock, so every process measures leases the same
// way.
// Clock returns the store's shared clock, which follows Time.
func (s *Store) Clock() runnel.Clock {
	s.clockOnce.Do(func() { s.clock = runnel.NewStoreClock(s) })
	return s.clock
}

func (s *Store) Time(ctx context.Context) (time.Time, error) {
	if s.d.NowQuery == "" {
		return time.Now().UTC(), nil
	}
	var ms int64
	if err := s.db.QueryRowContext(ctx, s.d.NowQuery).Scan(&ms); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) Migrate(ctx context.Context) error {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	unlock, err := s.d.MigrateLock(ctx, c)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := c.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS rn_migrations (version INT NOT NULL PRIMARY KEY)"+s.d.TableSuffix); err != nil {
		return err
	}
	var current sql.NullInt64
	if err := c.QueryRowContext(ctx, "SELECT MAX(version) FROM rn_migrations").Scan(&current); err != nil {
		return err
	}
	for i, stmts := range s.d.migrations() {
		v := int64(i + 1)
		if v <= current.Int64 {
			continue
		}
		for _, st := range stmts {
			if _, err := c.ExecContext(ctx, st); err != nil {
				return fmt.Errorf("runnel migration %d: %w", v, err)
			}
		}
		if _, err := c.ExecContext(ctx, s.q("INSERT INTO rn_migrations (version) VALUES (?)"), v); err != nil {
			return err
		}
	}
	return nil
}

type txn struct {
	*sql.Tx
	wake bool
	ends []string
}

func (s *Store) tx(ctx context.Context, ns string, lock bool, fn func(tx *txn) error) error {
	var err error
	for try := range 4 {
		if try > 0 {
			t := time.NewTimer(time.Duration(try*try) * 5 * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return err
			case <-t.C:
			}
		}
		var t *txn
		if t, err = s.attempt(ctx, ns, lock, fn); err == nil {
			s.notify(ns, t.wake, t.ends)
			return nil
		}
		if s.d.Retryable == nil || !s.d.Retryable(err) {
			return err
		}
	}
	return err
}

func (s *Store) attempt(ctx context.Context, ns string, lock bool, fn func(tx *txn) error) (*txn, error) {
	raw, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: s.d.Isolation})
	if err != nil {
		return nil, err
	}
	tx := &txn{Tx: raw}
	if lock {
		if err := s.lock(ctx, tx, ns); err != nil {
			_ = raw.Rollback()
			return nil, err
		}
	}
	if err := fn(tx); err != nil {
		_ = raw.Rollback()
		return nil, err
	}
	return tx, raw.Commit()
}

func (s *Store) lock(ctx context.Context, tx *txn, ns string) error {
	if !s.d.LockRows {
		return nil
	}
	sel := s.q("SELECT ns FROM rn_locks WHERE ns=? FOR UPDATE")
	var got string
	err := tx.QueryRowContext(ctx, sel, ns).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, s.q(s.d.Ignore("rn_locks", []string{"ns"})), ns); err != nil {
			return err
		}
		err = tx.QueryRowContext(ctx, sel, ns).Scan(&got)
	}
	return err
}

const jobCols = "id, ns, queue, kind, state, priority, tenant, pool, attempt, max_attempts, run_at, waiting_since, start_by, start_within, worker, lease_until, version, pending, unique_key, created_at, updated_at, started_at, finished_at, data"

type scanner interface{ Scan(dest ...any) error }

func scanJob(sc scanner) (*runnel.Job, error) {
	var j runnel.Job
	var state string
	var runAt, waiting, startBy, startWithin, lease, created, updated, started, finished int64
	var data []byte
	err := sc.Scan(&j.ID, &j.Namespace, &j.Queue, &j.Kind, &state, &j.Priority, &j.Tenant, &j.Pool, &j.Attempt, &j.MaxAttempts,
		&runAt, &waiting, &startBy, &startWithin, &j.Worker, &lease, &j.Version, &j.Pending, &j.UniqueKey,
		&created, &updated, &started, &finished, &data)
	if err != nil {
		return nil, err
	}
	j.State = runnel.State(state)
	j.RunAt, j.WaitingSince, j.StartBy, j.LeaseUntil = runnel.FromMillis(runAt), runnel.FromMillis(waiting), runnel.FromMillis(startBy), runnel.FromMillis(lease)
	j.CreatedAt, j.UpdatedAt, j.StartedAt, j.FinishedAt = runnel.FromMillis(created), runnel.FromMillis(updated), runnel.FromMillis(started), runnel.FromMillis(finished)
	if err := j.DecodeData(data); err != nil {
		return nil, err
	}
	return &j, nil
}

func collect(rows *sql.Rows, err error) ([]*runnel.Job, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*runnel.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func strArgs(prefix []any, items []string) []any {
	out := slices.Clone(prefix)
	for _, s := range items {
		out = append(out, s)
	}
	return out
}

func (s *Store) Insert(ctx context.Context, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	cfg, err := s.configs(ctx, s.db, ns)
	if err != nil {
		return nil, err
	}
	var out []runnel.Inserted
	err = s.tx(ctx, ns, capped(cfg, jobs) && s.d.LockRows, func(tx *txn) error {
		var err error
		out, err = s.insertWith(ctx, tx, ns, cfg, jobs, now)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InsertTx inserts jobs inside a transaction the caller owns, so the jobs
// exist only if that transaction commits. The transaction must run on the
// database that holds the Runnel tables. When the queue has a waiting cap the
// namespace lock is held until the caller commits or rolls back.
func (s *Store) InsertTx(ctx context.Context, raw *sql.Tx, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	cfg, err := s.configs(ctx, raw, ns)
	if err != nil {
		return nil, err
	}
	tx := &txn{Tx: raw}
	if capped(cfg, jobs) && s.d.LockRows {
		if err := s.lock(ctx, tx, ns); err != nil {
			return nil, err
		}
	}
	out, err := s.insertWith(ctx, tx, ns, cfg, jobs, now)
	if err != nil {
		return nil, err
	}
	if tx.wake && s.d.NotifyTx != nil {
		if err := s.d.NotifyTx(ctx, raw, ns); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func capped(cfg *nsConfig, jobs []*runnel.Job) bool {
	for _, j := range jobs {
		q := cfg.queues[j.Queue]
		if q.MaxWaiting > 0 || (q.TenantWaiting > 0 && j.Tenant != "") {
			return true
		}
	}
	return false
}

func (s *Store) insertWith(ctx context.Context, tx *txn, ns string, cfg *nsConfig, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	out := make([]runnel.Inserted, len(jobs))
	nowMs := runnel.Millis(now)
	err := func() error {
		remap := map[string]string{}
		inserted := false
		waiting := map[string]int{}
		count := func(key string, query string, args ...any) (int, error) {
			if n, ok := waiting[key]; ok {
				return n, nil
			}
			var n int
			err := tx.QueryRowContext(ctx, s.q(query), args...).Scan(&n)
			waiting[key] = n
			return n, err
		}
		for i, j := range jobs {
			out[i] = runnel.Inserted{ID: j.ID}
			deps, failed := remapDeps(j.DependsOn, remap)
			pending, waitOn := 0, []string(nil)
			if !failed && len(deps) > 0 {
				var err error
				pending, waitOn, err = s.depState(ctx, tx, ns, deps)
				if errors.Is(err, runnel.ErrDependency) {
					failed = true
				} else if err != nil {
					return err
				}
			}
			if failed {
				out[i].Err, remap[j.ID] = runnel.ErrDependency, ""
				continue
			}
			if j.UniqueKey != "" {
				holder, err := s.uniqueHolder(ctx, tx, ns, j.UniqueKey, nowMs, false)
				if err != nil {
					return err
				}
				if holder != "" {
					out[i], remap[j.ID] = runnel.Inserted{ID: holder, Duplicate: true}, holder
					continue
				}
			}
			q := cfg.queues[j.Queue]
			if q.MaxWaiting > 0 {
				n, err := count("q|"+j.Queue, "SELECT COUNT(*) FROM rn_jobs WHERE ns=? AND state IN (?, ?) AND queue=?",
					ns, string(runnel.StateWaiting), string(runnel.StateBlocked), j.Queue)
				if err != nil {
					return err
				}
				if n >= q.MaxWaiting {
					out[i].Err, remap[j.ID] = runnel.ErrQueueFull, ""
					continue
				}
			}
			if q.TenantWaiting > 0 && j.Tenant != "" {
				n, err := count("t|"+j.Queue+"|"+j.Tenant, "SELECT COUNT(*) FROM rn_jobs WHERE ns=? AND queue=? AND tenant=? AND state IN (?, ?)",
					ns, j.Queue, j.Tenant, string(runnel.StateWaiting), string(runnel.StateBlocked))
				if err != nil {
					return err
				}
				if n >= q.TenantWaiting {
					out[i].Err, remap[j.ID] = runnel.ErrTenantFull, ""
					continue
				}
			}
			if j.UniqueKey != "" {
				var until int64
				if j.UniqueFor > 0 {
					until = nowMs + j.UniqueFor.D().Milliseconds()
				}
				holder, err := s.claimUnique(ctx, tx, ns, j.UniqueKey, j.ID, until, nowMs)
				if err != nil {
					return err
				}
				if holder != "" {
					out[i], remap[j.ID] = runnel.Inserted{ID: holder, Duplicate: true}, holder
					continue
				}
			}
			j.DependsOn, j.Pending = deps, pending
			if pending > 0 {
				j.State, j.StartBy = runnel.StateBlocked, time.Time{}
			} else {
				j.State = runnel.StateWaiting
			}
			if err := s.insertRow(ctx, tx, j); err != nil {
				return err
			}
			if _, ok := waiting["q|"+j.Queue]; ok {
				waiting["q|"+j.Queue]++
			}
			if _, ok := waiting["t|"+j.Queue+"|"+j.Tenant]; ok {
				waiting["t|"+j.Queue+"|"+j.Tenant]++
			}
			for _, d := range waitOn {
				if _, err := tx.ExecContext(ctx, s.q(s.d.Ignore("rn_deps", []string{"ns", "dep_id", "job_id"})), ns, d, j.ID); err != nil {
					return err
				}
			}
			inserted = true
		}
		tx.wake = tx.wake || inserted
		return nil
	}()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) claimUnique(ctx context.Context, tx *txn, ns, key, id string, until, now int64) (string, error) {
	res, err := tx.ExecContext(ctx, s.q(s.d.Ignore("rn_uniques", []string{"ns", "ukey", "job_id", "until_at"})), ns, key, id, until)
	if err != nil {
		return "", err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return "", nil
	}
	holder, err := s.uniqueHolder(ctx, tx, ns, key, now, true)
	if err != nil || holder != "" {
		return holder, err
	}
	_, err = tx.ExecContext(ctx, s.q("UPDATE rn_uniques SET job_id=?, until_at=? WHERE ns=? AND ukey=?"), id, until, ns, key)
	return "", err
}

func remapDeps(deps []string, remap map[string]string) ([]string, bool) {
	out := make([]string, 0, len(deps))
	for _, d := range deps {
		if m, ok := remap[d]; ok {
			if m == "" {
				return nil, true
			}
			d = m
		}
		if !slices.Contains(out, d) {
			out = append(out, d)
		}
	}
	return out, false
}

func (s *Store) depState(ctx context.Context, tx *txn, ns string, deps []string) (int, []string, error) {
	rows, err := tx.QueryContext(ctx, s.q("SELECT id, state FROM rn_jobs WHERE ns=? AND id IN ("+placeholders(len(deps))+") ORDER BY id"+s.d.forShare()), strArgs([]any{ns}, deps)...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	states := map[string]runnel.State{}
	for rows.Next() {
		var id, st string
		if err := rows.Scan(&id, &st); err != nil {
			return 0, nil, err
		}
		states[id] = runnel.State(st)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	var waitOn []string
	for _, d := range deps {
		st, ok := states[d]
		switch {
		case !ok || st == runnel.StateDead || st == runnel.StateCancelled:
			return 0, nil, runnel.ErrDependency
		case st != runnel.StateCompleted:
			waitOn = append(waitOn, d)
		}
	}
	return len(waitOn), waitOn, nil
}

func (s *Store) uniqueHolder(ctx context.Context, tx *txn, ns, key string, now int64, lock bool) (string, error) {
	var id string
	var until int64
	suffix := ""
	if lock {
		suffix = s.d.forUpdate()
	}
	err := tx.QueryRowContext(ctx, s.q("SELECT job_id, until_at FROM rn_uniques WHERE ns=? AND ukey=?"+suffix), ns, key).Scan(&id, &until)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if until > 0 {
		if until > now {
			return id, nil
		}
		return "", nil
	}
	var st string
	err = tx.QueryRowContext(ctx, s.q("SELECT state FROM rn_jobs WHERE id=?"), id).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if runnel.State(st).Final() {
		return "", nil
	}
	return id, nil
}

func (s *Store) insertRow(ctx context.Context, tx *txn, j *runnel.Job) error {
	data, err := j.EncodeData()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, s.q("INSERT INTO rn_jobs ("+jobCols+") VALUES ("+placeholders(24)+")"),
		j.ID, j.Namespace, j.Queue, j.Kind, string(j.State), j.Priority, j.Tenant, j.Pool, j.Attempt, j.MaxAttempts,
		runnel.Millis(j.RunAt), runnel.Millis(j.WaitingSince), runnel.Millis(j.StartBy), j.StartWindow().Milliseconds(),
		j.Worker, runnel.Millis(j.LeaseUntil), j.Version, j.Pending, j.UniqueKey,
		runnel.Millis(j.CreatedAt), runnel.Millis(j.UpdatedAt), runnel.Millis(j.StartedAt), runnel.Millis(j.FinishedAt), data)
	return err
}

func (s *Store) queueConfigs(ctx context.Context, q querier, ns string) (map[string]runnel.QueueConfig, error) {
	out := map[string]runnel.QueueConfig{}
	err := s.eachConfig(ctx, q, ns, runnel.ConfigQueue, func(name string, b []byte) error {
		var c runnel.QueueConfig
		if err := json.Unmarshal(b, &c); err != nil {
			return err
		}
		out[name] = c
		return nil
	})
	return out, err
}

func (s *Store) poolConfigs(ctx context.Context, q querier, ns string) (map[string]runnel.PoolConfig, error) {
	out := map[string]runnel.PoolConfig{}
	err := s.eachConfig(ctx, q, ns, runnel.ConfigPool, func(name string, b []byte) error {
		var c runnel.PoolConfig
		if err := json.Unmarshal(b, &c); err != nil {
			return err
		}
		out[name] = c
		return nil
	})
	return out, err
}

func (s *Store) eachConfig(ctx context.Context, q querier, ns string, kind runnel.ConfigKind, fn func(string, []byte) error) error {
	rows, err := q.QueryContext(ctx, s.q("SELECT name, val FROM rn_configs WHERE ns=? AND kind=?"), ns, string(kind))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var val []byte
		if err := rows.Scan(&name, &val); err != nil {
			return err
		}
		if err := fn(name, val); err != nil {
			return err
		}
	}
	return rows.Err()
}

type bucket struct {
	tokens float64
	dirty  bool
}

type limiter struct {
	s       *Store
	tx      *txn
	ns      string
	now     int64
	buckets map[string]*bucket
}

func (l *limiter) get(ctx context.Context, key string, rate float64, burst int) (*bucket, error) {
	if b, ok := l.buckets[key]; ok {
		return b, nil
	}
	capacity := float64(burst)
	if burst <= 0 {
		capacity = math.Max(1, math.Ceil(rate))
	}
	b := &bucket{tokens: capacity}
	var tokens float64
	var at int64
	err := l.tx.QueryRowContext(ctx, l.s.q("SELECT tokens, at_ms FROM rn_buckets WHERE ns=? AND bkey=?"), l.ns, key).Scan(&tokens, &at)
	switch {
	case err == nil:
		b.tokens = math.Min(capacity, tokens+float64(max(l.now-at, 0))*rate/1000)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	l.buckets[key] = b
	return b, nil
}

func (l *limiter) save(ctx context.Context) error {
	up := l.s.d.Upsert("rn_buckets", []string{"ns", "bkey", "tokens", "at_ms"}, []string{"ns", "bkey"}, []string{"tokens", "at_ms"})
	for key, b := range l.buckets {
		if !b.dirty {
			continue
		}
		if _, err := l.tx.ExecContext(ctx, l.s.q(up), l.ns, key, b.tokens, l.now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Claim(ctx context.Context, ns string, req runnel.ClaimRequest) ([]*runnel.Job, error) {
	if req.Limit <= 0 || len(req.Queues) == 0 {
		return nil, nil
	}
	cfg, err := s.configs(ctx, s.db, ns)
	if err != nil {
		return nil, err
	}
	locked := cfg.poolLimits()
	var open []string
	for _, q := range req.Queues {
		c := cfg.queues[q]
		if c.Paused || slices.Contains(open, q) {
			continue
		}
		open = append(open, q)
		locked = locked || limited(c)
	}
	t := taker{s: s, ns: ns, worker: req.Worker, now: runnel.Millis(req.Now), only: req.IDs}
	t.lease = t.now + req.Lease.Milliseconds()
	var out []*runnel.Job
	err = s.tx(ctx, ns, locked && s.d.LockRows, func(tx *txn) error {
		t.tx, out = tx, out[:0]
		if s.d.LightCommit != "" {
			if _, err := tx.ExecContext(ctx, s.d.LightCommit); err != nil {
				return err
			}
		}
		polls, err := t.scan(ctx, runnel.StatePolling, unique(req.Queues), nil, req.Limit)
		if err != nil {
			return err
		}
		got, err := t.take(ctx, polls, true)
		if err != nil {
			return err
		}
		out = append(out, got...)
		need := req.Limit - len(out)
		if need <= 0 || len(open) == 0 {
			return nil
		}
		if !locked {
			cands, err := t.scan(ctx, runnel.StateWaiting, open, nil, need)
			if err != nil {
				return err
			}
			got, err := t.take(ctx, cands, false)
			out = append(out, got...)
			return err
		}
		act, err := s.activeCounts(ctx, tx, ns)
		if err != nil {
			return err
		}
		lim := &limiter{s: s, tx: tx, ns: ns, now: t.now, buckets: map[string]*bucket{}}
		ex := &exclusions{}
		for round := 0; round < 4 && need > 0; round++ {
			queues := slices.DeleteFunc(slices.Clone(open), func(q string) bool { return ex.queues[q] })
			if len(queues) == 0 {
				break
			}
			cands, err := t.scan(ctx, runnel.StateWaiting, queues, ex, max(need*4, 32))
			if err != nil {
				return err
			}
			var pick []cand
			blocked := false
			for _, c := range cands {
				if len(pick) == need {
					break
				}
				if ex.skip(c) {
					continue
				}
				why, err := admit(ctx, c, cfg.queues[c.queue], cfg.pools[c.pool], act, lim)
				if err != nil {
					return err
				}
				if why != admitted {
					ex.add(why, c)
					blocked = true
					continue
				}
				act.add(c.queue, c.tenant, c.pool, 1)
				pick = append(pick, c)
			}
			got, err := t.take(ctx, pick, false)
			if err != nil {
				return err
			}
			out = append(out, got...)
			need -= len(got)
			if !blocked {
				break
			}
		}
		return lim.save(ctx)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func unique(list []string) []string {
	out := make([]string, 0, len(list))
	for _, v := range list {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}

type cand struct {
	id, queue, tenant, pool string
	priority                int
	runAt                   int64
}

type verdict int

const (
	admitted verdict = iota
	queueFull
	queueTenantFull
	poolFull
	poolTenantFull
)

type exclusions struct {
	queues       map[string]bool
	queueTenants map[[2]string]bool
	pools        map[string]bool
	poolTenants  map[[2]string]bool
}

func (e *exclusions) add(v verdict, c cand) {
	switch v {
	case queueFull:
		e.queues = setOf(e.queues, c.queue)
	case queueTenantFull:
		e.queueTenants = setOf(e.queueTenants, [2]string{c.queue, c.tenant})
	case poolFull:
		e.pools = setOf(e.pools, c.pool)
	case poolTenantFull:
		e.poolTenants = setOf(e.poolTenants, [2]string{c.pool, c.tenant})
	}
}

func setOf[K comparable](m map[K]bool, k K) map[K]bool {
	if m == nil {
		m = map[K]bool{}
	}
	m[k] = true
	return m
}

func (e *exclusions) skip(c cand) bool {
	return e.queues[c.queue] || e.queueTenants[[2]string{c.queue, c.tenant}] || e.pools[c.pool] || e.poolTenants[[2]string{c.pool, c.tenant}]
}

func (e *exclusions) where(queue string) (string, []any) {
	if e == nil {
		return "", nil
	}
	var sb strings.Builder
	var args []any
	for qt := range e.queueTenants {
		if qt[0] == queue {
			sb.WriteString(" AND tenant<>?")
			args = append(args, qt[1])
		}
	}
	for p := range e.pools {
		sb.WriteString(" AND pool<>?")
		args = append(args, p)
	}
	for pt := range e.poolTenants {
		sb.WriteString(" AND NOT (pool=? AND tenant=?)")
		args = append(args, pt[0], pt[1])
	}
	return sb.String(), args
}

type taker struct {
	s          *Store
	tx         *txn
	ns, worker string
	now, lease int64
	only       []string
}

func (t *taker) scan(ctx context.Context, state runnel.State, queues []string, ex *exclusions, limit int) ([]cand, error) {
	order := " ORDER BY priority DESC, run_at, id"
	if state == runnel.StatePolling {
		order = " ORDER BY run_at, id"
	}
	var all []cand
	for _, q := range queues {
		where := "ns=? AND state=? AND queue=? AND run_at<=?"
		args := []any{t.ns, string(state), q, t.now}
		if len(t.only) > 0 {
			where += " AND id IN (" + placeholders(len(t.only)) + ")"
			args = strArgs(args, t.only)
		}
		extra, more := ex.where(q)
		rows, err := t.tx.QueryContext(ctx, t.s.q("SELECT id, queue, tenant, pool, priority, run_at FROM rn_jobs WHERE "+where+extra+order+" LIMIT ?"+t.s.d.skipLocked()),
			append(append(args, more...), limit)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var c cand
			if err := rows.Scan(&c.id, &c.queue, &c.tenant, &c.pool, &c.priority, &c.runAt); err != nil {
				rows.Close()
				return nil, err
			}
			all = append(all, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	if len(queues) > 1 {
		slices.SortStableFunc(all, func(a, b cand) int {
			if state == runnel.StateWaiting && a.priority != b.priority {
				return b.priority - a.priority
			}
			if a.runAt != b.runAt {
				return cmp.Compare(a.runAt, b.runAt)
			}
			return strings.Compare(a.id, b.id)
		})
	}
	return all[:min(len(all), limit)], nil
}

func (t *taker) take(ctx context.Context, cands []cand, poll bool) ([]*runnel.Job, error) {
	if len(cands) == 0 {
		return nil, nil
	}
	ids := make([]string, len(cands))
	for i, c := range cands {
		ids[i] = c.id
	}
	in := placeholders(len(ids))
	from := runnel.StateWaiting
	set := "attempt=attempt+1, started_at=CASE WHEN started_at=0 THEN ? ELSE started_at END, "
	args := []any{t.now}
	if poll {
		from, set, args = runnel.StatePolling, "", nil
	}
	args = strArgs(append(args, string(runnel.StateRunning), t.worker, t.lease, t.now), ids)
	if _, err := t.tx.ExecContext(ctx, t.s.q("UPDATE rn_jobs SET "+set+"state=?, worker=?, lease_until=?, version=version+1, updated_at=? WHERE id IN ("+in+") AND ns=? AND state=?"),
		append(args, t.ns, string(from))...); err != nil {
		return nil, err
	}
	jobs, err := collect(t.tx.QueryContext(ctx, t.s.q("SELECT "+jobCols+" FROM rn_jobs WHERE ns=? AND state=? AND worker=? AND lease_until=? AND id IN ("+in+")"),
		strArgs([]any{t.ns, string(runnel.StateRunning), t.worker, t.lease}, ids)...))
	if err != nil {
		return nil, err
	}
	rank := make(map[string]int, len(ids))
	for i, id := range ids {
		rank[id] = i
	}
	slices.SortFunc(jobs, func(a, b *runnel.Job) int { return rank[a.ID] - rank[b.ID] })
	return jobs, nil
}

type counts map[string]int

func (c counts) add(queue, tenant, pool string, d int) {
	c["q|"+queue] += d
	if tenant != "" {
		c["qt|"+queue+"|"+tenant] += d
	}
	if pool != "" {
		c["p|"+pool] += d
		if tenant != "" {
			c["pt|"+pool+"|"+tenant] += d
		}
	}
}

func (s *Store) activeCounts(ctx context.Context, tx *txn, ns string) (counts, error) {
	rows, err := tx.QueryContext(ctx, s.q("SELECT queue, tenant, pool, COUNT(*) FROM rn_jobs WHERE ns=? AND state IN (?, ?) GROUP BY queue, tenant, pool"),
		ns, string(runnel.StateRunning), string(runnel.StatePolling))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	c := counts{}
	for rows.Next() {
		var queue, tenant, pool string
		var n int
		if err := rows.Scan(&queue, &tenant, &pool, &n); err != nil {
			return nil, err
		}
		c.add(queue, tenant, pool, n)
	}
	return c, rows.Err()
}

func admit(ctx context.Context, j cand, q runnel.QueueConfig, p runnel.PoolConfig, act counts, lim *limiter) (verdict, error) {
	if q.Concurrency > 0 && act["q|"+j.queue] >= q.Concurrency {
		return queueFull, nil
	}
	if j.tenant != "" && q.TenantSlots > 0 && act["qt|"+j.queue+"|"+j.tenant] >= q.TenantSlots {
		return queueTenantFull, nil
	}
	if j.pool != "" {
		if p.Slots > 0 && act["p|"+j.pool] >= p.Slots {
			return poolFull, nil
		}
		if j.tenant != "" && p.TenantSlots > 0 && act["pt|"+j.pool+"|"+j.tenant] >= p.TenantSlots {
			return poolTenantFull, nil
		}
	}
	var take []*bucket
	if q.Rate > 0 {
		b, err := lim.get(ctx, "q|"+j.queue, q.Rate, q.Burst)
		if err != nil {
			return 0, err
		}
		if b.tokens < 1 {
			return queueFull, nil
		}
		take = append(take, b)
	}
	if j.tenant != "" && q.TenantRate > 0 {
		b, err := lim.get(ctx, "qt|"+j.queue+"|"+j.tenant, q.TenantRate, q.TenantBurst)
		if err != nil {
			return 0, err
		}
		if b.tokens < 1 {
			return queueTenantFull, nil
		}
		take = append(take, b)
	}
	for _, b := range take {
		b.tokens--
		b.dirty = true
	}
	return admitted, nil
}

func (s *Store) Extend(ctx context.Context, ns, worker string, ids []string, until time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	in := placeholders(len(ids))
	if _, err := s.db.ExecContext(ctx, s.q("UPDATE rn_jobs SET lease_until=? WHERE ns=? AND worker=? AND state=? AND id IN ("+in+")"),
		strArgs([]any{runnel.Millis(until), ns, worker, string(runnel.StateRunning)}, ids)...); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q("SELECT id FROM rn_jobs WHERE ns=? AND worker=? AND state=? AND id IN ("+in+")"),
		strArgs([]any{ns, worker, string(runnel.StateRunning)}, ids)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	kept := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		kept[id] = true
	}
	var lost []string
	for _, id := range ids {
		if !kept[id] {
			lost = append(lost, id)
		}
	}
	return lost, rows.Err()
}

func (s *Store) Apply(ctx context.Context, ns string, changes []runnel.Change, now time.Time) ([]bool, error) {
	out := make([]bool, len(changes))
	nowMs := runnel.Millis(now)
	order := make([]int, len(changes))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return strings.Compare(changes[a].Job.ID, changes[b].Job.ID) })
	err := s.tx(ctx, ns, false, func(tx *txn) error {
		for _, i := range order {
			ok, err := s.applyOne(ctx, tx, ns, changes[i], nowMs)
			if err != nil {
				return err
			}
			out[i] = ok
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i, c := range changes {
		if out[i] {
			c.Job.Version++
		}
	}
	return out, nil
}

func (s *Store) applyOne(ctx context.Context, tx *txn, ns string, c runnel.Change, now int64) (bool, error) {
	j := c.Job
	oldState, key := string(c.From), j.UniqueKey
	if c.From == "" {
		err := tx.QueryRowContext(ctx, s.q("SELECT state, unique_key FROM rn_jobs WHERE ns=? AND id=?"), ns, j.ID).Scan(&oldState, &key)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	data, err := j.EncodeData()
	if err != nil {
		return false, err
	}
	query := `UPDATE rn_jobs SET queue=?, kind=?, state=?, priority=?, tenant=?, pool=?, attempt=?, max_attempts=?,
		run_at=?, waiting_since=?, start_by=?, start_within=?, worker=?, lease_until=?, version=version+1, updated_at=?, started_at=?,
		finished_at=?, data=? WHERE ns=? AND id=? AND version=?`
	args := []any{j.Queue, j.Kind, string(j.State), j.Priority, j.Tenant, j.Pool, j.Attempt, j.MaxAttempts,
		runnel.Millis(j.RunAt), runnel.Millis(j.WaitingSince), runnel.Millis(j.StartBy), j.StartWindow().Milliseconds(),
		j.Worker, runnel.Millis(j.LeaseUntil), runnel.Millis(j.UpdatedAt), runnel.Millis(j.StartedAt), runnel.Millis(j.FinishedAt),
		data, ns, j.ID, j.Version}
	if c.From != "" {
		query += " AND state=?"
		args = append(args, string(c.From))
	}
	if !c.LeaseBefore.IsZero() {
		query += " AND lease_until < ?"
		args = append(args, runnel.Millis(c.LeaseBefore))
	}
	res, err := tx.ExecContext(ctx, s.q(query), args...)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return false, nil
	}
	if j.State == runnel.StateWaiting {
		tx.wake = true
	}
	if j.State.Final() && !runnel.State(oldState).Final() {
		if err := s.finalize(ctx, tx, ns, j.ID, j.Queue, j.Kind, j.State, key, now); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *Store) finalize(ctx context.Context, tx *txn, ns, id, queue, kind string, state runnel.State, key string, now int64) error {
	if _, err := tx.ExecContext(ctx, s.q("INSERT INTO rn_events (ns, job_id, queue, kind, state, at_ms) VALUES (?, ?, ?, ?, ?, ?)"),
		ns, id, queue, kind, string(state), now); err != nil {
		return err
	}
	tx.ends = append(tx.ends, id)
	if key != "" {
		if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_uniques WHERE ns=? AND ukey=? AND job_id=? AND until_at=0"), ns, key, id); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, s.q("SELECT job_id FROM rn_deps WHERE dep_id=?"), id)
	if err != nil {
		return err
	}
	var kids []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return err
		}
		kids = append(kids, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(kids) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_deps WHERE dep_id=?"), id); err != nil {
		return err
	}
	in := placeholders(len(kids))
	blocked := string(runnel.StateBlocked)
	if state == runnel.StateCompleted {
		if _, err := tx.ExecContext(ctx, s.q("UPDATE rn_jobs SET pending=pending-1, version=version+1, updated_at=? WHERE state=? AND id IN ("+in+")"),
			strArgs([]any{now, blocked}, kids)...); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, s.q(`UPDATE rn_jobs SET state=?, run_at=CASE WHEN run_at > ? THEN run_at ELSE ? END, waiting_since=?,
			start_by=CASE WHEN start_within > 0 THEN ? + start_within ELSE 0 END WHERE state=? AND pending<=0 AND id IN (`+in+")"),
			strArgs([]any{string(runnel.StateWaiting), now, now, now, now, blocked}, kids)...)
		tx.wake = true
		return err
	}
	victims, err := collect(tx.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE state=? AND id IN ("+in+")"), strArgs([]any{blocked}, kids)...))
	if err != nil {
		return err
	}
	for _, v := range victims {
		if _, err := tx.ExecContext(ctx, s.q("UPDATE rn_jobs SET state=?, finished_at=?, updated_at=?, start_by=0, version=version+1 WHERE id=?"),
			string(runnel.StateCancelled), now, now, v.ID); err != nil {
			return err
		}
		if err := s.finalize(ctx, tx, ns, v.ID, v.Queue, v.Kind, runnel.StateCancelled, v.UniqueKey, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Get(ctx context.Context, ns string, ids []string) ([]*runnel.Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	found, err := collect(s.db.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE ns=? AND id IN ("+placeholders(len(ids))+")"), strArgs([]any{ns}, ids)...))
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*runnel.Job, len(found))
	for _, j := range found {
		byID[j.ID] = j
	}
	out := make([]*runnel.Job, 0, len(found))
	for _, id := range ids {
		if j := byID[id]; j != nil {
			out = append(out, j)
			delete(byID, id)
		}
	}
	return out, nil
}

func (s *Store) List(ctx context.Context, ns string, q runnel.Query) ([]*runnel.Job, string, error) {
	where := []string{"ns=?"}
	args := []any{ns}
	add := func(col, v string) {
		if v != "" {
			where = append(where, col+"=?")
			args = append(args, v)
		}
	}
	add("queue", q.Queue)
	add("kind", q.Kind)
	add("tenant", q.Tenant)
	add("state", string(q.State))
	if q.Cursor != "" {
		where = append(where, "id<?")
		args = append(args, q.Cursor)
	}
	args = append(args, q.Limit)
	jobs, err := collect(s.db.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE "+strings.Join(where, " AND ")+" ORDER BY id DESC LIMIT ?"), args...))
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(jobs) == q.Limit {
		next = jobs[len(jobs)-1].ID
	}
	return jobs, next, nil
}

func (s *Store) Due(ctx context.Context, ns string, kind runnel.DueKind, now time.Time, limit int) ([]*runnel.Job, error) {
	switch kind {
	case runnel.DueLease:
		return collect(s.db.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE ns=? AND state=? AND lease_until<? ORDER BY lease_until LIMIT ?"),
			ns, string(runnel.StateRunning), runnel.Millis(now), limit))
	case runnel.DueStart:
		return collect(s.db.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE ns=? AND state=? AND start_by>0 AND start_by<=? ORDER BY start_by LIMIT ?"),
			ns, string(runnel.StateWaiting), runnel.Millis(now), limit))
	}
	return nil, fmt.Errorf("runnel: unknown due kind %d", kind)
}

func (s *Store) Prune(ctx context.Context, ns string, req runnel.PruneRequest) (int, error) {
	if len(req.States) == 0 || req.Limit <= 0 {
		return 0, nil
	}
	var states []string
	for _, st := range req.States {
		if !st.Final() {
			return 0, fmt.Errorf("%w: only finished jobs can be pruned", runnel.ErrInvalid)
		}
		states = append(states, string(st))
	}
	before := runnel.Millis(req.Before)
	n := 0
	err := s.tx(ctx, ns, false, func(tx *txn) error {
		where := "ns=? AND state IN (" + placeholders(len(states)) + ") AND finished_at>0 AND finished_at<?"
		args := append(strArgs([]any{ns}, states), before)
		if req.Queue != "" {
			where += " AND queue=?"
			args = append(args, req.Queue)
		}
		jobs, err := collect(tx.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE "+where+" ORDER BY finished_at LIMIT ?"), append(args, req.Limit)...))
		if err != nil || len(jobs) == 0 {
			n = 0
			return err
		}
		ids := make([]string, len(jobs))
		for i, j := range jobs {
			ids[i] = j.ID
			if req.Archive {
				full, err := json.Marshal(j)
				if err != nil {
					return err
				}
				up := s.d.Upsert("rn_archive", []string{"id", "ns", "queue", "state", "finished_at", "data"}, []string{"id"}, []string{"state", "finished_at", "data"})
				if _, err := tx.ExecContext(ctx, s.q(up), j.ID, ns, j.Queue, string(j.State), runnel.Millis(j.FinishedAt), full); err != nil {
					return err
				}
			}
		}
		in := placeholders(len(ids))
		if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_jobs WHERE ns=? AND id IN ("+in+")"), strArgs([]any{ns}, ids)...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_deps WHERE job_id IN ("+in+")"), strArgs(nil, ids)...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_uniques WHERE ns=? AND until_at>0 AND until_at<?"), ns, before); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}

func (s *Store) PruneEvents(ctx context.Context, ns string, req runnel.EventPruneRequest) (int, error) {
	if req.Limit <= 0 || req.Before.IsZero() {
		return 0, nil
	}
	where, args := "ns=? AND at_ms<?", []any{ns, runnel.Millis(req.Before)}
	if req.UpTo > 0 {
		where += " AND seq<=?"
		args = append(args, req.UpTo)
	}
	rows, err := s.db.QueryContext(ctx, s.q("SELECT seq FROM rn_events WHERE "+where+" ORDER BY seq LIMIT ?"), append(args, req.Limit)...)
	if err != nil {
		return 0, err
	}
	var seqs []any
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			return 0, err
		}
		seqs = append(seqs, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(seqs) == 0 {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, s.q("DELETE FROM rn_events WHERE ns=? AND seq IN ("+placeholders(len(seqs))+")"), append([]any{ns}, seqs...)...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) Events(ctx context.Context, ns string, after int64, limit int) ([]runnel.Event, error) {
	rows, err := s.db.QueryContext(ctx, s.q("SELECT seq, job_id, queue, kind, state, at_ms FROM rn_events WHERE ns=? AND seq>? ORDER BY seq LIMIT ?"), ns, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runnel.Event
	for rows.Next() {
		e := runnel.Event{Namespace: ns}
		var st string
		var at int64
		if err := rows.Scan(&e.Seq, &e.JobID, &e.Queue, &e.Kind, &st, &at); err != nil {
			return nil, err
		}
		e.State, e.At = runnel.State(st), runnel.FromMillis(at)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Cursor(ctx context.Context, ns, name string) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx, s.q("SELECT val FROM rn_cursors WHERE ns=? AND name=?"), ns, name).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

func (s *Store) SwapCursor(ctx context.Context, ns, name string, old, next int64) (bool, error) {
	if old == next {
		return true, nil
	}
	res, err := s.db.ExecContext(ctx, s.q("UPDATE rn_cursors SET val=? WHERE ns=? AND name=? AND val=?"), next, ns, name, old)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return true, nil
	}
	if old != 0 {
		return false, nil
	}
	res, err = s.db.ExecContext(ctx, s.q(s.d.Ignore("rn_cursors", []string{"ns", "name", "val"})), ns, name, next)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) PutConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	defer s.forget(ns)
	up := s.d.Upsert("rn_configs", []string{"ns", "kind", "name", "val"}, []string{"ns", "kind", "name"}, []string{"val"})
	_, err := s.db.ExecContext(ctx, s.q(up), ns, string(kind), name, value)
	return err
}

func (s *Store) Configs(ctx context.Context, ns string, kind runnel.ConfigKind) (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.eachConfig(ctx, s.db, ns, kind, func(name string, b []byte) error {
		out[name] = b
		return nil
	})
	return out, err
}

// ConfigsTx reads configuration inside the caller's transaction.
func (s *Store) ConfigsTx(ctx context.Context, tx *sql.Tx, ns string, kind runnel.ConfigKind) (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.eachConfig(ctx, tx, ns, kind, func(name string, b []byte) error {
		out[name] = b
		return nil
	})
	return out, err
}

func (s *Store) DeleteConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string) error {
	defer s.forget(ns)
	_, err := s.db.ExecContext(ctx, s.q("DELETE FROM rn_configs WHERE ns=? AND kind=? AND name=?"), ns, string(kind), name)
	return err
}

func (s *Store) Stats(ctx context.Context, ns string) ([]runnel.QueueStats, error) {
	rows, err := s.db.QueryContext(ctx, s.q("SELECT queue, state, COUNT(*) FROM rn_jobs WHERE ns=? GROUP BY queue, state ORDER BY queue"), ns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runnel.QueueStats
	for rows.Next() {
		var q, st string
		var n int64
		if err := rows.Scan(&q, &st, &n); err != nil {
			return nil, err
		}
		if len(out) == 0 || out[len(out)-1].Queue != q {
			out = append(out, runnel.QueueStats{Queue: q, Counts: map[runnel.State]int64{}})
		}
		out[len(out)-1].Counts[runnel.State(st)] = n
	}
	return out, rows.Err()
}

var (
	_ runnel.Store      = (*Store)(nil)
	_ runnel.TimeSource = (*Store)(nil)
	_ runnel.TxInserter = (*Store)(nil)
)
