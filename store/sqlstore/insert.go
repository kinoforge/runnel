package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
)

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
	b := &batchInsert{s: s, tx: tx, ns: ns, cfg: cfg, nowMs: runnel.Millis(now),
		remap: map[string]string{}, waiting: map[string]int{}}
	out := make([]runnel.Inserted, len(jobs))
	inserted := false
	for i, j := range jobs {
		res, written, err := b.insert(ctx, j)
		if err != nil {
			return nil, err
		}
		out[i], inserted = res, inserted || written
	}
	tx.wake = tx.wake || inserted
	return out, nil
}

// batchInsert holds what the jobs of one Insert call share: ids remapped to
// duplicates or rejections, and waiting counts already read.
type batchInsert struct {
	s       *Store
	tx      *txn
	ns      string
	cfg     *nsConfig
	nowMs   int64
	remap   map[string]string
	waiting map[string]int
}

type depInfo struct {
	deps    []string
	pending int
	waitOn  []string
}

func (b *batchInsert) insert(ctx context.Context, j *runnel.Job) (runnel.Inserted, bool, error) {
	d, ok, err := b.dependencies(ctx, j)
	if err != nil || !ok {
		return b.reject(j, runnel.ErrDependency), false, err
	}
	if j.UniqueKey != "" {
		holder, err := b.s.uniqueHolder(ctx, b.tx, b.ns, j.UniqueKey, b.nowMs, false)
		if err != nil || holder != "" {
			return b.duplicate(j, holder), false, err
		}
	}
	rejected, err := b.overCap(ctx, j)
	if err != nil || rejected != nil {
		return b.reject(j, rejected), false, err
	}
	if j.UniqueKey != "" {
		holder, err := b.claimUnique(ctx, j)
		if err != nil || holder != "" {
			return b.duplicate(j, holder), false, err
		}
	}
	return runnel.Inserted{ID: j.ID}, true, b.write(ctx, j, d)
}

func (b *batchInsert) reject(j *runnel.Job, err error) runnel.Inserted {
	b.remap[j.ID] = ""
	return runnel.Inserted{ID: j.ID, Err: err}
}

func (b *batchInsert) duplicate(j *runnel.Job, holder string) runnel.Inserted {
	b.remap[j.ID] = holder
	return runnel.Inserted{ID: holder, Duplicate: true}
}

func (b *batchInsert) dependencies(ctx context.Context, j *runnel.Job) (depInfo, bool, error) {
	deps, failed := remapDeps(j.DependsOn, b.remap)
	if failed {
		return depInfo{}, false, nil
	}
	d := depInfo{deps: deps}
	if len(deps) == 0 {
		return d, true, nil
	}
	var err error
	d.pending, d.waitOn, err = b.s.depState(ctx, b.tx, b.ns, deps)
	if errors.Is(err, runnel.ErrDependency) {
		return depInfo{}, false, nil
	}
	return d, err == nil, err
}

func queueKey(j *runnel.Job) string  { return "q|" + j.Queue }
func tenantKey(j *runnel.Job) string { return "t|" + j.Queue + "|" + j.Tenant }

func (b *batchInsert) overCap(ctx context.Context, j *runnel.Job) (rejected, err error) {
	q := b.cfg.queues[j.Queue]
	if q.MaxWaiting > 0 {
		n, err := b.count(ctx, queueKey(j), "SELECT COUNT(*) FROM rn_jobs WHERE ns=? AND state IN (?, ?) AND queue=?",
			b.ns, string(runnel.StateWaiting), string(runnel.StateBlocked), j.Queue)
		if err != nil || n >= q.MaxWaiting {
			return runnel.ErrQueueFull, err
		}
	}
	if q.TenantWaiting > 0 && j.Tenant != "" {
		n, err := b.count(ctx, tenantKey(j), "SELECT COUNT(*) FROM rn_jobs WHERE ns=? AND queue=? AND tenant=? AND state IN (?, ?)",
			b.ns, j.Queue, j.Tenant, string(runnel.StateWaiting), string(runnel.StateBlocked))
		if err != nil || n >= q.TenantWaiting {
			return runnel.ErrTenantFull, err
		}
	}
	return nil, nil
}

func (b *batchInsert) count(ctx context.Context, key, query string, args ...any) (int, error) {
	if n, ok := b.waiting[key]; ok {
		return n, nil
	}
	var n int
	err := b.tx.QueryRowContext(ctx, b.s.q(query), args...).Scan(&n)
	b.waiting[key] = n
	return n, err
}

func (b *batchInsert) claimUnique(ctx context.Context, j *runnel.Job) (string, error) {
	var until int64
	if j.UniqueFor > 0 {
		until = b.nowMs + j.UniqueFor.D().Milliseconds()
	}
	return b.s.claimUnique(ctx, b.tx, b.ns, j.UniqueKey, j.ID, until, b.nowMs)
}

func (b *batchInsert) write(ctx context.Context, j *runnel.Job, d depInfo) error {
	j.DependsOn, j.Pending = d.deps, d.pending
	if d.pending > 0 {
		j.State, j.StartBy = runnel.StateBlocked, time.Time{}
	} else {
		j.State = runnel.StateWaiting
	}
	if err := b.s.insertRow(ctx, b.tx, j); err != nil {
		return err
	}
	for _, key := range []string{queueKey(j), tenantKey(j)} {
		if _, ok := b.waiting[key]; ok {
			b.waiting[key]++
		}
	}
	for _, dep := range d.waitOn {
		if _, err := b.tx.ExecContext(ctx, b.s.q(b.s.d.Ignore("rn_deps", []string{"ns", "dep_id", "job_id"})), b.ns, dep, j.ID); err != nil {
			return err
		}
	}
	return nil
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
	_, err = tx.ExecContext(ctx, s.q("INSERT INTO rn_jobs ("+jobCols+") VALUES ("+placeholders(jobColCount)+")"),
		j.ID, j.Namespace, j.Queue, j.Kind, string(j.State), j.Priority, j.Tenant, j.Pool, j.Attempt, j.MaxAttempts,
		runnel.Millis(j.RunAt), runnel.Millis(j.WaitingSince), runnel.Millis(j.StartBy), j.StartWindow().Milliseconds(),
		j.Worker, runnel.Millis(j.LeaseUntil), j.Version, j.Pending, j.UniqueKey,
		runnel.Millis(j.CreatedAt), runnel.Millis(j.UpdatedAt), runnel.Millis(j.StartedAt), runnel.Millis(j.FinishedAt), data)
	return err
}
