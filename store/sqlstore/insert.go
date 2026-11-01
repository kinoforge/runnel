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
