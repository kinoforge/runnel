package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

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
	query := `UPDATE rn_jobs SET queue=?, kind=?, state=?, priority=?, grp=?, pool=?, attempt=?, max_attempts=?,
		run_at=?, waiting_since=?, start_by=?, start_timeout=?, deadline=?, worker=?, lease_until=?, version=version+1, updated_at=?, started_at=?,
		finished_at=?, data=? WHERE ns=? AND id=? AND version=?`
	args := []any{j.Queue, j.Kind, string(j.State), j.Priority, j.Group, j.Pool, j.Attempt, j.MaxAttempts,
		runnel.Millis(j.RunAt), runnel.Millis(j.WaitingSince), runnel.Millis(j.StartBy), j.StartTimeout.D().Milliseconds(),
		runnel.Millis(j.Deadline), j.Worker, runnel.Millis(j.LeaseUntil), runnel.Millis(j.UpdatedAt), runnel.Millis(j.StartedAt), runnel.Millis(j.FinishedAt),
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
			start_by=CASE WHEN start_timeout > 0 THEN ? + start_timeout ELSE 0 END WHERE state=? AND pending<=0 AND id IN (`+in+")"),
			strArgs([]any{string(runnel.StateWaiting), now, now, now, now, blocked}, kids)...)
		tx.wake = true
		return err
	}
	victims, err := collect(tx.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE state=? AND id IN ("+in+")"), strArgs([]any{blocked}, kids)...))
	if err != nil {
		return err
	}
	for _, v := range victims {
		res, err := tx.ExecContext(ctx, s.q("UPDATE rn_jobs SET state=?, pending=0, finished_at=?, updated_at=?, start_by=0, version=version+1 WHERE id=? AND state=?"),
			string(runnel.StateCancelled), now, now, v.ID, blocked)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			continue
		}
		if err := s.finalize(ctx, tx, ns, v.ID, v.Queue, v.Kind, runnel.StateCancelled, v.UniqueKey, now); err != nil {
			return err
		}
	}
	return nil
}
