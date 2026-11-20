package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kinoforge/runnel"
)

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
		jobs, err := collect(tx.QueryContext(ctx, s.q("SELECT "+jobCols+" FROM rn_jobs WHERE "+where+" ORDER BY finished_at LIMIT ?"+s.d.skipLocked()), append(args, req.Limit)...))
		if err != nil || len(jobs) == 0 {
			n = 0
			return err
		}
		ids, err := s.pruneJobs(ctx, tx, ns, jobs, before, req.Archive)
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			in := placeholders(len(ids))
			if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_deps WHERE job_id IN ("+in+")"), strArgs(nil, ids)...); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_uniques WHERE ns=? AND until_at>0 AND until_at<?"), ns, before); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}

func (s *Store) pruneJobs(ctx context.Context, tx *txn, ns string, jobs []*runnel.Job, before int64, archive bool) ([]string, error) {
	ids := make([]string, 0, len(jobs))
	for _, j := range jobs {
		res, err := tx.ExecContext(ctx, s.q("DELETE FROM rn_jobs WHERE ns=? AND id=? AND state=? AND version=? AND finished_at>0 AND finished_at<?"),
			ns, j.ID, string(j.State), j.Version, before)
		if err != nil {
			return nil, err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			continue
		}
		ids = append(ids, j.ID)
		if archive {
			if err := s.archive(ctx, tx, ns, j); err != nil {
				return nil, err
			}
		}
	}
	return ids, nil
}

func (s *Store) archive(ctx context.Context, tx *txn, ns string, j *runnel.Job) error {
	full, err := json.Marshal(j)
	if err != nil {
		return err
	}
	up := s.d.Upsert("rn_archive", []string{"id", "ns", "queue", "state", "finished_at", "data"}, []string{"id"}, []string{"state", "finished_at", "data"})
	_, err = tx.ExecContext(ctx, s.q(up), j.ID, ns, j.Queue, string(j.State), runnel.Millis(j.FinishedAt), full)
	return err
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
