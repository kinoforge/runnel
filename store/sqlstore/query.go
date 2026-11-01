package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

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
