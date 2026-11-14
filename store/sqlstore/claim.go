package sqlstore

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	scanFactor     = 4
	maxClaimRounds = 4
	minScan        = 32
)

func (s *Store) Claim(ctx context.Context, ns string, req runnel.ClaimRequest) ([]*runnel.Job, error) {
	if req.Limit <= 0 || len(req.Queues) == 0 {
		return nil, nil
	}
	cfg, err := s.configs(ctx, s.db, ns)
	if err != nil {
		return nil, err
	}
	open, locked := openQueues(cfg, req.Queues)
	t := taker{s: s, ns: ns, worker: req.Worker, now: runnel.Millis(req.Now), only: req.IDs}
	t.lease = t.now + req.Lease.Milliseconds()
	var out []*runnel.Job
	err = s.tx(ctx, ns, locked && s.d.LockRows, func(tx *txn) error {
		t.tx = tx
		var err error
		out, err = t.claim(ctx, cfg, req, open, locked)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func openQueues(cfg *nsConfig, queues []string) ([]string, bool) {
	locked := cfg.poolLimits()
	var open []string
	for _, q := range queues {
		c := cfg.queues[q]
		if c.Paused || slices.Contains(open, q) {
			continue
		}
		open = append(open, q)
		locked = locked || limited(c)
	}
	return open, locked
}

func (t *taker) claim(ctx context.Context, cfg *nsConfig, req runnel.ClaimRequest, open []string, locked bool) ([]*runnel.Job, error) {
	if t.s.d.LightCommit != "" {
		if _, err := t.tx.ExecContext(ctx, t.s.d.LightCommit); err != nil {
			return nil, err
		}
	}
	polls, err := t.scan(ctx, runnel.StatePolling, unique(req.Queues), nil, req.Limit)
	if err != nil {
		return nil, err
	}
	out, err := t.take(ctx, polls, true)
	if err != nil {
		return nil, err
	}
	need := req.Limit - len(out)
	if need <= 0 || len(open) == 0 {
		return out, nil
	}
	var got []*runnel.Job
	if locked {
		got, err = t.claimLimited(ctx, cfg, open, need)
	} else {
		got, err = t.claimFree(ctx, open, need)
	}
	return append(out, got...), err
}

func (t *taker) claimFree(ctx context.Context, open []string, need int) ([]*runnel.Job, error) {
	cands, err := t.scan(ctx, runnel.StateWaiting, open, nil, need)
	if err != nil {
		return nil, err
	}
	return t.take(ctx, cands, false)
}

func (t *taker) claimLimited(ctx context.Context, cfg *nsConfig, open []string, need int) ([]*runnel.Job, error) {
	act, err := t.s.activeCounts(ctx, t.tx, t.ns)
	if err != nil {
		return nil, err
	}
	adm := admission{cfg: cfg, act: act, ex: &exclusions{},
		lim: &limiter{s: t.s, tx: t.tx, ns: t.ns, now: t.now, buckets: map[string]*bucket{}}}
	var out []*runnel.Job
	for round := 0; round < maxClaimRounds && need > 0; round++ {
		queues := slices.DeleteFunc(slices.Clone(open), func(q string) bool { return adm.ex.queues[q] })
		if len(queues) == 0 {
			break
		}
		cands, err := t.scan(ctx, runnel.StateWaiting, queues, adm.ex, max(need*scanFactor, minScan))
		if err != nil {
			return nil, err
		}
		pick, blocked, err := adm.pick(ctx, cands, need)
		if err != nil {
			return nil, err
		}
		got, err := t.take(ctx, pick, false)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
		need -= len(got)
		if !blocked {
			break
		}
	}
	return out, adm.lim.save(ctx)
}

type admission struct {
	cfg *nsConfig
	act counts
	lim *limiter
	ex  *exclusions
}

func (a *admission) pick(ctx context.Context, cands []cand, need int) ([]cand, bool, error) {
	var pick []cand
	blocked := false
	for _, c := range cands {
		if len(pick) == need {
			break
		}
		if a.ex.skip(c) {
			continue
		}
		why, err := admit(ctx, c, a.cfg.queues[c.queue], a.cfg.pools[c.pool], a.act, a.lim)
		if err != nil {
			return nil, false, err
		}
		if why != admitted {
			a.ex.add(why, c)
			blocked = true
			continue
		}
		a.act.add(c.queue, c.group, c.pool, 1)
		pick = append(pick, c)
	}
	return pick, blocked, nil
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
	id, queue, group, pool string
	priority               int
	runAt                  int64
}

type verdict int

const (
	admitted verdict = iota
	queueFull
	queueGroupFull
	poolFull
	poolGroupFull
)

type exclusions struct {
	queues      map[string]bool
	queueGroups map[[2]string]bool
	pools       map[string]bool
	poolGroups  map[[2]string]bool
}

func (e *exclusions) add(v verdict, c cand) {
	switch v {
	case queueFull:
		e.queues = setOf(e.queues, c.queue)
	case queueGroupFull:
		e.queueGroups = setOf(e.queueGroups, [2]string{c.queue, c.group})
	case poolFull:
		e.pools = setOf(e.pools, c.pool)
	case poolGroupFull:
		e.poolGroups = setOf(e.poolGroups, [2]string{c.pool, c.group})
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
	return e.queues[c.queue] || e.queueGroups[[2]string{c.queue, c.group}] || e.pools[c.pool] || e.poolGroups[[2]string{c.pool, c.group}]
}

func (e *exclusions) where(queue string) (string, []any) {
	if e == nil {
		return "", nil
	}
	var sb strings.Builder
	var args []any
	for qt := range e.queueGroups {
		if qt[0] == queue {
			sb.WriteString(" AND grp<>?")
			args = append(args, qt[1])
		}
	}
	for p := range e.pools {
		sb.WriteString(" AND pool<>?")
		args = append(args, p)
	}
	for pt := range e.poolGroups {
		sb.WriteString(" AND NOT (pool=? AND grp=?)")
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
		rows, err := t.tx.QueryContext(ctx, t.s.q("SELECT id, queue, grp, pool, priority, run_at FROM rn_jobs WHERE "+where+extra+order+" LIMIT ?"+t.s.d.skipLocked()),
			append(append(args, more...), limit)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var c cand
			if err := rows.Scan(&c.id, &c.queue, &c.group, &c.pool, &c.priority, &c.runAt); err != nil {
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
