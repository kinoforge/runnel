package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"github.com/kinoforge/runnel"
)

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

type counts map[string]int

func (c counts) add(queue, tenant, resource string, d int) {
	c["q|"+queue] += d
	if tenant != "" {
		c["qt|"+queue+"|"+tenant] += d
	}
	if resource != "" {
		c["r|"+resource] += d
		if tenant != "" {
			c["rt|"+resource+"|"+tenant] += d
		}
	}
}

func (s *Store) activeCounts(ctx context.Context, tx *txn, ns string) (counts, error) {
	rows, err := tx.QueryContext(ctx, s.q("SELECT queue, tenant, resource, COUNT(*) FROM rn_jobs WHERE ns=? AND state IN (?, ?) GROUP BY queue, tenant, resource"),
		ns, string(runnel.StateRunning), string(runnel.StatePolling))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	c := counts{}
	for rows.Next() {
		var queue, tenant, resource string
		var n int
		if err := rows.Scan(&queue, &tenant, &resource, &n); err != nil {
			return nil, err
		}
		c.add(queue, tenant, resource, n)
	}
	return c, rows.Err()
}

func admit(ctx context.Context, j cand, q runnel.QueueConfig, p runnel.ResourceConfig, act counts, lim *limiter) (verdict, error) {
	if q.MaxRunning > 0 && act["q|"+j.queue] >= q.MaxRunning {
		return queueFull, nil
	}
	if j.tenant != "" && q.TenantMaxRunning > 0 && act["qt|"+j.queue+"|"+j.tenant] >= q.TenantMaxRunning {
		return queueTenantFull, nil
	}
	if j.resource != "" {
		if p.MaxRunning > 0 && act["r|"+j.resource] >= p.MaxRunning {
			return resourceFull, nil
		}
		if j.tenant != "" && p.TenantMaxRunning > 0 && act["rt|"+j.resource+"|"+j.tenant] >= p.TenantMaxRunning {
			return resourceTenantFull, nil
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
