package hybrid

import (
	"container/heap"
	"context"
	"math"
	"slices"

	"github.com/kinoforge/runnel"
)

const (
	// maxExamine bounds how many refused candidates one claim looks past.
	maxExamine = 4096
	pageSize   = 64
)

func (s *Store) Claim(_ context.Context, ns string, req runnel.ClaimRequest) ([]*runnel.Job, error) {
	if req.Limit <= 0 || len(req.Queues) == 0 {
		return nil, nil
	}
	var out []*runnel.Job
	err := s.lease(ns, func(t *tx) error {
		c := claimer{t: t, req: req, now: runnel.Millis(req.Now), buckets: map[string]*token{}, full: map[string]bool{}}
		c.lease = c.now + req.Lease.Milliseconds()
		out = c.run()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type claimer struct {
	t       *tx
	req     runnel.ClaimRequest
	now     int64
	lease   int64
	out     []*runnel.Job
	buckets map[string]*token
	full    map[string]bool
}

func (c *claimer) run() []*runnel.Job {
	sp := c.t.sp
	queues := slices.Compact(slices.Sorted(slices.Values(c.req.Queues)))
	if len(c.req.IDs) == 0 {
		c.polls(queues)
	}
	var open []string
	for _, q := range queues {
		if !sp.queueConfig(q).Paused {
			open = append(open, q)
			if len(c.req.IDs) == 0 {
				sp.promote(q, c.now)
			}
		}
	}
	if len(c.req.IDs) > 0 {
		c.byID(open)
	} else {
		c.ready(open)
	}
	for key, b := range c.buckets {
		if b.taken {
			c.t.putBucket(key, bucket{tokens: b.tokens, at: c.now})
		}
	}
	return c.out
}

// polls hands out polling jobs whose next poll is due; they hold their slot
// already, so limits do not apply.
func (c *claimer) polls(queues []string) {
	sp := c.t.sp
	for _, q := range queues {
		t := sp.poll[q]
		if t == nil {
			continue
		}
		var due []string
		t.Ascend(func(k timeKey) bool {
			if k.at > c.now || len(c.out)+len(due) >= c.req.Limit {
				return false
			}
			due = append(due, k.id)
			return true
		})
		for _, id := range due {
			j := sp.jobs[id].Clone()
			j.State, j.Worker, j.LeaseUntil = runnel.StateRunning, c.req.Worker, runnel.FromMillis(c.lease)
			c.save(j)
		}
	}
}

func (c *claimer) save(j *runnel.Job) {
	j.Version++
	j.UpdatedAt = runnel.FromMillis(c.now)
	c.t.putJob(j)
	c.out = append(c.out, j.Clone())
}

func (c *claimer) take(j *runnel.Job) {
	n := j.Clone()
	n.State, n.Worker, n.LeaseUntil = runnel.StateRunning, c.req.Worker, runnel.FromMillis(c.lease)
	n.Attempt++
	if n.StartedAt.IsZero() {
		n.StartedAt = runnel.FromMillis(c.now)
	}
	c.save(n)
}

func (c *claimer) byID(open []string) {
	sp := c.t.sp
	var cands []*runnel.Job
	for _, id := range slices.Compact(slices.Sorted(slices.Values(c.req.IDs))) {
		j := sp.jobs[id]
		if j != nil && j.State == runnel.StateWaiting && runnel.Millis(j.RunAt) <= c.now && slices.Contains(open, j.Queue) {
			cands = append(cands, j)
		}
	}
	slices.SortFunc(cands, func(a, b *runnel.Job) int {
		if lessReady(readyOf(a), readyOf(b)) {
			return -1
		}
		return 1
	})
	for _, j := range cands {
		if len(c.out) >= c.req.Limit {
			return
		}
		if c.admit(j) {
			c.take(j)
		}
	}
}

// ready claims in priority order across the open queues, merging their ready
// trees. Jobs a limit refuses are passed over, so the jobs behind them still run.
func (c *claimer) ready(open []string) {
	sp := c.t.sp
	h := &merge{}
	for _, q := range open {
		if t := sp.ready[q]; t != nil {
			if cur := (&cursor{t: t}); cur.fill() {
				h.items = append(h.items, cur)
			}
		}
	}
	heap.Init(h)
	examined := 0
	for h.Len() > 0 && len(c.out) < c.req.Limit && examined < maxExamine {
		cur := h.items[0]
		k := cur.head()
		if j := sp.jobs[k.id]; j != nil && j.State == runnel.StateWaiting {
			if c.admit(j) {
				c.take(j)
			} else {
				examined++
			}
		}
		if cur.next() {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
	}
}

// admit checks j against every running limit and rate limit, taking a token
// from each rate bucket when it passes. A refusal marks the queue, tenant or
// resource full for the rest of the claim.
func (c *claimer) admit(j *runnel.Job) bool {
	sp := c.t.sp
	qk, qtk := "q|"+j.Queue, "qt|"+j.Queue+"|"+j.Tenant
	rk, rtk := "r|"+j.Resource, "rt|"+j.Resource+"|"+j.Tenant
	if c.full[qk] || c.full[qtk] || c.full[rk] || c.full[rtk] {
		return false
	}
	q := sp.queueConfig(j.Queue)
	if over(q.MaxRunning, sp.active[qk]) {
		return c.refuse(qk)
	}
	if j.Tenant != "" && over(q.TenantMaxRunning, sp.active[qtk]) {
		return c.refuse(qtk)
	}
	if j.Resource != "" {
		r := sp.resourceConfig(j.Resource)
		if over(r.MaxRunning, sp.active[rk]) {
			return c.refuse(rk)
		}
		if j.Tenant != "" && over(r.TenantMaxRunning, sp.active[rtk]) {
			return c.refuse(rtk)
		}
	}
	var take []*token
	if q.Rate > 0 {
		b := c.bucket(qk, q.Rate, q.Burst)
		if b.tokens < 1 {
			return c.refuse(qk)
		}
		take = append(take, b)
	}
	if j.Tenant != "" && q.TenantRate > 0 {
		b := c.bucket(qtk, q.TenantRate, q.TenantBurst)
		if b.tokens < 1 {
			return c.refuse(qtk)
		}
		take = append(take, b)
	}
	for _, b := range take {
		b.tokens--
		b.taken = true
	}
	return true
}

func over(limit, n int) bool { return limit > 0 && n >= limit }

func (c *claimer) refuse(key string) bool {
	c.full[key] = true
	return false
}

type token struct {
	tokens float64
	taken  bool
}

// bucket returns the token bucket under key refilled to now. Only buckets the
// claim took from are written back.
func (c *claimer) bucket(key string, rate float64, burst int) *token {
	if b, ok := c.buckets[key]; ok {
		return b
	}
	capacity := float64(burst)
	if burst <= 0 {
		capacity = math.Max(1, math.Ceil(rate))
	}
	b := &token{tokens: capacity}
	if old, ok := c.t.sp.buckets[key]; ok {
		b.tokens = math.Min(capacity, old.tokens+float64(max(c.now-old.at, 0))*rate/1000)
	}
	c.buckets[key] = b
	return b
}

// cursor walks one ready tree a page at a time, resuming after the last key it
// returned, so claiming from the tree while walking it is safe.
type cursor struct {
	t    *readyTree
	page []readyKey
	last *readyKey
}

func (c *cursor) fill() bool {
	c.page = c.page[:0]
	visit := func(k readyKey) bool {
		if c.last != nil && !lessReady(*c.last, k) {
			return true
		}
		c.page = append(c.page, k)
		return len(c.page) < pageSize
	}
	if c.last == nil {
		c.t.Ascend(visit)
	} else {
		c.t.AscendGreaterOrEqual(*c.last, visit)
	}
	return len(c.page) > 0
}

func (c *cursor) head() readyKey { return c.page[0] }

func (c *cursor) next() bool {
	k := c.page[0]
	c.last = &k
	c.page = c.page[1:]
	return len(c.page) > 0 || c.fill()
}

type merge struct{ items []*cursor }

func (m *merge) Len() int           { return len(m.items) }
func (m *merge) Less(i, j int) bool { return lessReady(m.items[i].head(), m.items[j].head()) }
func (m *merge) Swap(i, j int)      { m.items[i], m.items[j] = m.items[j], m.items[i] }
func (m *merge) Push(x any)         { m.items = append(m.items, x.(*cursor)) }
func (m *merge) Pop() any {
	n := len(m.items)
	x := m.items[n-1]
	m.items = m.items[:n-1]
	return x
}
