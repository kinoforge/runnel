package hybrid

import (
	"container/heap"
	"context"
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
	if sp := s.space(ns); sp.spilled.Load() && s.cold != nil {
		if err := s.wake(sp, req); err != nil {
			return nil, err
		}
	}
	out, held, err := s.claim(ns, req)
	if err == nil && len(held) > 0 && len(out) < req.Limit {
		out, err = s.claimCold(ns, req, out)
	}
	if err != nil {
		return nil, err
	}
	return s.rebuild(ns, out)
}

// claim runs one claim and returns, with the jobs it took, the queues whose
// next job waits in the cold store.
func (s *Store) claim(ns string, req runnel.ClaimRequest) ([]draft, []string, error) {
	var out []draft
	var held []string
	err := s.lease(ns, func(t *tx) error {
		c := claimer{s: s, t: t, sp: t.sp, req: req, now: ms(req.Now), buckets: map[string]*token{}, full: map[countKey]bool{}}
		c.lease = c.now + req.Lease.Milliseconds()
		c.worker = t.sp.syms.id(req.Worker)
		c.run()
		out, held = c.out, c.held
		return nil
	})
	return out, held, err
}

type claimer struct {
	s       *Store
	t       *tx
	sp      *space
	req     runnel.ClaimRequest
	now     int64
	lease   int64
	worker  sym
	out     []draft
	held    []string
	buckets map[string]*token
	full    map[countKey]bool
}

func (c *claimer) run() {
	sp := c.sp
	var queues []sym
	for _, q := range slices.Compact(slices.Sorted(slices.Values(c.req.Queues))) {
		if v, ok := sp.syms.find(q); ok {
			queues = append(queues, v)
		}
	}
	if len(c.req.IDs) == 0 {
		c.polls(queues)
	}
	var open []sym
	for _, q := range queues {
		if !sp.queueConfig(sp.syms.name(q)).Paused {
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
}

// polls hands out polling jobs whose next poll is due; they hold their slot
// already, so limits do not apply.
func (c *claimer) polls(queues []sym) {
	for _, q := range queues {
		t := c.sp.poll[q]
		if t == nil {
			continue
		}
		var due []uid
		t.Ascend(func(k timeKey) bool {
			if k.at > c.now || len(c.out)+len(due) >= c.req.Limit {
				return false
			}
			due = append(due, k.id)
			return true
		})
		for _, id := range due {
			h, _ := c.sp.get(id)
			h.state, h.worker, h.lease = stRunning, c.worker, c.lease
			c.save(h)
		}
	}
}

func (c *claimer) save(h job) {
	h.version++
	h.updated = c.now
	c.t.putHot(h)
	c.out = append(c.out, c.sp.draft(&h))
}

func (c *claimer) take(h job) {
	h.state, h.worker, h.lease = stRunning, c.worker, c.lease
	h.attempt++
	if h.started == 0 {
		h.started = c.now
	}
	c.save(h)
}

func (c *claimer) byID(open []sym) {
	var cands []job
	for _, s := range slices.Compact(slices.Sorted(slices.Values(c.req.IDs))) {
		id, ok := parseID(s)
		if !ok {
			continue
		}
		if h, ok := c.sp.get(id); ok && h.is(stWaiting) && h.runAt <= c.now && slices.Contains(open, h.queue) {
			cands = append(cands, h)
		}
	}
	slices.SortFunc(cands, func(a, b job) int {
		if lessReady(readyOf(&a), readyOf(&b)) {
			return -1
		}
		return 1
	})
	for _, h := range cands {
		if len(c.out) >= c.req.Limit {
			return
		}
		if c.admit(&h) {
			c.take(h)
		}
	}
}

// ready claims in priority order across the open queues, merging their ready
// trees. Jobs a limit refuses are passed over, so the jobs behind them still run.
func (c *claimer) ready(open []sym) {
	h := &merge{}
	for _, q := range open {
		t, stop := c.sp.ready[q], c.sp.stop(q)
		if t == nil && stop == nil {
			continue
		}
		if cur := (&cursor{t: t, stop: stop, q: q}); cur.fill() {
			h.items = append(h.items, cur)
		}
	}
	heap.Init(h)
	examined := 0
	for h.Len() > 0 && len(c.out) < c.req.Limit && examined < maxExamine {
		cur := h.items[0]
		if cur.held() {
			c.held = append(c.held, c.sp.syms.name(cur.q))
			return
		}
		if j, ok := c.sp.get(cur.head().id); ok && j.is(stWaiting) {
			if c.admit(&j) {
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

// cursor walks one ready tree a page at a time, resuming after the last key it
// returned, so claiming from the tree while walking it is safe. It stops at
// stop, the queue's head in the cold store, and is then held there.
type cursor struct {
	t    *readyTree
	stop *readyKey
	q    sym
	page []readyKey
	last *readyKey
}

func (c *cursor) fill() bool {
	c.page = c.page[:0]
	visit := func(k readyKey) bool {
		if c.last != nil && !lessReady(*c.last, k) {
			return true
		}
		if c.stop != nil && !lessReady(k, *c.stop) {
			return false
		}
		c.page = append(c.page, k)
		return len(c.page) < pageSize
	}
	switch {
	case c.t == nil:
	case c.last == nil:
		c.t.Ascend(visit)
	default:
		c.t.AscendGreaterOrEqual(*c.last, visit)
	}
	return len(c.page) > 0 || c.stop != nil
}

func (c *cursor) held() bool { return len(c.page) == 0 }

func (c *cursor) head() readyKey {
	if c.held() {
		return *c.stop
	}
	return c.page[0]
}

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
