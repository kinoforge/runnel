package hybrid

import "math"

// admit checks h against every running limit and rate limit, taking a token
// from each rate bucket when it passes. A refusal marks the queue, group or
// pool full for the rest of the claim.
func (c *claimer) admit(h *job) bool {
	sp := c.sp
	qk := countKey{cQueue, h.queue, 0}
	qtk := countKey{cQueueGroup, h.queue, h.group}
	rk := countKey{cPool, h.pool, 0}
	rtk := countKey{cPoolGroup, h.pool, h.group}
	if c.full[qk] || c.full[qtk] || c.full[rk] || c.full[rtk] {
		return false
	}
	qname := sp.syms.name(h.queue)
	q := sp.queueConfig(qname)
	if over(q.MaxRunning, sp.active[qk]) {
		return c.refuse(qk)
	}
	if h.group != 0 && over(q.GroupMaxRunning, sp.active[qtk]) {
		return c.refuse(qtk)
	}
	var take []*token
	if h.pool != 0 {
		pname := sp.syms.name(h.pool)
		r := sp.poolConfig(pname)
		if r.Stopped(c.now) || over(r.Slots, sp.active[rk]) {
			return c.refuse(rk)
		}
		if h.group != 0 && over(r.GroupSlots, sp.active[rtk]) {
			return c.refuse(rtk)
		}
		if r.Rate > 0 {
			b := c.bucket("r|"+pname, r.Rate, r.Burst)
			if b.tokens < 1 {
				return c.refuse(rk)
			}
			take = append(take, b)
		}
	}
	if q.Rate > 0 {
		b := c.bucket("q|"+qname, q.Rate, q.Burst)
		if b.tokens < 1 {
			return c.refuse(qk)
		}
		take = append(take, b)
	}
	if h.group != 0 && q.GroupRate > 0 {
		b := c.bucket("qt|"+qname+"|"+sp.syms.name(h.group), q.GroupRate, q.GroupBurst)
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

func (c *claimer) refuse(key countKey) bool {
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
	if old, ok := c.sp.buckets[key]; ok {
		b.tokens = math.Min(capacity, old.tokens+float64(max(c.now-old.at, 0))*rate/1000)
	}
	c.buckets[key] = b
	return b
}
