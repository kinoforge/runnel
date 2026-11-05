package hybrid

import "math"

// admit checks h against every running limit and rate limit, taking a token
// from each rate bucket when it passes. A refusal marks the queue, tenant or
// resource full for the rest of the claim.
func (c *claimer) admit(h *job) bool {
	sp := c.sp
	qk := countKey{cQueue, h.queue, 0}
	qtk := countKey{cQueueTenant, h.queue, h.tenant}
	rk := countKey{cResource, h.resource, 0}
	rtk := countKey{cResourceTenant, h.resource, h.tenant}
	if c.full[qk] || c.full[qtk] || c.full[rk] || c.full[rtk] {
		return false
	}
	qname := sp.syms.name(h.queue)
	q := sp.queueConfig(qname)
	if over(q.MaxRunning, sp.active[qk]) {
		return c.refuse(qk)
	}
	if h.tenant != 0 && over(q.TenantMaxRunning, sp.active[qtk]) {
		return c.refuse(qtk)
	}
	if h.resource != 0 {
		r := sp.resourceConfig(sp.syms.name(h.resource))
		if over(r.MaxRunning, sp.active[rk]) {
			return c.refuse(rk)
		}
		if h.tenant != 0 && over(r.TenantMaxRunning, sp.active[rtk]) {
			return c.refuse(rtk)
		}
	}
	var take []*token
	if q.Rate > 0 {
		b := c.bucket("q|"+qname, q.Rate, q.Burst)
		if b.tokens < 1 {
			return c.refuse(qk)
		}
		take = append(take, b)
	}
	if h.tenant != 0 && q.TenantRate > 0 {
		b := c.bucket("qt|"+qname+"|"+sp.syms.name(h.tenant), q.TenantRate, q.TenantBurst)
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
