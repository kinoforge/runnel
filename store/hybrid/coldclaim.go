package hybrid

import (
	"math"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

// Runs under waitMu so jobs brought back stay in memory, and claims again first, since another call may have brought them back already.
func (s *Store) claimCold(ns string, req runnel.ClaimRequest, out []draft) ([]draft, error) {
	sp := s.space(ns)
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	for range s.opt.MaxRefills {
		r := req
		r.Limit -= len(out)
		more, h, err := s.claim(ns, r)
		if err != nil {
			return nil, err
		}
		out = append(out, more...)
		if len(h) == 0 || len(out) >= req.Limit {
			break
		}
		for _, q := range h {
			if _, err := s.refill(sp, q); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (s *Store) wake(sp *space, req runnel.ClaimRequest) error {
	now := ms(req.Now)
	due, ids := sp.toWake(req, now)
	if len(due) == 0 && len(ids) == 0 {
		return nil
	}
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	for _, q := range due {
		if err := s.wakeQueue(sp, q, now); err != nil {
			return err
		}
	}
	var jobs []cold.Job
	for _, id := range ids {
		j, ok, err := s.cold.Get(sp.ns, cold.ID(id))
		if err != nil {
			return err
		}
		if ok && j.State == stWaiting {
			jobs = append(jobs, j)
		}
	}
	if len(jobs) == 0 {
		return nil
	}
	return s.bring(sp, jobs, "", nil)
}

func (sp *space) toWake(req runnel.ClaimRequest, now int64) ([]string, []uid) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var due []string
	for _, q := range req.Queues {
		if v, ok := sp.syms.find(q); ok {
			if h, ok := sp.heads[v]; ok && h.HasDue && h.Due <= now {
				due = append(due, q)
			}
		}
	}
	var ids []uid
	for _, raw := range req.IDs {
		if id, ok := parseID(raw); ok && sp.mayBeCold(id) {
			if _, in := sp.get(id); !in {
				ids = append(ids, id)
			}
		}
	}
	return due, ids
}

func (s *Store) wakeQueue(sp *space, q string, now int64) error {
	sp.mu.Lock()
	v := sp.syms.id(q)
	h := sp.heads[v]
	h.Ready, h.HasReady = cold.Next{Prio: math.MaxInt32, RunAt: math.MinInt64}, true
	sp.setHead(v, h)
	sp.mu.Unlock()
	for {
		n, err := s.cold.Wake(sp.ns, q, now, s.opt.WakeBatch)
		if err != nil {
			return err
		}
		if n < s.opt.WakeBatch {
			break
		}
	}
	h, err := s.cold.Front(sp.ns, q)
	if err != nil {
		return err
	}
	sp.mu.Lock()
	sp.setHead(v, h)
	sp.mu.Unlock()
	return nil
}
