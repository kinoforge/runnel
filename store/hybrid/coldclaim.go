package hybrid

import (
	"math"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

// claimCold goes on with a claim held at the head of a queue's jobs in the
// cold store: under waitMu, so that jobs brought back stay in memory, it
// claims again, as another call may have brought them back already, and
// brings back the next jobs of the queues still held.
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

// wake moves the future jobs of the claimed queues that are due by the claim
// to the cold store's claim order, and brings back the jobs claimed by id
// that wait there.
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

// toWake names the claimed queues with future jobs in the cold store due by
// now, and the ids claimed that memory does not hold.
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

// wakeQueue readies q's future jobs due at now. Until its head is read back,
// the queue is held at the front, as the jobs readied may come first.
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
