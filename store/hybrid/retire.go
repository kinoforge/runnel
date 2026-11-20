package hybrid

import (
	"errors"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

const coldDir = "done"

func (s *Store) retirer() {
	defer s.wg.Done()
	t := time.NewTicker(s.opt.RetireEvery)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		if s.broken() == nil {
			if err := s.retire(ms(time.Now().Add(-s.opt.RetireAfter)), false); err != nil {
				s.fail(err)
			}
		}
	}
}

// Syncs the log first, so the cold store never holds an ending the log could lose.
func (s *Store) retire(cutoff int64, replace bool) error {
	s.coldMu.Lock()
	defer s.coldMu.Unlock()
	if err := s.log.Sync(); err != nil {
		return err
	}
	for _, sp := range s.allSpaces() {
		for {
			n, err := s.retireSome(sp, cutoff, replace)
			if err != nil {
				return err
			}
			if n < s.opt.RetireBatch {
				break
			}
		}
		if err := s.retireEvents(sp, cutoff); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) retireSome(sp *space, cutoff int64, replace bool) (int, error) {
	ds := sp.retirable(cutoff, s.opt.RetireBatch)
	if len(ds) == 0 {
		return 0, nil
	}
	var jobs []cold.Job
	var err error
	ds, jobs, err = s.coldJobs(sp.ns, ds, 0)
	if err != nil || len(jobs) == 0 {
		return 0, err
	}
	put := s.cold.Add
	if replace {
		put = s.cold.Put
	}
	delta, err := put(jobs)
	if err != nil {
		return 0, err
	}
	if stale := sp.forget(ds, delta, nil); len(stale) > 0 {
		if err := s.dropCold(sp, stale); err != nil {
			return 0, err
		}
	}
	return len(ds), nil
}

func (s *Store) coldJobs(ns string, ds []draft, now int64) ([]draft, []cold.Job, error) {
	s.fill(ds)
	kept := make([]draft, 0, len(ds))
	out := make([]cold.Job, 0, len(ds))
	for i := range ds {
		d := &ds[i]
		if d.body == nil {
			if err := s.fetch(ns, d); err != nil {
				if errors.Is(err, errNoBody) {
					continue
				}
				return nil, nil, err
			}
		}
		var e enc
		e.named(&d.h, d.names)
		e.bytes(d.body)
		h := &d.h
		kept = append(kept, *d)
		out = append(out, cold.Job{NS: ns, ID: cold.ID(h.id), State: h.state, Finished: h.finished,
			Prio: h.prio, RunAt: h.runAt, StartBy: h.startBy, Deadline: h.deadline, Ready: h.runAt <= now,
			Queue: d.names[0], Kind: d.names[1], Group: d.names[2], Data: e.b})
	}
	return kept, out, nil
}

func (s *Store) dropCold(sp *space, ids []uid) error {
	keys := make([]cold.ID, len(ids))
	for i, id := range ids {
		keys[i] = cold.ID(id)
	}
	delta, err := s.cold.Delete(sp.ns, keys)
	if err != nil {
		return err
	}
	sp.mu.Lock()
	sp.addCold(delta)
	sp.retired.Add(1)
	sp.mu.Unlock()
	return nil
}

func (s *Store) loadCold(ns string, id uid, t *syms) (job, []byte, bool, error) {
	if s.cold == nil {
		return job{}, nil, false, nil
	}
	j, ok, err := s.cold.Get(ns, cold.ID(id))
	if !ok || err != nil {
		return job{}, nil, false, err
	}
	d := newDec(j.Data)
	h := decodeHot(d, t)
	body := d.bytes()
	return h, body, d.err == nil, d.err
}

func (s *Store) coldGet(ns string, id uid) (*runnel.Job, error) {
	t := newSyms()
	h, body, ok, err := s.loadCold(ns, id, &t)
	if !ok || err != nil {
		return nil, err
	}
	return t.full(ns, &h, body)
}

func (s *Store) rebuildOne(ns string, d *draft) (*runnel.Job, error) {
	if d.body == nil {
		if err := s.fetch(ns, d); err != nil {
			if errors.Is(err, errNoBody) {
				if cj, cerr := s.coldGet(ns, d.h.id); cerr == nil && cj != nil {
					return cj, nil
				}
			}
			return nil, err
		}
	}
	return d.job(ns)
}

func (sp *space) retirable(cutoff int64, n int) []draft {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var out []draft
	sp.fin.Ascend(func(k timeKey) bool {
		if k.at >= cutoff || len(out) >= n {
			return false
		}
		if sp.unretiring[k.id] == 0 {
			h, _ := sp.get(k.id)
			out = append(out, sp.draft(&h))
		}
		return true
	})
	return out
}

func (sp *space) forget(ds []draft, delta map[cold.Count]int64, gone func(*job)) []uid {
	top := uid{}
	for i := range ds {
		if cmpID(ds[i].h.id, top) > 0 {
			top = ds[i].h.id
		}
	}
	sp.noteCold(top)
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.addCold(delta)
	var stale []uid
	for i := range ds {
		id := ds[i].h.id
		if h, ok := sp.get(id); ok && h.version == ds[i].h.version && len(sp.kids[id]) == 0 {
			sp.delJob(id)
			if gone != nil {
				gone(&h)
			}
		} else {
			stale = append(stale, id)
		}
	}
	sp.retired.Add(1)
	return stale
}

func (sp *space) noteCold(id uid) {
	for {
		cur := sp.coldMax.Load()
		if cur != nil && cmpID(id, *cur) <= 0 {
			return
		}
		if sp.coldMax.CompareAndSwap(cur, &id) {
			return
		}
	}
}

func (sp *space) mayBeCold(id uid) bool {
	top := sp.coldMax.Load()
	return top != nil && cmpID(id, *top) <= 0
}

func (sp *space) addCold(delta map[cold.Count]int64) {
	for c, d := range delta {
		if c.NS != sp.ns || c.State == 0 || c.State >= stCount {
			continue
		}
		if c.Group != "" {
			k := [2]string{c.Queue, c.Group}
			if sp.coldQueued[k] += d; sp.coldQueued[k] == 0 {
				delete(sp.coldQueued, k)
			}
			continue
		}
		n := sp.coldCounts[c.Queue]
		if n == nil {
			n = &[stCount]int64{}
			sp.coldCounts[c.Queue] = n
		}
		n[c.State] += d
	}
}

func (sp *space) hasCold() bool {
	for _, n := range sp.coldCounts {
		for _, v := range n {
			if v != 0 {
				return true
			}
		}
	}
	return false
}

// Deletes the cold copies only once the log holding the change is on disk.
func (s *Store) unretire(sp *space, ids []uid) error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	err := s.dropCold(sp, ids)
	sp.mu.Lock()
	for _, id := range ids {
		if sp.unretiring[id] > 1 {
			sp.unretiring[id]--
		} else {
			delete(sp.unretiring, id)
		}
	}
	sp.mu.Unlock()
	return err
}

func (s *Store) gather(ns string, want []uid, ds []draft) ([]*runnel.Job, error) {
	jobs, err := s.rebuild(ns, ds)
	if err != nil {
		return nil, err
	}
	byID := make(map[uid]*runnel.Job, len(jobs))
	for i := range jobs {
		byID[ds[i].h.id] = jobs[i]
	}
	out := make([]*runnel.Job, 0, len(want))
	for _, id := range want {
		j := byID[id]
		if j == nil {
			if j, err = s.coldGet(ns, id); err != nil {
				return nil, err
			}
		}
		if j != nil {
			out = append(out, j)
		}
	}
	return out, nil
}

var finals = []uint8{stCompleted, stDead, stCancelled, stExpired}

func (s *Store) coldNewest(ns string, st uint8, q runnel.Query, cur uid, hasCur bool, ram []uid) ([]uid, error) {
	states := append([]uint8{stWaiting}, finals...)
	switch {
	case s.cold == nil || (st != 0 && st != stWaiting && !final(st)):
		return ram, nil
	case st != 0:
		states = []uint8{st}
	}
	var at *cold.ID
	if hasCur {
		c := cold.ID(cur)
		at = &c
	}
	f := cold.Filter{Queue: q.Queue, Kind: q.Kind, Group: q.Group}
	seen := make(map[uid]bool, len(ram))
	ids := slices.Clone(ram)
	for _, id := range ram {
		seen[id] = true
	}
	for _, st := range states {
		found, err := s.cold.List(ns, st, f, at, q.Limit)
		if err != nil {
			return nil, err
		}
		for _, c := range found {
			if id := uid(c); !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	slices.SortFunc(ids, func(a, b uid) int { return cmpID(b, a) })
	if q.Limit > 0 && len(ids) > q.Limit {
		ids = ids[:q.Limit]
	}
	return ids, nil
}
