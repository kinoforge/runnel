package hybrid

import (
	"slices"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

const (
	coldDir = "done"
	// Finished jobs stay in memory for retireAfter, so callers waiting for a
	// result find it there, then move to the cold store in batches.
	retireAfter = time.Second
	retireEvery = time.Second
	retireBatch = slabSize
)

// retirer moves finished jobs to the cold store until the store closes.
func (s *Store) retirer() {
	defer s.wg.Done()
	t := time.NewTicker(s.retireEvery)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		if s.broken() == nil {
			if err := s.retire(ms(time.Now().Add(-s.retireAfter))); err != nil {
				s.fail(err)
			}
		}
	}
}

// retire moves every job that finished before cutoff to the cold store. The
// log is synced first, so the cold store never holds an ending the log could
// lose.
func (s *Store) retire(cutoff int64) error {
	s.coldMu.Lock()
	defer s.coldMu.Unlock()
	if err := s.log.Sync(); err != nil {
		return err
	}
	for _, sp := range s.allSpaces() {
		for {
			n, err := s.retireSome(sp, cutoff)
			if err != nil {
				return err
			}
			if n < retireBatch {
				break
			}
		}
		if err := s.retireEvents(sp, cutoff); err != nil {
			return err
		}
	}
	return nil
}

// retireSome copies up to retireBatch finished jobs to the cold store, then
// drops from memory those that did not change meanwhile.
func (s *Store) retireSome(sp *space, cutoff int64) (int, error) {
	ds := sp.retirable(cutoff, retireBatch)
	if len(ds) == 0 {
		return 0, nil
	}
	jobs, err := s.coldJobs(sp.ns, ds)
	if err != nil {
		return 0, err
	}
	delta, err := s.cold.Put(jobs)
	if err != nil {
		return 0, err
	}
	if stale := sp.forget(ds, delta); len(stale) > 0 {
		if err := s.dropCold(sp, stale); err != nil {
			return 0, err
		}
	}
	return len(ds), nil
}

// coldJobs encodes drafts for the cold store, reading the bodies they lack.
func (s *Store) coldJobs(ns string, ds []draft) ([]cold.Job, error) {
	s.fill(ds)
	out := make([]cold.Job, len(ds))
	for i := range ds {
		d := &ds[i]
		if d.body == nil {
			if err := s.fetch(ns, d); err != nil {
				return nil, err
			}
		}
		var e enc
		e.named(&d.h, d.names)
		e.bytes(d.body)
		out[i] = cold.Job{NS: ns, ID: cold.ID(d.h.id), State: d.h.state, Finished: d.h.finished,
			Queue: d.names[0], Kind: d.names[1], Tenant: d.names[2], Data: e.b}
	}
	return out, nil
}

// dropCold deletes ids from the cold store and counts them out.
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
	sp.mu.Unlock()
	return nil
}

// loadCold reads a job only the cold store holds, interning its names in t.
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

// coldGet returns the whole job id from the cold store, or nil.
func (s *Store) coldGet(ns string, id uid) (*runnel.Job, error) {
	t := newSyms()
	h, body, ok, err := s.loadCold(ns, id, &t)
	if !ok || err != nil {
		return nil, err
	}
	return t.full(ns, &h, body)
}

// retirable copies up to n jobs that finished before cutoff, oldest first.
func (sp *space) retirable(cutoff int64, n int) []draft {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var out []draft
	sp.fin.Ascend(func(k timeKey) bool {
		if k.at >= cutoff || len(out) >= n {
			return false
		}
		h, _ := sp.get(k.id)
		out = append(out, sp.draft(&h))
		return true
	})
	return out
}

// forget counts delta into the cold counts and drops from memory the drafted
// jobs that did not change since; it returns the others, whose cold copy is
// stale.
func (sp *space) forget(ds []draft, delta map[cold.Count]int64) []uid {
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
		if h, ok := sp.get(id); ok && h.version == ds[i].h.version {
			sp.delJob(id)
		} else {
			stale = append(stale, id)
		}
	}
	sp.retired.Add(1)
	return stale
}

// noteCold raises coldMax to id.
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

// mayBeCold reports whether id could be in the cold store.
func (sp *space) mayBeCold(id uid) bool {
	top := sp.coldMax.Load()
	return top != nil && cmpID(id, *top) <= 0
}

// addCold adds delta to the counts of the namespace's jobs in the cold store.
func (sp *space) addCold(delta map[cold.Count]int64) {
	for c, d := range delta {
		if c.NS != sp.ns || c.State == 0 || c.State >= stCount {
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

// hasCold reports whether the cold store holds any job of the namespace.
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

// unretire deletes from the cold store the jobs a call brought back into
// memory, once the log holding that change is on disk.
func (s *Store) unretire(sp *space, ids []uid) error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	return s.dropCold(sp, ids)
}

// gather returns the jobs want names, in that order: those drafted from
// memory, the others from the cold store. Jobs found in neither are left out.
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

// finals are the states a job can end in.
var finals = []uint8{stCompleted, stDead, stCancelled}

// coldNewest adds to ram, the ids a listing found in memory, those of the
// finished jobs in the cold store that match q, and returns the newest limit.
func (s *Store) coldNewest(ns string, st uint8, q runnel.Query, cur uid, hasCur bool, ram []uid) ([]uid, error) {
	states := finals
	switch {
	case s.cold == nil || (st != 0 && !final(st)):
		return ram, nil
	case st != 0:
		states = []uint8{st}
	}
	var at *cold.ID
	if hasCur {
		c := cold.ID(cur)
		at = &c
	}
	f := cold.Filter{Queue: q.Queue, Kind: q.Kind, Tenant: q.Tenant}
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
