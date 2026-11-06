package hybrid

import (
	"cmp"
	"slices"
	"time"

	"github.com/kinoforge/runnel/store/hybrid/cold"
)

// Memory holds the front of each queue's waiting jobs; the rest wait in the
// cold store, in claim order, so a backlog is bounded by disk, not memory.
// A claim takes from memory the jobs ahead of a queue's first job in the
// cold store, its head; past it, the claim brings the next ones back first.
// spiller keeps the waiting jobs in memory within budget until the store
// closes.
func (s *Store) spiller() {
	defer s.wg.Done()
	t := time.NewTicker(s.opt.SpillEvery)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		if s.broken() == nil {
			if err := s.balance(); err != nil {
				s.fail(err)
			}
		}
	}
}

// balance moves waiting jobs past the budget to the cold store, last in claim
// order first, and, while memory has room, brings back the next ones of the
// queues that run low, ahead of the claims that will reach them.
func (s *Store) balance() error {
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	spaces := s.allSpaces()
	total := s.hot.Load()
	for total > s.hotJobs {
		sp := slices.MaxFunc(spaces, func(a, b *space) int { return cmp.Compare(a.hot.Load(), b.hot.Load()) })
		n, err := s.spillSome(sp, min(total-s.hotJobs, s.opt.SpillBatch))
		if err != nil || n == 0 {
			return err
		}
		total -= n
	}
	for _, sp := range spaces {
		for _, q := range sp.lowQueues(s.opt.RefillAhead) {
			for range s.opt.RefillAhead / s.opt.RefillBatch {
				if total+s.opt.RefillBatch > s.hotJobs {
					return nil
				}
				n, err := s.refill(sp, q)
				if err != nil || n == 0 {
					return err
				}
				total += int64(n)
			}
		}
	}
	return nil
}

// makeRoom has an insert wait while memory holds twice the budget of waiting
// jobs, moving some to the cold store itself, so producers faster than the
// spiller slow to its pace instead of filling memory.
func (s *Store) makeRoom() error {
	if s.cold == nil || s.hot.Load() <= 2*s.hotJobs+s.opt.SpillBatch {
		return nil
	}
	return s.balance()
}

// spillSome moves up to n waiting jobs of sp to the cold store.
func (s *Store) spillSome(sp *space, n int64) (int64, error) {
	now := ms(time.Now())
	ds := sp.spillable(int(n), int(s.opt.JobBytes), s.opt.BatchBytes)
	if len(ds) == 0 {
		return 0, nil
	}
	jobs, err := s.coldJobs(sp.ns, ds, now)
	if err != nil {
		return 0, err
	}
	delta, err := s.cold.Add(jobs)
	if err != nil {
		return 0, err
	}
	stale := sp.forget(ds, delta, func(h *job) { sp.spilt(h, now) })
	if len(stale) > 0 {
		if err := s.dropCold(sp, stale); err != nil {
			return 0, err
		}
	}
	return int64(len(ds) - len(stale)), nil
}

// spillable drafts up to n waiting jobs, holding at most maxBytes counting
// jobBytes each besides bodies, from the queues holding the most,
// last in claim order first: future ones by run time, then ready ones. Jobs
// others depend on stay, as their dependents are listed in memory, and so do
// jobs still being deleted from the cold store, so that no job is there
// twice and the cold store can add the others without looking them up.
func (sp *space) spillable(n, jobBytes, maxBytes int) []draft {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	queues := make([]sym, 0, len(sp.counts))
	for q, c := range sp.counts {
		if c[stWaiting] > 0 {
			queues = append(queues, q)
		}
	}
	slices.SortFunc(queues, func(a, b sym) int { return cmp.Compare(sp.counts[b][stWaiting], sp.counts[a][stWaiting]) })
	var out []draft
	size := 0
	add := func(id uid) bool {
		if h, ok := sp.get(id); ok && h.is(stWaiting) && len(sp.kids[id]) == 0 && sp.unretiring[id] == 0 {
			out = append(out, sp.draft(&h))
			size += jobBytes + int(h.body.n) + len(sp.cache.bodies[id])
		}
		return len(out) < n && size < maxBytes
	}
	for _, q := range queues {
		if t := sp.sched[q]; t != nil && len(out) < n && size < maxBytes {
			t.Descend(func(k timeKey) bool { return add(k.id) })
		}
		if t := sp.ready[q]; t != nil && len(out) < n && size < maxBytes {
			t.Descend(func(k readyKey) bool { return add(k.id) })
		}
	}
	return out
}

// spilt notes that h left memory for the cold store: its start deadline stays
// indexed, and its queue's head moves to it when it comes first.
func (sp *space) spilt(h *job, now int64) {
	if h.startBy != 0 {
		sp.startBy.ReplaceOrInsert(timeKey{h.startBy, h.id})
	}
	hd := sp.heads[h.queue]
	if h.runAt <= now {
		k := cold.Next{Prio: h.prio, RunAt: h.runAt, ID: cold.ID(h.id)}
		if !hd.HasReady || lessReady(readyOf(h), nextKey(hd.Ready)) {
			hd.Ready, hd.HasReady = k, true
		}
	} else if !hd.HasDue || h.runAt < hd.Due {
		hd.Due, hd.HasDue = h.runAt, true
	}
	sp.setHead(h.queue, hd)
}

func nextKey(n cold.Next) readyKey { return readyKey{prio: n.Prio, at: n.RunAt, id: uid(n.ID)} }

func (sp *space) setHead(q sym, h cold.Head) {
	if h.Empty() {
		delete(sp.heads, q)
	} else {
		sp.heads[q] = h
	}
	sp.spilled.Store(len(sp.heads) > 0)
}

// stop returns the head of q's ready jobs in the cold store, if any.
func (sp *space) stop(q sym) *readyKey {
	if h, ok := sp.heads[q]; ok && h.HasReady {
		k := nextKey(h.Ready)
		return &k
	}
	return nil
}

// lowQueues names the queues with ready jobs in the cold store and fewer
// than ahead in memory.
func (sp *space) lowQueues(ahead int64) []string {
	if !sp.spilled.Load() {
		return nil
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var out []string
	for q, h := range sp.heads {
		if c := sp.counts[q]; h.HasReady && (c == nil || c[stWaiting] < ahead) {
			out = append(out, sp.syms.name(q))
		}
	}
	return out
}

// refill brings the next ready jobs of queue q back from the cold store. The
// caller holds waitMu.
func (s *Store) refill(sp *space, q string) (int, error) {
	jobs, head, err := s.cold.Take(sp.ns, q, int(s.opt.RefillBatch), s.opt.BatchBytes)
	if err != nil {
		return 0, err
	}
	return len(jobs), s.bring(sp, jobs, q, &head)
}

// bring puts jobs from the cold store back in memory, apart from those memory
// holds already, newer, and deletes them there once logged; with head set,
// it becomes the head of queue q. The caller holds waitMu, so the cold store
// still holds jobs as read.
func (s *Store) bring(sp *space, jobs []cold.Job, q string, head *cold.Head) error {
	err := s.lease(sp.ns, func(t *tx) error {
		for i := range jobs {
			d := newDec(jobs[i].Data)
			h := decodeHot(d, &t.sp.syms)
			body := d.bytes()
			if d.err != nil {
				return d.err
			}
			if _, ok := t.sp.get(h.id); !ok {
				t.put(h, body)
			}
		}
		if head != nil {
			t.sp.setHead(t.sp.syms.id(q), *head)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		s.fail(err)
		return s.broken()
	}
	delta, err := s.cold.Remove(jobs)
	if err != nil {
		return err
	}
	sp.mu.Lock()
	sp.addCold(delta)
	sp.mu.Unlock()
	return nil
}

// loadWaiting reads the heads of the queues with waiting jobs in the cold
// store and indexes their start deadlines.
func (s *Store) loadWaiting() error {
	err := s.cold.Deadlines(func(ns string, at int64, id cold.ID) {
		sp := s.space(ns)
		sp.startBy.ReplaceOrInsert(timeKey{at, uid(id)})
	})
	if err != nil {
		return err
	}
	for _, sp := range s.allSpaces() {
		for q, c := range sp.coldCounts {
			if c[stWaiting] <= 0 {
				continue
			}
			h, err := s.cold.Front(sp.ns, q)
			if err != nil {
				return err
			}
			sp.setHead(sp.syms.id(q), h)
		}
	}
	return nil
}
