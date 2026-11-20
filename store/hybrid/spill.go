package hybrid

import (
	"cmp"
	"slices"
	"time"

	"github.com/kinoforge/runnel/store/hybrid/cold"
)

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

func (s *Store) balance() error {
	s.coldMu.Lock()
	defer s.coldMu.Unlock()
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	spaces := s.allSpaces()
	total, err := s.spillDown(spaces, s.hot.Load())
	if err != nil {
		return err
	}
	for _, sp := range spaces {
		for _, q := range sp.lowQueues(s.opt.RefillAhead) {
			var full bool
			if total, full, err = s.refillQueue(sp, q, total); err != nil || full {
				return err
			}
		}
	}
	return nil
}

func (s *Store) spillDown(spaces []*space, total int64) (int64, error) {
	if total <= s.hotJobs {
		return total, nil
	}
	slices.SortFunc(spaces, func(a, b *space) int { return cmp.Compare(b.hot.Load(), a.hot.Load()) })
	for _, sp := range spaces {
		for total > s.hotJobs {
			n, err := s.spillSome(sp, min(total-s.hotJobs, s.opt.SpillBatch))
			if err != nil {
				return total, err
			}
			if n == 0 {
				break
			}
			total -= n
		}
		if total <= s.hotJobs {
			break
		}
	}
	return total, nil
}

func (s *Store) refillQueue(sp *space, q string, total int64) (int64, bool, error) {
	for range s.opt.RefillAhead / s.opt.RefillBatch {
		if total+s.opt.RefillBatch > s.hotJobs {
			return total, true, nil
		}
		n, err := s.refill(sp, q)
		if err != nil || n == 0 {
			return total, false, err
		}
		total += int64(n)
	}
	return total, false, nil
}

func (s *Store) makeRoom() error {
	if s.cold == nil || s.hot.Load() <= 2*s.hotJobs+s.opt.SpillBatch {
		return nil
	}
	return s.balance()
}

type spill struct {
	now   int64
	ds    []draft
	delta map[cold.Count]int64
}

func (s *Store) spillSome(sp *space, n int64) (int64, error) {
	p, err := s.spillStart(sp, n)
	if err != nil || p == nil {
		return 0, err
	}
	return s.spillEnd(sp, p)
}

func (s *Store) spillStart(sp *space, n int64) (*spill, error) {
	p := &spill{now: ms(time.Now())}
	p.ds = sp.spillable(int(n), int(s.opt.JobBytes), s.opt.BatchBytes)
	if len(p.ds) == 0 {
		return nil, nil
	}
	var jobs []cold.Job
	var err error
	p.ds, jobs, err = s.coldJobs(sp.ns, p.ds, p.now)
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	if p.delta, err = s.cold.Add(jobs); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) spillEnd(sp *space, p *spill) (int64, error) {
	stale := sp.forget(p.ds, p.delta, func(h *job) { sp.spilt(h, p.now) })
	if len(stale) > 0 {
		if err := s.dropCold(sp, stale); err != nil {
			return 0, err
		}
	}
	return int64(len(p.ds) - len(stale)), nil
}

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

func (sp *space) spilt(h *job, now int64) {
	if h.startBy != 0 {
		sp.startBy.ReplaceOrInsert(timeKey{h.startBy, h.id})
	}
	if h.deadline != 0 {
		sp.ends.ReplaceOrInsert(timeKey{h.deadline, h.id})
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

func (sp *space) stop(q sym) *readyKey {
	if h, ok := sp.heads[q]; ok && h.HasReady {
		k := nextKey(h.Ready)
		return &k
	}
	return nil
}

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

// The caller holds waitMu.
func (s *Store) refill(sp *space, q string) (int, error) {
	jobs, head, err := s.cold.Take(sp.ns, q, int(s.opt.RefillBatch), s.opt.BatchBytes)
	if err != nil {
		return 0, err
	}
	return len(jobs), s.bring(sp, jobs, q, &head)
}

// The caller holds waitMu.
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
		s.fail(err)
		return s.broken()
	}
	sp.mu.Lock()
	sp.addCold(delta)
	sp.mu.Unlock()
	return nil
}

func (s *Store) loadWaiting() error {
	err := s.cold.Starts(func(ns string, at int64, id cold.ID) {
		s.space(ns).startBy.ReplaceOrInsert(timeKey{at, uid(id)})
	})
	if err == nil {
		err = s.cold.Deadlines(func(ns string, at int64, id cold.ID) {
			s.space(ns).ends.ReplaceOrInsert(timeKey{at, uid(id)})
		})
	}
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
