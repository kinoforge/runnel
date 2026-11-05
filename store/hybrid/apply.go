package hybrid

import (
	"context"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
)

func (s *Store) Apply(_ context.Context, ns string, changes []runnel.Change, now time.Time) ([]bool, error) {
	out := make([]bool, len(changes))
	bodies := make([][]byte, len(changes))
	for i, c := range changes {
		bodies[i] = encodeBody(c.Job)
	}
	err := s.write(ns, func(t *tx) error {
		for i, c := range changes {
			ok, err := s.applyOne(t, c, bodies[i], ms(now))
			if err != nil {
				return err
			}
			out[i] = ok
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i, c := range changes {
		if out[i] {
			c.Job.Version++
		}
	}
	return out, nil
}

// applyOne writes c if its guards hold. The store keeps the fields it owns:
// pending, unique key, dependencies and creation time. body is c.Job encoded
// before the lock was taken; it is encoded again only when c.Job changed a
// field the store keeps.
func (s *Store) applyOne(t *tx, c runnel.Change, body []byte, now int64) (bool, error) {
	sp := t.sp
	id, ok := parseID(c.Job.ID)
	if !ok {
		return false, nil
	}
	old, ok := sp.get(id)
	if !ok || old.version != c.Job.Version || (c.From != "" && old.state != stateCode(c.From)) {
		return false, nil
	}
	if !c.LeaseBefore.IsZero() && old.lease >= ms(c.LeaseBefore) {
		return false, nil
	}
	key, deps, err := s.keys(sp, &old)
	if err != nil {
		return false, err
	}
	if key != c.Job.UniqueKey || !slices.Equal(deps, c.Job.DependsOn) {
		j := *c.Job
		j.UniqueKey, j.DependsOn = key, deps
		body = encodeBody(&j)
	}
	h := sp.syms.hot(c.Job, &old)
	h.id, h.version, h.pending, h.created, h.flags = id, old.version+1, old.pending, old.created, old.flags
	t.put(h, body)
	if h.final() && !old.final() {
		return true, s.finalize(t, h, key, now)
	}
	return true, nil
}

// keys returns the unique key and dependencies of a job, reading its body only
// when it has them.
func (s *Store) keys(sp *space, h *job) (string, []string, error) {
	if h.flags == 0 {
		return "", nil, nil
	}
	b, err := sp.body(h, s.files)
	if err != nil {
		return "", nil, err
	}
	return keyOf(b)
}

// finalize records the event of a job reaching a final state, frees its unique
// key and settles the jobs that depend on it: they run once all their
// dependencies completed, and are cancelled when one did not.
func (s *Store) finalize(t *tx, h job, key string, now int64) error {
	sp := t.sp
	sp.seq++
	t.putEvent(event{seq: sp.seq, id: h.id, queue: h.queue, kind: h.kind, state: h.state, at: now})
	if key != "" {
		if u, ok := sp.uniques[key]; ok && u.id == h.id && u.until == 0 {
			t.delUnique(key)
		}
	}
	kids := sp.kids[h.id]
	if len(kids) == 0 {
		return nil
	}
	t.delKids(h.id)
	for _, id := range kids {
		k, ok := sp.get(id)
		if !ok || !k.is(stBlocked) {
			continue
		}
		k.version++
		k.updated = now
		switch {
		case !h.is(stCompleted):
			k.state, k.finished, k.startBy = stCancelled, now, 0
			t.putHot(k)
			kkey, _, err := s.keys(sp, &k)
			if err != nil {
				return err
			}
			if err := s.finalize(t, k, kkey, now); err != nil {
				return err
			}
			continue
		case k.pending > 1:
			k.pending--
		default:
			k.pending, k.state, k.waitingSince = 0, stWaiting, now
			k.runAt = max(k.runAt, now)
			k.startBy = 0
			if k.window > 0 {
				k.startBy = now + k.window
			}
		}
		t.putHot(k)
	}
	return nil
}

func (s *Store) Extend(_ context.Context, ns, worker string, ids []string, until time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var lost []string
	err := s.lease(ns, func(t *tx) error {
		w, known := t.sp.syms.find(worker)
		for _, s := range ids {
			id, ok := parseID(s)
			h, found := t.sp.get(id)
			if !ok || !found || !known || !h.is(stRunning) || h.worker != w {
				lost = append(lost, s)
				continue
			}
			h.lease = ms(until)
			t.putHot(h)
		}
		return nil
	})
	return lost, err
}
