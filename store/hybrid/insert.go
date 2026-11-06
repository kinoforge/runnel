package hybrid

import (
	"context"
	"fmt"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

func (s *Store) Insert(_ context.Context, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	out := make([]runnel.Inserted, len(jobs))
	bodies := make([][]byte, len(jobs))
	for i, j := range jobs {
		if len(j.DependsOn) == 0 {
			bodies[i] = encodeBody(j)
		}
	}
	if err := s.makeRoom(); err != nil {
		return nil, err
	}
	known, gen, err := s.coldKnown(ns, jobs)
	if err != nil {
		return nil, err
	}
	err = s.write(ns, func(t *tx) error {
		in := inserter{s: s, t: t, now: ms(now), remap: map[string]string{}, known: known, gen: gen}
		for i, j := range jobs {
			out[i] = in.one(j, bodies[i])
		}
		return in.err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// inserter adds the jobs of one batch. remap points a job id of the batch at the
// job that stands for it: the held job for a duplicate, "" for a refused job.
type inserter struct {
	s     *Store
	t     *tx
	now   int64
	remap map[string]string
	// known holds the states of the batch's ids and dependencies found in the
	// cold store before the lock was taken, valid while no job left memory
	// since: gen is the space's retired count then.
	known map[uid]uint8
	gen   uint64
	err   error
}

// coldKnown looks up in the cold store the ids and dependencies of jobs,
// before the namespace lock is taken.
func (s *Store) coldKnown(ns string, jobs []*runnel.Job) (map[uid]uint8, uint64, error) {
	if s.cold == nil {
		return nil, 0, nil
	}
	sp := s.space(ns)
	gen := sp.retired.Load()
	known := map[uid]uint8{}
	look := func(raw string) error {
		id, ok := parseID(raw)
		if !ok || !sp.mayBeCold(id) {
			return nil
		}
		j, found, err := s.cold.Meta(ns, cold.ID(id))
		if found {
			known[id] = j.State
		}
		return err
	}
	for _, j := range jobs {
		if err := look(j.ID); err != nil {
			return nil, 0, err
		}
		for _, d := range j.DependsOn {
			if err := look(d); err != nil {
				return nil, 0, err
			}
		}
	}
	return known, gen, nil
}

// coldState returns the state of a job only the cold store holds.
func (in *inserter) coldState(id uid) (uint8, bool) {
	sp := in.t.sp
	if in.s.cold == nil || !sp.mayBeCold(id) {
		return 0, false
	}
	if sp.retired.Load() == in.gen {
		st, ok := in.known[id]
		return st, ok
	}
	return in.lookup(id)
}

// lookup reads the state of id from the cold store.
func (in *inserter) lookup(id uid) (uint8, bool) {
	if in.s.cold == nil || !in.t.sp.mayBeCold(id) {
		return 0, false
	}
	j, ok, err := in.s.cold.Meta(in.t.sp.ns, cold.ID(id))
	if err != nil && in.err == nil {
		in.err = err
	}
	return j.State, ok
}

func (in *inserter) refuse(j *runnel.Job, err error) runnel.Inserted {
	in.remap[j.ID] = ""
	return runnel.Inserted{ID: j.ID, Err: err}
}

// one adds j. body is j encoded before the lock was taken, or nil when its
// dependencies may still be remapped.
func (in *inserter) one(j *runnel.Job, body []byte) runnel.Inserted {
	id, ok := parseID(j.ID)
	if !ok {
		return in.refuse(j, fmt.Errorf("%w: job id %q is not a canonical UUID", runnel.ErrInvalid, j.ID))
	}
	deps, wait, ok := in.deps(j.DependsOn)
	if !ok {
		return in.refuse(j, runnel.ErrDependency)
	}
	if held := in.held(id, j.UniqueKey); held != "" {
		in.remap[j.ID] = held
		return runnel.Inserted{ID: held, Duplicate: true}
	}
	if err := in.full(j); err != nil {
		return in.refuse(j, err)
	}
	if j.UniqueKey != "" {
		u := unique{id: id}
		if j.UniqueFor > 0 {
			u.until = in.now + j.UniqueFor.D().Milliseconds()
		}
		in.t.putUnique(j.UniqueKey, u)
	}
	j.DependsOn, j.Pending, j.State = deps, len(wait), runnel.StateWaiting
	if len(wait) > 0 {
		j.State, j.StartBy = runnel.StateBlocked, time.Time{}
	}
	if body == nil {
		body = encodeBody(j)
	}
	in.t.put(in.t.sp.syms.hot(j, nil), body)
	for _, d := range wait {
		in.t.addKid(d, id)
	}
	return runnel.Inserted{ID: j.ID}
}

// deps maps the dependency ids through the batch and returns them, those still
// to finish, and false when one is missing, dead or cancelled.
func (in *inserter) deps(ids []string) ([]string, []uid, bool) {
	var deps []string
	var wait []uid
	seen := map[string]bool{}
	for _, d := range ids {
		if m, ok := in.remap[d]; ok {
			if m == "" {
				return nil, nil, false
			}
			d = m
		}
		if seen[d] {
			continue
		}
		seen[d] = true
		deps = append(deps, d)
		id, _ := parseID(d)
		st := uint8(0)
		if p, ok := in.t.sp.get(id); ok {
			st = p.state
		} else if c, ok := in.coldState(id); ok {
			st = c
		}
		if st == 0 || st == stDead || st == stCancelled {
			return nil, nil, false
		}
		if st != stCompleted {
			wait = append(wait, id)
		}
	}
	return deps, wait, true
}

// held returns the id of a job already stored under id, or of the job holding
// the unique key.
func (in *inserter) held(id uid, key string) string {
	sp := in.t.sp
	if _, ok := sp.get(id); ok {
		return id.String()
	}
	if _, ok := in.coldState(id); ok {
		return id.String()
	}
	if key == "" {
		return ""
	}
	u, ok := sp.uniques[key]
	switch {
	case !ok:
		return ""
	case u.until > 0:
		if u.until > in.now {
			return u.id.String()
		}
		return ""
	}
	if h, ok := sp.get(u.id); ok && !h.final() {
		return u.id.String()
	}
	if st, ok := in.lookup(u.id); ok && !final(st) {
		return u.id.String()
	}
	return ""
}

func (in *inserter) full(j *runnel.Job) error {
	sp := in.t.sp
	q := sp.queueConfig(j.Queue)
	qs, known := sp.syms.find(j.Queue)
	if !known {
		return nil
	}
	if q.MaxQueued > 0 {
		n := int64(0)
		if c := sp.counts[qs]; c != nil {
			n += c[stWaiting] + c[stBlocked]
		}
		if c := sp.coldCounts[j.Queue]; c != nil {
			n += c[stWaiting]
		}
		if n >= int64(q.MaxQueued) {
			return runnel.ErrQueueFull
		}
	}
	if j.Tenant != "" && q.TenantMaxQueued > 0 {
		n := sp.coldQueued[[2]string{j.Queue, j.Tenant}]
		if ts, ok := sp.syms.find(j.Tenant); ok {
			n += int64(sp.queued[[2]sym{qs, ts}])
		}
		if n >= int64(q.TenantMaxQueued) {
			return runnel.ErrTenantFull
		}
	}
	return nil
}
