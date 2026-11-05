package hybrid

import (
	"context"
	"fmt"
	"time"

	"github.com/kinoforge/runnel"
)

func (s *Store) Insert(_ context.Context, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	out := make([]runnel.Inserted, len(jobs))
	err := s.write(ns, func(t *tx) error {
		in := inserter{t: t, now: ms(now), remap: map[string]string{}}
		for i, j := range jobs {
			out[i] = in.one(j)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// inserter adds the jobs of one batch. remap points a job id of the batch at the
// job that stands for it: the held job for a duplicate, "" for a refused job.
type inserter struct {
	t     *tx
	now   int64
	remap map[string]string
}

func (in *inserter) refuse(j *runnel.Job, err error) runnel.Inserted {
	in.remap[j.ID] = ""
	return runnel.Inserted{ID: j.ID, Err: err}
}

func (in *inserter) one(j *runnel.Job) runnel.Inserted {
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
	in.t.put(in.t.sp.syms.hot(j), encodeBody(j))
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
		p, ok := in.t.sp.get(id)
		if !ok || p.is(stDead) || p.is(stCancelled) {
			return nil, nil, false
		}
		if !p.is(stCompleted) {
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
	return ""
}

func (in *inserter) full(j *runnel.Job) error {
	sp := in.t.sp
	q := sp.queueConfig(j.Queue)
	qs, known := sp.syms.find(j.Queue)
	if !known {
		return nil
	}
	if c := sp.counts[qs]; q.MaxQueued > 0 && c != nil && c[stWaiting]+c[stBlocked] >= int64(q.MaxQueued) {
		return runnel.ErrQueueFull
	}
	if j.Tenant != "" && q.TenantMaxQueued > 0 {
		if ts, ok := sp.syms.find(j.Tenant); ok && sp.queued[[2]sym{qs, ts}] >= q.TenantMaxQueued {
			return runnel.ErrTenantFull
		}
	}
	return nil
}
