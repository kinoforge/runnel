package hybrid

import (
	"context"
	"time"

	"github.com/kinoforge/runnel"
)

func (s *Store) Insert(_ context.Context, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	out := make([]runnel.Inserted, len(jobs))
	err := s.write(ns, func(t *tx) error {
		ins := inserter{t: t, now: runnel.Millis(now), remap: map[string]string{}}
		for i, j := range jobs {
			out[i] = ins.one(j)
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

func (in *inserter) one(j *runnel.Job) runnel.Inserted {
	deps, wait, ok := in.deps(j.DependsOn)
	if !ok {
		in.remap[j.ID] = ""
		return runnel.Inserted{ID: j.ID, Err: runnel.ErrDependency}
	}
	if held := in.held(j); held != "" {
		in.remap[j.ID] = held
		return runnel.Inserted{ID: held, Duplicate: true}
	}
	if err := in.full(j); err != nil {
		in.remap[j.ID] = ""
		return runnel.Inserted{ID: j.ID, Err: err}
	}
	if j.UniqueKey != "" {
		u := unique{id: j.ID}
		if j.UniqueFor > 0 {
			u.until = in.now + j.UniqueFor.D().Milliseconds()
		}
		in.t.putUnique(j.UniqueKey, u)
	}
	j.DependsOn, j.Pending, j.State = deps, len(wait), runnel.StateWaiting
	if len(wait) > 0 {
		j.State, j.StartBy = runnel.StateBlocked, time.Time{}
	}
	in.t.putJob(j.Clone())
	for _, d := range wait {
		in.t.addKid(d, j.ID)
	}
	return runnel.Inserted{ID: j.ID}
}

// deps maps the dependency ids through the batch and returns them, those still
// to finish, and false when one is missing, dead or cancelled.
func (in *inserter) deps(ids []string) ([]string, []string, bool) {
	var deps, wait []string
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
		p := in.t.sp.jobs[d]
		if p == nil || p.State == runnel.StateDead || p.State == runnel.StateCancelled {
			return nil, nil, false
		}
		if p.State != runnel.StateCompleted {
			wait = append(wait, d)
		}
	}
	return deps, wait, true
}

// held returns the id of the job holding j's unique key, or of a job already
// stored under j's id.
func (in *inserter) held(j *runnel.Job) string {
	sp := in.t.sp
	if sp.jobs[j.ID] != nil {
		return j.ID
	}
	if j.UniqueKey == "" {
		return ""
	}
	u, ok := sp.uniques[j.UniqueKey]
	if !ok {
		return ""
	}
	if u.until > 0 {
		if u.until > in.now {
			return u.id
		}
		return ""
	}
	if h := sp.jobs[u.id]; h != nil && !h.State.Final() {
		return u.id
	}
	return ""
}

func (in *inserter) full(j *runnel.Job) error {
	sp := in.t.sp
	q := sp.queueConfig(j.Queue)
	if q.MaxQueued > 0 {
		c := sp.counts[j.Queue]
		if c[runnel.StateWaiting]+c[runnel.StateBlocked] >= int64(q.MaxQueued) {
			return runnel.ErrQueueFull
		}
	}
	if j.Tenant != "" && q.TenantMaxQueued > 0 && sp.queued[j.Queue+"|"+j.Tenant] >= q.TenantMaxQueued {
		return runnel.ErrTenantFull
	}
	return nil
}
