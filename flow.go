package runnel

import (
	"context"
	"slices"
)

// Flow is a set of jobs with dependencies between them, enqueued together. A
// step runs once the steps it waits for complete, and reads their results
// through Dependencies.
type Flow struct {
	steps []flowStep
	names map[string]int
	err   error
}

type flowStep struct {
	name     string
	job      *Job
	after    []string
	origDeps []string
}

func NewFlow() *Flow { return &Flow{names: map[string]int{}} }

// Add adds a step named name that waits for the steps in after. Errors are
// reported by EnqueueFlow.
func (f *Flow) Add(name string, j *Job, after ...string) *Flow {
	if f.err != nil {
		return f
	}
	if !ValidName(name) {
		f.err = invalid("step name %q is not valid", name)
		return f
	}
	if _, dup := f.names[name]; dup {
		f.err = invalid("step %q added twice", name)
		return f
	}
	if j == nil {
		f.err = invalid("step %q has no job", name)
		return f
	}
	f.names[name] = len(f.steps)
	f.steps = append(f.steps, flowStep{name: name, job: j, after: after, origDeps: slices.Clone(j.DependsOn)})
	return f
}

func (f *Flow) order() ([]int, error) {
	if f.err != nil {
		return nil, f.err
	}
	if len(f.steps) == 0 || len(f.steps) > maxBatch {
		return nil, invalid("a flow needs 1 to %d steps", maxBatch)
	}
	indeg := make([]int, len(f.steps))
	next := make([][]int, len(f.steps))
	for i, s := range f.steps {
		for _, a := range s.after {
			p, ok := f.names[a]
			if !ok {
				return nil, invalid("step %q waits on unknown step %q", s.name, a)
			}
			indeg[i]++
			next[p] = append(next[p], i)
		}
	}
	var queue, out []int
	for i, d := range indeg {
		if d == 0 {
			queue = append(queue, i)
		}
	}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		out = append(out, i)
		for _, n := range next[i] {
			if indeg[n]--; indeg[n] == 0 {
				queue = append(queue, n)
			}
		}
	}
	if len(out) != len(f.steps) {
		return nil, invalid("flow has a cycle")
	}
	return out, nil
}

// EnqueueFlow enqueues every step of a flow in one atomic call and returns each
// step's result by name. It refuses unknown steps and cycles.
func (c *Client) EnqueueFlow(ctx context.Context, f *Flow) (map[string]Inserted, error) {
	order, err := f.order()
	if err != nil {
		return nil, err
	}
	ids := make(map[string]string, len(f.steps))
	for _, s := range f.steps {
		s.job.ID = NewID()
		ids[s.name] = s.job.ID
	}
	jobs := make([]*Job, 0, len(order))
	for _, i := range order {
		s := f.steps[i]
		deps := slices.Clone(s.origDeps)
		for _, a := range s.after {
			deps = append(deps, ids[a])
		}
		s.job.DependsOn = deps
		jobs = append(jobs, s.job)
	}
	res, err := c.EnqueueJobs(ctx, jobs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Inserted, len(res))
	for k, i := range order {
		out[f.steps[i].name] = res[k]
	}
	return out, nil
}
