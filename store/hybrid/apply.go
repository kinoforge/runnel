package hybrid

import (
	"context"
	"time"

	"github.com/kinoforge/runnel"
)

func (s *Store) Apply(_ context.Context, ns string, changes []runnel.Change, now time.Time) ([]bool, error) {
	out := make([]bool, len(changes))
	err := s.write(ns, func(t *tx) error {
		for i, c := range changes {
			out[i] = applyOne(t, c, runnel.Millis(now))
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
// pending, unique key, dependencies and creation time.
func applyOne(t *tx, c runnel.Change, now int64) bool {
	old := t.sp.jobs[c.Job.ID]
	if old == nil || old.Version != c.Job.Version || (c.From != "" && old.State != c.From) {
		return false
	}
	if !c.LeaseBefore.IsZero() && runnel.Millis(old.LeaseUntil) >= runnel.Millis(c.LeaseBefore) {
		return false
	}
	j := c.Job.Clone()
	j.Version = old.Version + 1
	j.Pending, j.UniqueKey, j.DependsOn, j.CreatedAt = old.Pending, old.UniqueKey, old.DependsOn, old.CreatedAt
	t.putJob(j)
	if j.State.Final() && !old.State.Final() {
		finalize(t, j, now)
	}
	return true
}

// finalize records the event of a job reaching a final state, frees its unique
// key and settles the jobs that depend on it: they run once all their
// dependencies completed, and are cancelled when one did not.
func finalize(t *tx, j *runnel.Job, now int64) {
	sp := t.sp
	sp.seq++
	t.putEvent(runnel.Event{Seq: sp.seq, Namespace: sp.ns, JobID: j.ID, Queue: j.Queue, Kind: j.Kind, State: j.State, At: runnel.FromMillis(now)})
	if j.UniqueKey != "" {
		if u, ok := sp.uniques[j.UniqueKey]; ok && u.id == j.ID && u.until == 0 {
			t.delUnique(j.UniqueKey)
		}
	}
	kids := sp.kids[j.ID]
	if len(kids) == 0 {
		return
	}
	t.delKids(j.ID)
	at := runnel.FromMillis(now)
	for _, id := range kids {
		k := sp.jobs[id]
		if k == nil || k.State != runnel.StateBlocked {
			continue
		}
		k = k.Clone()
		k.Version++
		k.UpdatedAt = at
		switch {
		case j.State != runnel.StateCompleted:
			k.State, k.FinishedAt, k.StartBy = runnel.StateCancelled, at, time.Time{}
			t.putJob(k)
			finalize(t, k, now)
			continue
		case k.Pending > 1:
			k.Pending--
		default:
			k.Pending = 0
			k.State, k.WaitingSince = runnel.StateWaiting, at
			if k.RunAt.Before(at) {
				k.RunAt = at
			}
			k.StartBy = time.Time{}
			if w := k.StartWindow(); w > 0 {
				k.StartBy = at.Add(w)
			}
		}
		t.putJob(k)
	}
}

func (s *Store) Extend(_ context.Context, ns, worker string, ids []string, until time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var lost []string
	err := s.write(ns, func(t *tx) error {
		for _, id := range ids {
			j := t.sp.jobs[id]
			if j == nil || j.State != runnel.StateRunning || j.Worker != worker {
				lost = append(lost, id)
				continue
			}
			j = j.Clone()
			j.LeaseUntil = until
			t.putJob(j)
		}
		return nil
	})
	return lost, err
}
