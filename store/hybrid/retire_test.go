package hybrid_test

import (
	"context"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
)

// TestFinishedJobsLeaveMemory finishes jobs, waits for them to reach the cold
// store, and checks they read, list, count, survive a restart, come back on
// retry and go on prune.
func TestFinishedJobsLeaveMemory(t *testing.T) {
	dir := t.TempDir()
	opt := hybrid.Options{Dir: dir}
	s, err := hybrid.OpenRetiring(opt, time.Nanosecond, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	var ids []string
	for i := range 10 {
		j, err := c.Enqueue(ctx, "q", "k", map[string]int{"n": i})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	now := time.Now()
	jobs, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 10, Lease: time.Minute, Now: now})
	if err != nil || len(jobs) != 10 {
		t.Fatalf("claim %d %v", len(jobs), err)
	}
	var changes []runnel.Change
	for i, j := range jobs {
		o := runnel.Outcome{Kind: runnel.OutcomeDone}
		if i == 0 {
			o = runnel.Outcome{Kind: runnel.OutcomeFail, Error: "boom"}
		}
		changes = append(changes, runnel.Change{Job: runnel.Decide(j, o, now), From: runnel.StateRunning})
	}
	if _, err := s.Apply(ctx, ns, changes, now); err != nil {
		t.Fatal(err)
	}
	waitCold(t, s, 10)
	want := dump(t, s)
	_ = s.Close()
	if s, err = hybrid.OpenRetiring(opt, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if got := dump(t, s); got != want || s.Cold(ns) != 10 {
		t.Fatalf("after restart, %d cold\n got %s\nwant %s", s.Cold(ns), got, want)
	}
	c = runnel.NewClient(s, ns)
	got, err := c.Get(ctx, ids[0])
	if err != nil || got.State != runnel.StateCompleted && got.State != runnel.StateDead {
		t.Fatalf("get from cold %+v %v", got, err)
	}
	dead, _, _ := c.List(ctx, runnel.Query{State: runnel.StateDead, Limit: 10})
	if len(dead) != 1 {
		t.Fatalf("dead jobs listed %d", len(dead))
	}
	if _, err := c.Retry(ctx, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	if back, _ := c.Get(ctx, dead[0].ID); back.State != runnel.StateWaiting || s.Cold(ns) != 9 {
		t.Fatalf("retried job %s, %d cold", back.State, s.Cold(ns))
	}
	again, _ := runnel.NewJob("q", "k", nil)
	again.ID = ids[1]
	if res, err := c.EnqueueJobs(ctx, []*runnel.Job{again}); err != nil || !res[0].Duplicate {
		t.Fatalf("enqueue under a cold job's id: %+v %v", res, err)
	}
	if st, _ := c.Stats(ctx); st[0].Counts[runnel.StateCompleted] != 9 || st[0].Counts[runnel.StateWaiting] != 1 {
		t.Fatalf("duplicate of a cold job was added: %+v", st)
	}
	n, err := s.Prune(ctx, ns, runnel.PruneRequest{States: []runnel.State{runnel.StateCompleted}, Before: time.Now().Add(time.Hour), Limit: 100})
	if err != nil || n != 9 || s.Cold(ns) != 0 {
		t.Fatalf("pruned %d (%v), %d cold", n, err, s.Cold(ns))
	}
	evs, err := s.Events(ctx, ns, 3, 100)
	if err != nil || len(evs) != 7 || evs[0].Seq != 4 || evs[6].Seq != 10 {
		t.Fatalf("events after 3 from the cold store: %d %v", len(evs), err)
	}
	if n, err := s.PruneEvents(ctx, ns, runnel.EventPruneRequest{Before: time.Now().Add(time.Hour), UpTo: 6, Limit: 100}); err != nil || n != 6 {
		t.Fatalf("pruned %d events: %v", n, err)
	}
	if evs, _ = s.Events(ctx, ns, 0, 100); len(evs) != 4 || evs[0].Seq != 7 {
		t.Fatalf("events after prune %+v", evs)
	}
	now = time.Now()
	retried, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 1, Lease: time.Minute, Now: now})
	if err != nil || len(retried) != 1 {
		t.Fatalf("claim retried job: %d %v", len(retried), err)
	}
	done := runnel.Change{Job: runnel.Decide(retried[0], runnel.Outcome{Kind: runnel.OutcomeDone}, now), From: runnel.StateRunning}
	if _, err := s.Apply(ctx, ns, []runnel.Change{done}, now); err != nil {
		t.Fatal(err)
	}
	if evs, _ = s.Events(ctx, ns, 10, 10); len(evs) != 1 || evs[0].Seq != 11 {
		t.Fatalf("event numbers do not go on after the cold ones: %+v", evs)
	}
}

func waitCold(t *testing.T, s *hybrid.Store, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.Cold(ns) != n {
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs in the cold store, want %d", s.Cold(ns), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
