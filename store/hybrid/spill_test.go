package hybrid_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid"
)

func TestWaitingJobsLeaveMemory(t *testing.T) {
	const n = 3000
	opt := hybrid.Options{Dir: t.TempDir()}
	s, err := hybrid.OpenSpilling(opt, 1, 2*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	later := time.Now().Add(time.Hour)
	var jobs []*runnel.Job
	for i := range n {
		opts := []runnel.Option{runnel.Priority(i % 3)}
		if i%10 == 0 {
			opts = append(opts, runnel.Priority(5), runnel.At(later))
		}
		j, err := runnel.NewJob("q", "k", map[string]int{"n": i}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, j)
	}
	jobs[1].UniqueKey = "only"
	for part := range slices.Chunk(jobs, 1000) {
		if _, err := c.EnqueueJobs(ctx, part); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	waitCold(t, s, n-1)
	_ = s.Close()
	if s, err = hybrid.OpenSpilling(opt, 1, 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	c = runnel.NewClient(s, ns)
	waitCold(t, s, n-1)
	if st, _ := c.Stats(ctx); st[0].Counts[runnel.StateWaiting] != n {
		t.Fatalf("stats %+v", st)
	}
	if got, err := c.Get(ctx, jobs[7].ID); err != nil || got.State != runnel.StateWaiting {
		t.Fatalf("get %+v %v", got, err)
	}
	if page, _, _ := c.List(ctx, runnel.Query{State: runnel.StateWaiting, Limit: 10}); len(page) != 10 {
		t.Fatalf("listed %d", len(page))
	}
	dup, _ := runnel.NewJob("q", "k", nil)
	dup.UniqueKey = "only"
	if res, err := c.EnqueueJobs(ctx, []*runnel.Job{dup}); err != nil || !res[0].Duplicate || res[0].ID != jobs[1].ID {
		t.Fatalf("unique key held in the cold store: %+v %v", res, err)
	}
	if _, err := c.Cancel(ctx, jobs[2].ID); err != nil {
		t.Fatal(err)
	}
	if err := c.SetQueue(ctx, runnel.QueueConfig{Name: "q", MaxQueued: n - 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, "q", "k", nil); err == nil {
		t.Fatal("queue full of cold jobs took one more")
	}
	claimInOrder(t, s, now, n-n/10-1)
	claimInOrder(t, s, later, n/10)
}

func TestClaimOrderAcrossMemoryAndCold(t *testing.T) {
	s, err := hybrid.OpenSpilling(hybrid.Options{Dir: t.TempDir()}, 500, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	add := func(n, prio int) {
		for range n {
			if _, err := c.Enqueue(ctx, "q", "k", nil, runnel.Priority(prio)); err != nil {
				t.Fatal(err)
			}
		}
	}
	add(3000, 0)
	waitCold(t, s, 2500)
	add(300, 9)
	add(300, 0)
	add(300, 4)
	claimInOrder(t, s, time.Now(), 3900)
}

func claimInOrder(t *testing.T, s *hybrid.Store, now time.Time, want int) {
	t.Helper()
	var got []*runnel.Job
	for {
		batch, err := s.Claim(context.Background(), ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 97, Lease: time.Hour, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		got = append(got, batch...)
	}
	if len(got) != want {
		t.Fatalf("claimed %d at %v, want %d", len(got), now, want)
	}
	sorted := slices.IsSortedFunc(got, func(a, b *runnel.Job) int {
		if a.Priority != b.Priority {
			return b.Priority - a.Priority
		}
		if c := a.RunAt.Compare(b.RunAt); c != 0 {
			return c
		}
		return compareStr(a.ID, b.ID)
	})
	if !sorted {
		t.Fatal("claims out of order")
	}
}

func compareStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func TestSpillKeepsAParentThatGainsAKid(t *testing.T) {
	s, err := hybrid.OpenSpilling(hybrid.Options{Dir: t.TempDir()}, 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	late, err := c.Enqueue(ctx, "slow", "k", nil, runnel.StartTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var kid runnel.Inserted
	if _, err := s.SpillAround(ns, func() {
		if kid, err = c.Enqueue(ctx, "after", "k", nil, runnel.After(late.ID)); err != nil {
			t.Error(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if err := runnel.NewMaintainer(s, runnel.MaintainerConfig{Namespaces: []string{ns}}).Tick(ctx, ns); err != nil {
		t.Fatal(err)
	}
	if j, err := c.Get(ctx, late.ID); err != nil || j.State != runnel.StateExpired {
		t.Fatalf("parent %+v %v", j, err)
	}
	if j, err := c.Get(ctx, kid.ID); err != nil || j.State != runnel.StateCancelled {
		t.Fatalf("kid of an expired parent %+v %v", j, err)
	}
}

func TestClaimFindsJobsSpiltWhileItRuns(t *testing.T) {
	spec.Rule(t, "C01")
	later := time.Now().Add(time.Hour)
	for name, delayed := range map[string]bool{"delayed jobs now due": true, "jobs claimed by id": false} {
		t.Run(name, func(t *testing.T) {
			s, err := hybrid.OpenSpilling(hybrid.Options{Dir: t.TempDir()}, 1, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			var opts []runnel.Option
			if delayed {
				opts = append(opts, runnel.At(later))
			}
			req := runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 10, Lease: time.Hour, Now: later}
			for range 3 {
				res, err := runnel.NewClient(s, ns).Enqueue(context.Background(), "q", "k", nil, opts...)
				if err != nil {
					t.Fatal(err)
				}
				if !delayed {
					req.IDs = append(req.IDs, res.ID)
				}
			}
			var spilt int64
			got, err := s.ClaimAround(ns, req, func() {
				var serr error
				if spilt, serr = s.SpillAround(ns, func() {}); serr != nil {
					t.Error(serr)
				}
			})
			if err != nil || spilt != 3 || len(got) != 3 {
				t.Fatalf("claimed %d of the %d jobs spilt while the claim ran: %v", len(got), spilt, err)
			}
		})
	}
}
