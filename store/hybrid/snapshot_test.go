package hybrid_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func TestSnapshotUnderLoad(t *testing.T) {
	spec.Rule(t, "X04")
	dir := t.TempDir()
	s := open(t, dir)
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := range 3 {
		wg.Go(func() {
			for i := 0; i < 150 && !stop.Load(); i++ {
				first, err := c.Enqueue(ctx, "q", "k", map[string]int{"g": g, "i": i})
				if err == nil {
					_, err = c.Enqueue(ctx, "q", "k", nil, runnel.After(first.ID), runnel.Unique(first.ID, 0))
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for !stop.Load() {
			now := time.Now()
			jobs, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 7, Lease: time.Minute, Now: now})
			if err != nil {
				t.Error(err)
				return
			}
			changes := make([]runnel.Change, 0, len(jobs))
			for i, j := range jobs {
				if i%3 != 0 {
					changes = append(changes, runnel.Change{Job: runnel.Decide(j, runnel.Outcome{Kind: runnel.OutcomeDone}, now), From: runnel.StateRunning})
				}
			}
			if _, err := s.Apply(ctx, ns, changes, now); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range 10 {
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	want := dump(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	defer s.Close()
	if got := dump(t, s); got != want {
		t.Fatalf("after restart\n got %s\nwant %s", got, want)
	}
}
