package hybrid_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

func TestCancelledJobsStayCancelledWhileSpillingRetiringAndCheckpointing(t *testing.T) {
	spec.Rule(t, "M01", "X04")
	const n = 400
	ctx := context.Background()
	opt := hybrid.Options{Dir: t.TempDir(), Sync: wal.SyncOff}
	s, err := hybrid.OpenSpilling(opt, 4, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	c := runnel.NewClient(s, ns)
	ids := make([]string, n)
	for i := range n {
		res, err := c.Enqueue(ctx, "q", "k", map[string]int{"n": i})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = res.ID
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.Checkpoint(); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	for i := 0; i < n; i += 2 {
		if _, err := c.Cancel(ctx, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if _, err := c.Enqueue(ctx, "q", "k", nil); err != nil {
		t.Fatalf("the store broke while spilling, retiring and checkpointing: %v", err)
	}
	_ = s.Close()
	if s, err = hybrid.OpenSpilling(opt, 4, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	cancelled := map[string]bool{}
	for i := 0; i < n; i += 2 {
		cancelled[ids[i]] = true
	}
	claimed := 0
	for range 100 {
		jobs, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 100, Lease: time.Minute, Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs {
			if cancelled[j.ID] {
				t.Fatalf("cancelled job %s was claimed", j.ID)
			}
		}
		claimed += len(jobs)
	}
	if claimed != n/2+1 {
		t.Fatalf("claimed %d jobs, want %d", claimed, n/2+1)
	}
	stats, err := runnel.NewClient(s, ns).Stats(ctx)
	if err != nil || stats[0].Counts[runnel.StateCancelled] != n/2 || stats[0].Counts[runnel.StateWaiting] != 0 {
		t.Fatalf("stats %+v %v", stats, err)
	}
}
