package hybrid_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid"
)

func TestLowDiskRefusesOnlyNewJobs(t *testing.T) {
	spec.Rule(t, "D03")
	ctx := context.Background()
	dir := t.TempDir()
	s := open(t, dir)
	held, err := runnel.NewClient(s, ns).Enqueue(ctx, "q", "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = hybrid.Open(hybrid.Options{Dir: dir, MinFreeBytes: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := runnel.NewClient(s, ns)
	if _, err := c.Enqueue(ctx, "q", "k", nil); !errors.Is(err, runnel.ErrNoSpace) {
		t.Fatalf("enqueue on a disk below the free space floor: %v, want ErrNoSpace", err)
	}
	now := time.Now()
	got, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 1, Lease: time.Minute, Now: now})
	if err != nil || len(got) != 1 || got[0].ID != held.ID {
		t.Fatalf("claim on a low disk: %v %v", got, err)
	}
	done := runnel.Decide(got[0], runnel.Outcome{Kind: runnel.OutcomeDone}, now)
	if ok, err := s.Apply(ctx, ns, []runnel.Change{{Job: done, From: runnel.StateRunning}}, now); err != nil || !ok[0] {
		t.Fatalf("finish on a low disk: %v %v", ok, err)
	}
	if j, err := c.Get(ctx, held.ID); err != nil || j.State != runnel.StateCompleted {
		t.Fatalf("job finished on a low disk: %+v %v", j, err)
	}
}
