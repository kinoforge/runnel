package hybrid_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid"
)

func TestRetriedJobFromSnapshotAndColdStoreReplaysOnce(t *testing.T) {
	spec.Rule(t, "X04", "M02")
	ctx := context.Background()
	opt := hybrid.Options{Dir: t.TempDir()}
	s, err := hybrid.OpenRetiring(opt, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c := runnel.NewClient(s, ns)
	res, err := c.Enqueue(ctx, "q", "k", nil, runnel.MaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	jobs, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 1, Lease: time.Minute, Now: now})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim %v %v", jobs, err)
	}
	dead := runnel.Decide(jobs[0], runnel.Outcome{Kind: runnel.OutcomeFail, Error: "boom"}, now)
	if ok, err := s.Apply(ctx, ns, []runnel.Change{{Job: dead, From: runnel.StateRunning}}, now); err != nil || !ok[0] {
		t.Fatalf("apply %v %v", ok, err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if s, err = hybrid.OpenRetiring(opt, time.Nanosecond, 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitCold(t, s, 1)
	_ = s.Close()
	saved := filepath.Join(t.TempDir(), "cold")
	if err := os.CopyFS(saved, os.DirFS(filepath.Join(opt.Dir, "done"))); err != nil {
		t.Fatal(err)
	}
	if s, err = hybrid.OpenRetiring(opt, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := runnel.NewClient(s, ns).Retry(ctx, res.ID); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if err := os.RemoveAll(filepath.Join(opt.Dir, "done")); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(filepath.Join(opt.Dir, "done"), os.DirFS(saved)); err != nil {
		t.Fatal(err)
	}
	if s, err = hybrid.OpenRetiring(opt, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	c = runnel.NewClient(s, ns)
	stats, err := c.Stats(ctx)
	if err != nil || len(stats) != 1 || stats[0].Counts[runnel.StateWaiting] != 1 || stats[0].Counts[runnel.StateDead] != 0 {
		t.Fatalf("after restart the retried job counts as %+v, %v", stats, err)
	}
	if dead, _, _ := c.List(ctx, runnel.Query{State: runnel.StateDead, Limit: 10}); len(dead) != 0 {
		t.Fatalf("the retried job is still listed as dead: %v", dead)
	}
	if j, err := c.Get(ctx, res.ID); err != nil || j.State != runnel.StateWaiting {
		t.Fatalf("retried job after restart %+v %v", j, err)
	}
}
