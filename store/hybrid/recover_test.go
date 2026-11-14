package hybrid_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const ns = "acme"

func open(t *testing.T, dir string) *hybrid.Store {
	t.Helper()
	s, err := hybrid.Open(hybrid.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fill(t *testing.T, s runnel.Store) {
	t.Helper()
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	if err := c.SetQueue(ctx, runnel.QueueConfig{Name: "q", MaxRunning: 5, Rate: 100}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPool(ctx, runnel.PoolConfig{Name: "mail-1", Slots: 2}); err != nil {
		t.Fatal(err)
	}
	first, err := c.Enqueue(ctx, "q", "k", map[string]int{"n": 1}, runnel.Pool("mail-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, "q", "k", nil, runnel.After(first.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, "q", "k", nil, runnel.Unique("u1", time.Hour)); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	jobs, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 2, Lease: time.Minute, Now: now})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("claim %d %v", len(jobs), err)
	}
	done := runnel.Decide(jobs[0], runnel.Outcome{Kind: runnel.OutcomeDone}, now)
	if ok, err := s.Apply(ctx, ns, []runnel.Change{{Job: done}}, now); err != nil || !ok[0] {
		t.Fatalf("apply %v %v", ok, err)
	}
	if _, err := s.SwapCursor(ctx, ns, "hooks", 0, 1); err != nil {
		t.Fatal(err)
	}
}

func dump(t *testing.T, s runnel.Store) string {
	t.Helper()
	ctx := context.Background()
	jobs, _, err1 := s.List(ctx, ns, runnel.Query{Limit: 1000})
	stats, err2 := s.Stats(ctx, ns)
	events, err3 := s.Events(ctx, ns, 0, 100)
	queues, err4 := s.Configs(ctx, ns, runnel.ConfigQueue)
	cur, err5 := s.Cursor(ctx, ns, "hooks")
	if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal([]any{jobs, stats, events, queues, cur})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRestartReplaysLog(t *testing.T) {
	spec.Rule(t, "X04")
	dir := t.TempDir()
	s := open(t, dir)
	fill(t, s)
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

func TestRestartFromSnapshot(t *testing.T) {
	spec.Rule(t, "X04")
	dir := t.TempDir()
	s := open(t, dir)
	fill(t, s)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	c := runnel.NewClient(s, ns)
	if _, err := c.Enqueue(context.Background(), "q", "after", nil); err != nil {
		t.Fatal(err)
	}
	want := dump(t, s)
	_ = s.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	snaps, _ := filepath.Glob(filepath.Join(dir, "snap-*.bin"))
	if len(snaps) != 1 || len(segs) != 1 {
		t.Fatalf("checkpoint left %d snapshots and %d segments", len(snaps), len(segs))
	}
	s = open(t, dir)
	defer s.Close()
	if got := dump(t, s); got != want {
		t.Fatalf("after snapshot restart\n got %s\nwant %s", got, want)
	}
	c = runnel.NewClient(s, ns)
	if _, err := c.Enqueue(context.Background(), "q", "k", nil, runnel.Unique("u1", time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Stats(context.Background()); got[0].Counts[runnel.StateWaiting] != 2 {
		t.Fatalf("unique key lost in snapshot: %+v", got)
	}
}

func TestTornTailIsCut(t *testing.T) {
	spec.Rule(t, "X04")
	dir := t.TempDir()
	s := open(t, dir)
	fill(t, s)
	want := dump(t, s)
	_ = s.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	f, err := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{200, 0, 0, 0, 1, 2, 3})
	_ = f.Close()
	s = open(t, dir)
	if got := dump(t, s); got != want {
		t.Fatalf("after torn tail\n got %s\nwant %s", got, want)
	}
	_ = s.Close()
	s = open(t, dir)
	defer s.Close()
	if got := dump(t, s); got != want {
		t.Fatal("second restart after torn tail differs")
	}
}

func TestDirectoryLocked(t *testing.T) {
	spec.Rule(t, "X05")
	dir := t.TempDir()
	s := open(t, dir)
	defer s.Close()
	if _, err := hybrid.Open(hybrid.Options{Dir: dir}); !errors.Is(err, wal.ErrLocked) {
		t.Fatalf("second open: %v", err)
	}
}
