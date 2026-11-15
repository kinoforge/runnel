package storetest

import (
	"errors"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testCronOverlap(t *testing.T, open Opener) {
	spec.Rule(t, "T02")
	e := setup(t, open)
	bad := runnel.Schedule{Name: "chore", Spec: "@every 1s", Queue: "house", Kind: "k", Overlap: "sometimes"}
	if err := e.c.SetSchedule(e.ctx, bad); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("overlap %q accepted: %v", bad.Overlap, err)
	}
	bad.Overlap = runnel.OverlapSkip
	if err := e.c.SetSchedule(e.ctx, bad); err != nil {
		t.Fatal(err)
	}
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	tick := func(d time.Duration) {
		for until := time.Now().Add(d); time.Now().Before(until); time.Sleep(50 * time.Millisecond) {
			if err := m.Tick(e.ctx, e.ns); err != nil {
				t.Fatal(err)
			}
		}
	}
	tick(2500 * time.Millisecond)
	if n := e.total(); n != 1 {
		t.Fatalf("a skip schedule enqueued %d jobs while its first job waited, want 1", n)
	}
	held := e.claim([]string{"house"}, 1)
	if len(held) != 1 {
		t.Fatalf("claimed %d scheduled jobs", len(held))
	}
	tick(2 * time.Second)
	if n := e.total(); n != 1 {
		t.Fatalf("a skip schedule enqueued %d jobs while its job ran, want 1", n)
	}
	e.finish(held[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	tick(2 * time.Second)
	if done, waiting := e.count("house", runnel.StateCompleted), e.count("house", runnel.StateWaiting); done != 1 || waiting != 1 {
		t.Fatalf("after the job finished: %d completed, %d waiting, want 1 and 1", done, waiting)
	}
}
