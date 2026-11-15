package storetest

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testStoryFlakyProvider(t *testing.T, open Opener) {
	spec.Rule(t, "U04", "O02", "O04", "O08", "O10", "M02")
	e := setup(t, open)
	var fixed atomic.Bool
	w := runnel.NewWorker(e.s, e.ns, fast("payments"))
	w.HandleFunc("charge", func(ctx context.Context, j *runnel.Job) error {
		if j.Attempt < 3 {
			return runnel.RetryIn(10*time.Millisecond, errors.New("provider answered 503"))
		}
		return j.SetResult("charged")
	})
	w.HandleFunc("refund", func(ctx context.Context, j *runnel.Job) error {
		if !fixed.Load() {
			return runnel.Fail(errors.New("card closed"))
		}
		return j.SetResult("refunded")
	})
	stop := runWorker(t, e, w)
	defer stop()

	charge, _ := e.c.Enqueue(e.ctx, "payments", "charge", nil)
	waitFor(t, "charge completed", 10*time.Second, e.state(charge.ID, runnel.StateCompleted))
	j := e.get(charge.ID)
	if j.Attempt != 3 || string(j.Result) != `"charged"` || len(j.Errors) != 2 {
		t.Fatalf("charge attempt=%d result=%s errors=%+v", j.Attempt, j.Result, j.Errors)
	}
	for i, er := range j.Errors {
		if er.Attempt != i+1 || er.Queue != "payments" || er.Kind != "charge" || er.At.IsZero() || !strings.Contains(er.Error, "503") {
			t.Errorf("error %d: %+v", i, er)
		}
	}

	refund, _ := e.c.Enqueue(e.ctx, "payments", "refund", nil)
	waitFor(t, "refund dead", 10*time.Second, e.state(refund.ID, runnel.StateDead))
	if j := e.get(refund.ID); j.Attempt != 1 || len(j.Errors) != 1 || j.Errors[0].Error != "card closed" || j.FinishedAt.IsZero() {
		t.Fatalf("failed refund %+v", j)
	}
	fixed.Store(true)
	if _, err := e.c.Retry(e.ctx, refund.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retried refund completed", 10*time.Second, e.state(refund.ID, runnel.StateCompleted))
	if j := e.get(refund.ID); j.Attempt != 1 || len(j.Errors) != 1 || string(j.Result) != `"refunded"` {
		t.Fatalf("retried refund attempt=%d errors=%+v result=%s", j.Attempt, j.Errors, j.Result)
	}
}

func testStoryAsyncRender(t *testing.T, open Opener) {
	spec.Rule(t, "U05", "O07", "C09")
	e := setup(t, open)
	var ready atomic.Bool
	var polls atomic.Int32
	w := runnel.NewWorker(e.s, e.ns, fast("renders"))
	w.Handle("video", runnel.Async(
		func(ctx context.Context, j *runnel.Job) error {
			var in struct{ Broken bool }
			_ = j.Bind(&in)
			if in.Broken {
				return runnel.Pending("task-broken", 10*time.Millisecond)
			}
			return runnel.Pending("task-1", 10*time.Millisecond)
		},
		func(ctx context.Context, j *runnel.Job) error {
			if j.Handle == "task-broken" {
				return runnel.Fail(errors.New("provider lost the task"))
			}
			polls.Add(1)
			if !ready.Load() {
				return runnel.Pending("", 20*time.Millisecond)
			}
			return j.SetResult("https://cdn.example.com/" + j.Handle + ".mp4")
		}))
	stop := runWorker(t, e, w)
	defer stop()

	res, _ := e.c.Enqueue(e.ctx, "renders", "video", nil)
	var seen []runnel.State
	waitFor(t, "render polled twice", 10*time.Second, func() bool {
		j := e.get(res.ID)
		if polls.Load() >= 2 && j.State == runnel.StatePolling {
			return true
		}
		if len(seen) == 0 || seen[len(seen)-1] != j.State {
			seen = append(seen, j.State)
			t.Logf("render %s attempt=%d handle=%q polls=%d run_at=%v worker=%q lease=%v errors=%+v", j.State, j.Attempt, j.Handle, polls.Load(), j.RunAt, j.Worker, j.LeaseUntil, j.Errors)
		}
		return false
	})
	if j := e.get(res.ID); j.Attempt != 1 || j.Handle != "task-1" || len(j.Errors) != 0 {
		t.Fatalf("polling render attempt=%d handle=%q errors=%+v", j.Attempt, j.Handle, j.Errors)
	}
	ready.Store(true)
	waitFor(t, "render completed", 10*time.Second, e.state(res.ID, runnel.StateCompleted))
	if j := e.get(res.ID); j.Attempt != 1 || string(j.Result) != `"https://cdn.example.com/task-1.mp4"` || j.Handle != "" {
		t.Fatalf("render attempt=%d result=%s handle=%q", j.Attempt, j.Result, j.Handle)
	}

	broken, _ := e.c.Enqueue(e.ctx, "renders", "video", map[string]bool{"broken": true})
	waitFor(t, "broken render dead", 10*time.Second, e.state(broken.ID, runnel.StateDead))
	if j := e.get(broken.ID); j.Attempt != 1 || len(j.Errors) != 1 || j.Errors[0].Error != "provider lost the task" {
		t.Fatalf("broken render attempt=%d errors=%+v", j.Attempt, j.Errors)
	}
}

func testStoryWorkerCrash(t *testing.T, open Opener) {
	spec.Rule(t, "U06", "L02", "O10", "O11")
	e := setup(t, open)
	res := e.add("q", quick())
	held, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "crashed", Limit: 1, Lease: 300 * time.Millisecond, Now: e.now()})
	if err != nil || len(held) != 1 || held[0].ID != res.ID {
		t.Fatalf("crashed worker claimed %v %v", held, err)
	}
	var ranOn atomic.Value
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
		ranOn.Store(runnel.JobFrom(ctx).Worker)
		return nil
	})
	stopWorker := runWorker(t, e, w)
	defer stopWorker()
	stopMaintainer := e.maintain(t, runnel.MaintainerConfig{})
	defer stopMaintainer()

	waitFor(t, "job taken back and completed", 10*time.Second, e.state(res.ID, runnel.StateCompleted))
	j := e.get(res.ID)
	if j.Attempt != 2 || len(j.Errors) != 1 || j.Errors[0].Attempt != 1 || !strings.Contains(j.Errors[0].Error, "crashed") {
		t.Fatalf("job after a crash attempt=%d errors=%+v", j.Attempt, j.Errors)
	}
	if on, _ := ranOn.Load().(string); on != w.ID() {
		t.Fatalf("job ran on %q, want the live worker %q", on, w.ID())
	}
	if j.Worker != "" || !j.LeaseUntil.IsZero() || j.FinishedAt.IsZero() {
		t.Fatalf("finished job keeps worker %q lease %v finished %v", j.Worker, j.LeaseUntil, j.FinishedAt)
	}
}

func testStoryGracefulDeploy(t *testing.T, open Opener) {
	spec.Rule(t, "U07", "R06")
	e := setup(t, open)
	started := make(chan struct{}, 1)
	var cause atomic.Value
	cfg := fast("q")
	cfg.ShutdownTimeout = 100 * time.Millisecond
	old := runnel.NewWorker(e.s, e.ns, cfg)
	old.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
		started <- struct{}{}
		<-ctx.Done()
		cause.Store(context.Cause(ctx))
		return ctx.Err()
	})
	stopOld := runWorker(t, e, old)
	res := e.add("q")
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job never started")
	}
	stopOld()
	if c, _ := cause.Load().(error); !errors.Is(c, runnel.ErrShutdown) {
		t.Fatalf("handler stopped by %v, want ErrShutdown", c)
	}
	j := e.get(res.ID)
	if j.State != runnel.StateWaiting || j.Attempt != 0 || len(j.Errors) != 0 || j.Worker != "" {
		t.Fatalf("job after shutdown state=%s attempt=%d errors=%+v worker=%q", j.State, j.Attempt, j.Errors, j.Worker)
	}

	next := runnel.NewWorker(e.s, e.ns, fast("q"))
	next.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error { return nil })
	stopNext := runWorker(t, e, next)
	defer stopNext()
	waitFor(t, "next worker completed the job", 10*time.Second, e.state(res.ID, runnel.StateCompleted))
	if j := e.get(res.ID); j.Attempt != 1 || len(j.Errors) != 0 {
		t.Fatalf("job after deploy attempt=%d errors=%+v", j.Attempt, j.Errors)
	}
}
