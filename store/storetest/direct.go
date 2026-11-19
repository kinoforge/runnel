package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testDirectShutdown(t *testing.T, open Opener) {
	spec.Rule(t, "W04", "R06")
	e := setup(t, open)
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.HandleFunc("k", func(context.Context, *runnel.Job) error { return runnel.RetryIn(time.Hour, errors.New("later")) })
	stop := runWorker(t, e, w)
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			j, _ := runnel.NewJob("q", "k", nil)
			if _, err := w.Do(ctx, j); !errors.Is(err, runnel.ErrInvalid) {
				returned <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	waitFor(t, "first attempt", 5*time.Second, func() bool {
		jobs, _, _ := e.c.List(e.ctx, runnel.Query{Queue: "q", State: runnel.StateWaiting, Limit: 2})
		return len(jobs) == 1 && jobs[0].Attempt == 1
	})
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop while Do waited for a retried job")
	}
	select {
	case err := <-returned:
		t.Fatalf("Do returned %v before its job was final", err)
	default:
	}
}

func testRetryCascaded(t *testing.T, open Opener) {
	spec.Rule(t, "M02", "F03")
	e := setup(t, open)
	a, b := e.add("q"), e.add("q")
	kid := e.add("q", runnel.After(a.ID, b.ID))
	for _, j := range e.claim([]string{"q"}, 2) {
		kind := runnel.OutcomeDone
		if j.ID == a.ID {
			kind = runnel.OutcomeFail
		}
		e.finish(j, runnel.Outcome{Kind: kind, Error: "no"})
	}
	if got, err := e.c.Get(e.ctx, kid.ID); err != nil || got.State != runnel.StateCancelled {
		t.Fatalf("child of a failed job: %+v %v", got, err)
	}
	if _, err := e.c.Retry(e.ctx, kid.ID); !errors.Is(err, runnel.ErrDependency) {
		t.Fatalf("retry before its dependency completed: %v", err)
	}
	if _, err := e.c.Retry(e.ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	again := e.claim([]string{"q"}, 2)
	if len(again) != 1 || again[0].ID != a.ID {
		t.Fatalf("claimed %+v", again)
	}
	e.finish(again[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	retried, err := e.c.Retry(e.ctx, kid.ID)
	if err != nil || retried.State != runnel.StateWaiting {
		t.Fatalf("retry once its dependencies completed: %+v %v", retried, err)
	}
	if got := e.claim([]string{"q"}, 2); len(got) != 1 || got[0].ID != kid.ID {
		t.Fatalf("retried child not claimable: %+v", got)
	}
}
