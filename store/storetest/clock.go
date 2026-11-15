package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

type skewClock struct{ by time.Duration }

func (c skewClock) Now() time.Time { return time.Now().Add(c.by) }

type skewedStore struct {
	runnel.Store
	clock runnel.Clock
}

func (s skewedStore) Clock() runnel.Clock { return s.clock }

func testDeadlineOnStoreClock(t *testing.T, open Opener) {
	spec.Rule(t, "K01", "T06")
	for _, by := range []time.Duration{-2 * time.Second, 2 * time.Second} {
		e := setup(t, open)
		e.s = skewedStore{Store: e.s, clock: skewClock{by: by}}
		e.c = runnel.NewClient(e.s, e.ns)
		type end struct {
			at    time.Time
			cause error
		}
		ended := make(chan end, 1)
		w := runnel.NewWorker(e.s, e.ns, fast("q"))
		w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
			<-ctx.Done()
			ended <- end{at: runnel.ClockOf(e.s).Now(), cause: context.Cause(ctx)}
			return ctx.Err()
		})
		stop := runWorker(t, e, w)
		res := e.add("q", runnel.TTL(time.Second))
		deadline := e.get(res.ID).Deadline
		select {
		case got := <-ended:
			if !errors.Is(got.cause, runnel.ErrDeadline) || got.at.Before(deadline) || got.at.After(deadline.Add(time.Second)) {
				t.Errorf("store clock %v off: handler stopped by %v at %v, deadline %v", by, got.cause, got.at, deadline)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("store clock %v off: handler never stopped", by)
		}
		waitFor(t, "job final", 10*time.Second, func() bool { return e.get(res.ID).State.Final() })
		if j := e.get(res.ID); j.State != runnel.StateExpired {
			t.Errorf("store clock %v off: job past its deadline ended %s, want expired", by, j.State)
		}
		stop()
	}
}
