package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func pipeline(t *testing.T, e *env, fail bool) map[string]runnel.Inserted {
	t.Helper()
	f := runnel.NewFlow()
	fetch, _ := runnel.NewJob("media", "fetch", nil)
	resize, _ := runnel.NewJob("media", "resize", map[string]bool{"fail": fail})
	publish, _ := runnel.NewJob("media", "publish", nil)
	f.Add("fetch", fetch).Add("resize", resize, "fetch").Add("publish", publish, "fetch", "resize")
	res, err := e.c.EnqueueFlow(e.ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func results(ctx context.Context) (map[string]string, error) {
	deps, err := runnel.Dependencies(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, d := range deps {
		var s string
		if err := json.Unmarshal(d.Result, &s); err != nil {
			return nil, runnel.Fail(err)
		}
		out[d.Kind] = s
	}
	return out, nil
}

func testStoryPipeline(t *testing.T, open Opener) {
	spec.Rule(t, "U08", "F01", "F03", "F06", "M05")
	e := setup(t, open)
	var mu sync.Mutex
	var order []string
	ran := func(kind string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, kind)
	}
	var published atomic.Value
	w := runnel.NewWorker(e.s, e.ns, fast("media"))
	w.HandleFunc("fetch", func(ctx context.Context, j *runnel.Job) error {
		ran("fetch")
		return j.SetResult("raw.png")
	})
	w.HandleFunc("resize", func(ctx context.Context, j *runnel.Job) error {
		ran("resize")
		var in struct{ Fail bool }
		_ = j.Bind(&in)
		if in.Fail {
			return runnel.Fail(errors.New("image is corrupt"))
		}
		got, err := results(ctx)
		if err != nil {
			return err
		}
		return j.SetResult("small-" + got["fetch"])
	})
	w.HandleFunc("publish", func(ctx context.Context, j *runnel.Job) error {
		ran("publish")
		got, err := results(ctx)
		if err != nil {
			return err
		}
		published.Store(got)
		return j.SetResult("ok")
	})
	stop := runWorker(t, e, w)
	defer stop()

	good := pipeline(t, e, false)
	waitFor(t, "pipeline published", 10*time.Second, e.state(good["publish"].ID, runnel.StateCompleted))
	mu.Lock()
	if !slices.Equal(order, []string{"fetch", "resize", "publish"}) {
		t.Errorf("steps ran in order %v", order)
	}
	order = nil
	mu.Unlock()
	if got, _ := published.Load().(map[string]string); got["fetch"] != "raw.png" || got["resize"] != "small-raw.png" || len(got) != 2 {
		t.Fatalf("publish read %v", got)
	}

	bad := pipeline(t, e, true)
	waitFor(t, "publish cancelled", 10*time.Second, e.state(bad["publish"].ID, runnel.StateCancelled))
	want := map[string]runnel.State{"fetch": runnel.StateCompleted, "resize": runnel.StateDead, "publish": runnel.StateCancelled}
	for step, st := range want {
		id := bad[step].ID
		if j := e.get(id); j.State != st {
			t.Errorf("%s ended %s, want %s", step, j.State, st)
		}
		evs := e.events(id)
		if len(evs) != 1 || evs[0].State != st {
			t.Errorf("%s has events %+v, want one %s", step, evs, st)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if slices.Contains(order, "publish") {
		t.Errorf("publish ran after resize failed: %v", order)
	}
}

func testStoryCancelExport(t *testing.T, open Opener) {
	spec.Rule(t, "U09", "L01", "L05", "M01", "M02", "F03")
	e := setup(t, open)
	var runs atomic.Int32
	started := make(chan struct{}, 2)
	stopped := make(chan bool, 1)
	w := runnel.NewWorker(e.s, e.ns, fast("exports"))
	w.HandleFunc("export", func(ctx context.Context, j *runnel.Job) error {
		if runs.Add(1) > 1 {
			return j.SetResult("export.csv")
		}
		started <- struct{}{}
		<-ctx.Done()
		stopped <- runnel.Cancelled(ctx) && errors.Is(context.Cause(ctx), runnel.ErrLost)
		return ctx.Err()
	})
	w.HandleFunc("notify", func(ctx context.Context, j *runnel.Job) error { return nil })
	stop := runWorker(t, e, w)
	defer stop()

	export, _ := e.c.Enqueue(e.ctx, "exports", "export", nil)
	notify, err := e.c.Enqueue(e.ctx, "exports", "notify", nil, runnel.After(export.ID))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("export never started")
	}
	if _, err := e.c.Cancel(e.ctx, export.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case lost := <-stopped:
		if !lost {
			t.Fatal("handler was not told its job was cancelled")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled export kept running")
	}
	if j := e.get(export.ID); j.State != runnel.StateCancelled || j.Worker != "" || j.FinishedAt.IsZero() {
		t.Fatalf("cancelled export %+v", j)
	}
	waitFor(t, "notify cancelled", 10*time.Second, e.state(notify.ID, runnel.StateCancelled))
	if _, err := e.c.Cancel(e.ctx, export.ID); err != nil {
		t.Fatalf("cancelling a cancelled export again: %v", err)
	}

	if _, err := e.c.Retry(e.ctx, export.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retried export completed", 10*time.Second, e.state(export.ID, runnel.StateCompleted))
	if j := e.get(export.ID); j.Attempt != 1 || string(j.Result) != `"export.csv"` {
		t.Fatalf("retried export attempt=%d result=%s", j.Attempt, j.Result)
	}
	if _, err := e.c.Cancel(e.ctx, export.ID); !errors.Is(err, runnel.ErrFinal) {
		t.Fatalf("cancelling a completed export: %v", err)
	}
}

func testStoryDeadlines(t *testing.T, open Opener) {
	spec.Rule(t, "U10", "T01", "T06", "C04")
	e := setup(t, open)
	stopMaintainer := e.maintain(t, runnel.MaintainerConfig{})
	defer stopMaintainer()
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "slow", Paused: true}); err != nil {
		t.Fatal(err)
	}
	stuck := e.add("slow", runnel.StartTimeout(300*time.Millisecond))
	waitFor(t, "stuck job expired", 10*time.Second, e.state(stuck.ID, runnel.StateExpired))
	j := e.get(stuck.ID)
	if len(j.Errors) != 1 || !strings.Contains(j.Errors[0].Error, "not started within") || j.Attempt != 0 || j.FinishedAt.IsZero() {
		t.Fatalf("expired job attempt=%d errors=%+v", j.Attempt, j.Errors)
	}
	if evs := e.events(stuck.ID); len(evs) != 1 || evs[0].State != runnel.StateExpired {
		t.Fatalf("expired job events %+v", evs)
	}

	cause := make(chan error, 1)
	w := runnel.NewWorker(e.s, e.ns, fast("live"))
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	})
	stopWorker := runWorker(t, e, w)
	defer stopWorker()
	late := e.add("live", runnel.TTL(time.Second))
	select {
	case c := <-cause:
		if !errors.Is(c, runnel.ErrDeadline) {
			t.Fatalf("running job's context ended with %v, want ErrDeadline", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("running job past its deadline was not stopped")
	}
	waitFor(t, "running job expired", 10*time.Second, func() bool { return e.get(late.ID).State.Final() })
	if j := e.get(late.ID); j.State != runnel.StateExpired || j.Worker != "" {
		t.Fatalf("job past its deadline ended %s on %q", j.State, j.Worker)
	}
}

func testStorySafeReenqueue(t *testing.T, open Opener) {
	spec.Rule(t, "U11", "J06", "J07")
	e := setup(t, open)
	first := e.add("orders", runnel.Unique("order-1001", 0))
	again := e.add("orders", runnel.Unique("order-1001", 0))
	if first.Duplicate || !again.Duplicate || again.ID != first.ID {
		t.Fatalf("retried enqueue: first %+v again %+v", first, again)
	}
	if n := e.count("orders", runnel.StateWaiting); n != 1 {
		t.Fatalf("%d jobs for one order", n)
	}
	for _, j := range e.claim([]string{"orders"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	if next := e.add("orders", runnel.Unique("order-1001", 0)); next.Duplicate || next.ID == first.ID {
		t.Fatalf("key still taken after the job finished: %+v", next)
	}

	window := 1500 * time.Millisecond
	before := time.Now()
	held := e.add("mail", runnel.Unique("digest", window))
	after := time.Now()
	for _, j := range e.claim([]string{"mail"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	dup := e.add("mail", runnel.Unique("digest", window))
	if took := time.Since(before); took >= window {
		t.Fatalf("enqueue, claim and finish took %v, longer than the %v window under test", took, window)
	}
	if !dup.Duplicate || dup.ID != held.ID {
		t.Fatalf("key free inside its window after the job finished: %+v", dup)
	}
	waitFor(t, "window over", 10*time.Second, func() bool { return time.Since(after) > window+200*time.Millisecond })
	if free := e.add("mail", runnel.Unique("digest", window)); free.Duplicate || free.ID == held.ID {
		t.Fatalf("key still taken after its window: %+v", free)
	}
}
