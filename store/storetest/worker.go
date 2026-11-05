package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func runWorker(t *testing.T, e *env, w *runnel.Worker) func() {
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(ctx); err != nil {
			t.Errorf("worker: %v", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fast(queues ...string) runnel.WorkerConfig {
	return runnel.WorkerConfig{Queues: queues, Concurrency: 4, PollInterval: 20 * time.Millisecond, Lease: 3 * time.Second, ShutdownTimeout: time.Second}
}

func testWorkerEndToEnd(t *testing.T, open Opener) {
	spec.Rule(t, "O01", "R02")
	e := setup(t, open)
	var n atomic.Int32
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.HandleFunc("sum", func(ctx context.Context, j *runnel.Job) error {
		var in struct{ A, B int }
		if err := j.Bind(&in); err != nil {
			return runnel.Fail(err)
		}
		n.Add(1)
		return j.SetResult(map[string]int{"sum": in.A + in.B})
	})
	stop := runWorker(t, e, w)
	defer stop()
	res, err := e.c.Enqueue(e.ctx, "q", "sum", map[string]int{"A": 2, "B": 3})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "completion", 5*time.Second, func() bool { return e.get(res.ID).State == runnel.StateCompleted })
	if j := e.get(res.ID); string(j.Result) != `{"sum":5}` || j.Attempt != 1 {
		t.Fatalf("result %s attempt %d", j.Result, j.Attempt)
	}
	nh, _ := e.c.Enqueue(e.ctx, "q", "unknown", nil)
	waitFor(t, "dead unknown kind", 5*time.Second, func() bool { return e.get(nh.ID).State == runnel.StateDead })
}

func testWorkerFallback(t *testing.T, open Opener) {
	spec.Rule(t, "O02", "O05", "M02")
	e := setup(t, open)
	w := runnel.NewWorker(e.s, e.ns, fast("primary", "backup"))
	w.HandleFunc("flaky", func(ctx context.Context, j *runnel.Job) error { return errors.New("provider down") })
	w.HandleFunc("steady", func(ctx context.Context, j *runnel.Job) error { return j.SetResult(j.Queue) })
	stop := runWorker(t, e, w)
	defer stop()
	res, _ := e.c.Enqueue(e.ctx, "primary", "flaky", nil, runnel.MaxAttempts(2),
		runnel.WithBackoff(runnel.Backoff{Base: runnel.Duration(10 * time.Millisecond), Jitter: -1}),
		runnel.Fallbacks(runnel.Target{Queue: "backup", Kind: "steady"}))
	waitFor(t, "fallback completion", 5*time.Second, func() bool { return e.get(res.ID).State == runnel.StateCompleted })
	j := e.get(res.ID)
	if j.Queue != "backup" || j.Kind != "steady" || len(j.Errors) != 2 || string(j.Result) != `"backup"` {
		t.Fatalf("fallback job %+v", j)
	}
	if _, err := e.c.Retry(e.ctx, res.ID); !errors.Is(err, runnel.ErrNotFinal) {
		t.Fatalf("retried a completed job: %v", err)
	}
	dead, _ := e.c.Enqueue(e.ctx, "primary", "flaky", nil, runnel.MaxAttempts(1))
	waitFor(t, "dead", 5*time.Second, func() bool { return e.get(dead.ID).State == runnel.StateDead })
	if _, err := e.c.Retry(e.ctx, dead.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "dead again", 5*time.Second, func() bool {
		j := e.get(dead.ID)
		return j.State == runnel.StateDead && len(j.Errors) == 2
	})
}

func testWorkerBusy(t *testing.T, open Opener) {
	spec.Rule(t, "O06")
	e := setup(t, open)
	var calls atomic.Int32
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
		if calls.Add(1) < 3 {
			return runnel.Busy(20 * time.Millisecond)
		}
		return nil
	})
	stop := runWorker(t, e, w)
	defer stop()
	res, _ := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.MaxAttempts(1))
	first := e.get(res.ID).WaitingSince
	waitFor(t, "busy then done", 5*time.Second, func() bool { return e.get(res.ID).State == runnel.StateCompleted })
	j := e.get(res.ID)
	if j.Attempt != 1 || !j.WaitingSince.Equal(first) || len(j.Errors) != 0 {
		t.Fatalf("busy counted as attempt: attempt=%d waiting=%v first=%v errors=%d", j.Attempt, j.WaitingSince, first, len(j.Errors))
	}
}

func testWorkerAsync(t *testing.T, open Opener) {
	spec.Rule(t, "O07")
	e := setup(t, open)
	var polls atomic.Int32
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.Handle("render", runnel.Async(
		func(ctx context.Context, j *runnel.Job) error { return runnel.Pending("task-42", 10*time.Millisecond) },
		func(ctx context.Context, j *runnel.Job) error {
			if j.Handle != "task-42" {
				return runnel.Fail(fmt.Errorf("handle %q", j.Handle))
			}
			if polls.Add(1) < 3 {
				return runnel.Pending("", 10*time.Millisecond)
			}
			return j.SetResult("rendered")
		}))
	stop := runWorker(t, e, w)
	defer stop()
	res, _ := e.c.Enqueue(e.ctx, "q", "render", nil)
	waitFor(t, "async done", 5*time.Second, func() bool { return e.get(res.ID).State == runnel.StateCompleted })
	if j := e.get(res.ID); j.Attempt != 1 || string(j.Result) != `"rendered"` || polls.Load() != 3 {
		t.Fatalf("async job attempt=%d result=%s polls=%d", j.Attempt, j.Result, polls.Load())
	}
}

func testWorkerFlow(t *testing.T, open Opener) {
	spec.Rule(t, "F06")
	e := setup(t, open)
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.HandleFunc("num", func(ctx context.Context, j *runnel.Job) error {
		var n int
		_ = j.Bind(&n)
		return j.SetResult(n)
	})
	w.HandleFunc("add", func(ctx context.Context, j *runnel.Job) error {
		deps, err := runnel.Dependencies(ctx)
		if err != nil {
			return err
		}
		sum := 0
		for _, d := range deps {
			var n int
			_ = json.Unmarshal(d.Result, &n)
			sum += n
		}
		return j.SetResult(sum)
	})
	stop := runWorker(t, e, w)
	defer stop()
	f := runnel.NewFlow()
	for i := 1; i <= 4; i++ {
		j, _ := runnel.NewJob("q", "num", i)
		f.Add(fmt.Sprintf("n%d", i), j)
	}
	total, _ := runnel.NewJob("q", "add", nil)
	f.Add("total", total, "n1", "n2", "n3", "n4")
	res, err := e.c.EnqueueFlow(e.ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "fan-in", 5*time.Second, func() bool { return e.get(res["total"].ID).State == runnel.StateCompleted })
	if j := e.get(res["total"].ID); string(j.Result) != "10" {
		t.Fatalf("fan-in result %s", j.Result)
	}
}

func testWorkerCancel(t *testing.T, open Opener) {
	spec.Rule(t, "L05", "M01")
	e := setup(t, open)
	started := make(chan string, 1)
	stopped := make(chan bool, 1)
	cfg := fast("q")
	w := runnel.NewWorker(e.s, e.ns, cfg)
	w.HandleFunc("long", func(ctx context.Context, j *runnel.Job) error {
		started <- j.ID
		<-ctx.Done()
		stopped <- runnel.Cancelled(ctx)
		return ctx.Err()
	})
	stop := runWorker(t, e, w)
	defer stop()
	res, _ := e.c.Enqueue(e.ctx, "q", "long", nil)
	<-started
	if _, err := e.c.Cancel(e.ctx, res.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case lost := <-stopped:
		if !lost {
			t.Fatalf("handler not told the job was taken away")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("running job not stopped after cancel")
	}
	if j := e.get(res.ID); j.State != runnel.StateCancelled {
		t.Fatalf("state %s", j.State)
	}
}

func testWait(t *testing.T, open Opener) {
	spec.Rule(t, "W01", "W02")
	e := setup(t, open)
	res := e.add("q")
	go func() {
		time.Sleep(100 * time.Millisecond)
		for _, j := range e.claim([]string{"q"}, 1) {
			j.Result = json.RawMessage(`"ok"`)
			e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone, Result: j.Result})
		}
	}()
	began := time.Now()
	j, err := e.c.Wait(e.ctx, res.ID)
	if err != nil || j.State != runnel.StateCompleted || string(j.Result) != `"ok"` {
		t.Fatalf("wait: %+v %v", j, err)
	}
	if took := time.Since(began); took > 3*time.Second {
		t.Fatalf("wait noticed the end after %v", took)
	}
	other := e.add("q")
	ctx, cancel := context.WithTimeout(e.ctx, 150*time.Millisecond)
	defer cancel()
	j, err = e.c.Wait(ctx, other.ID)
	if !errors.Is(err, context.DeadlineExceeded) || j == nil || j.State != runnel.StateWaiting {
		t.Fatalf("wait past deadline: %+v %v", j, err)
	}
	if _, err := e.c.Wait(e.ctx, runnel.NewID()); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("wait on a missing job: %v", err)
	}
}

func testDirect(t *testing.T, open Opener) {
	spec.Rule(t, "W04", "W05")
	e := setup(t, open)
	w := runnel.NewWorker(e.s, e.ns, fast("q"))
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error { return j.SetResult(runnel.JobFrom(ctx).Worker) })
	j, _ := runnel.NewJob("q", "k", nil)
	if _, err := w.Do(e.ctx, j); err == nil {
		t.Fatalf("direct start on a stopped worker")
	}
	stop := runWorker(t, e, w)
	defer stop()
	var done *runnel.Job
	waitFor(t, "direct start", 5*time.Second, func() bool {
		j, _ := runnel.NewJob("q", "k", nil)
		var err error
		done, err = w.Do(e.ctx, j)
		return err == nil
	})
	if done.State != runnel.StateCompleted || string(done.Result) != strconv.Quote(w.ID()) || done.Attempt != 1 {
		t.Fatalf("direct start: %+v", done)
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Paused: true}); err != nil {
		t.Fatal(err)
	}
	j, _ = runnel.NewJob("q", "k", nil)
	ctx, cancel := context.WithTimeout(e.ctx, 1500*time.Millisecond)
	defer cancel()
	held, err := w.Do(ctx, j)
	if !errors.Is(err, context.DeadlineExceeded) || held == nil || held.State != runnel.StateWaiting {
		t.Fatalf("direct start ran a job on a paused queue: %+v %v", held, err)
	}
	for _, bad := range []*runnel.Job{{Queue: "q", Kind: "nobody"}, {Queue: "elsewhere", Kind: "k"}} {
		if _, err := w.Do(e.ctx, bad); err == nil {
			t.Fatalf("direct start accepted %s/%s", bad.Queue, bad.Kind)
		}
	}
}

func testWorkerConcurrency(t *testing.T, open Opener) {
	spec.Rule(t, "R01")
	e := setup(t, open)
	batch := make([]*runnel.Job, 6)
	for i := range batch {
		batch[i], _ = runnel.NewJob("q", "k", nil)
	}
	if _, err := e.c.EnqueueJobs(e.ctx, batch); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	running, most := 0, 0
	cfg := fast("q")
	cfg.Concurrency = 2
	w := runnel.NewWorker(e.s, e.ns, cfg)
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
		mu.Lock()
		running++
		most = max(most, running)
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})
	stop := runWorker(t, e, w)
	defer stop()
	waitFor(t, "six jobs", 10*time.Second, func() bool {
		stats, _ := e.c.Stats(e.ctx)
		return len(stats) == 1 && stats[0].Counts[runnel.StateCompleted] == 6
	})
	mu.Lock()
	defer mu.Unlock()
	if most != 2 {
		t.Fatalf("ran %d at once with concurrency 2", most)
	}
}

// testWorkerGather fills a worker's slots one job at a time: the last free
// slot is still claimed although it is fewer than a quarter of them.
func testWorkerGather(t *testing.T, open Opener) {
	spec.Rule(t, "R08")
	e := setup(t, open)
	cfg := fast("q")
	cfg.Concurrency = 8
	release := make(chan struct{})
	var started atomic.Int32
	w := runnel.NewWorker(e.s, e.ns, cfg)
	w.HandleFunc("k", func(ctx context.Context, _ *runnel.Job) error {
		started.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})
	stop := runWorker(t, e, w)
	defer stop()
	defer close(release)
	for i := range int32(cfg.Concurrency) {
		j, _ := runnel.NewJob("q", "k", nil)
		if _, err := e.c.EnqueueJobs(e.ctx, []*runnel.Job{j}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, fmt.Sprintf("job %d to start", i+1), 5*time.Second, func() bool { return started.Load() == i+1 })
	}
}
