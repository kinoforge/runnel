package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func (e *env) batch(queue string, n int, opts ...runnel.Option) {
	e.t.Helper()
	jobs := make([]*runnel.Job, n)
	for i := range jobs {
		var err error
		if jobs[i], err = runnel.NewJob(queue, "k", nil, opts...); err != nil {
			e.t.Fatal(err)
		}
	}
	res, err := e.c.EnqueueJobs(e.ctx, jobs)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, r := range res {
		if r.Err != nil {
			e.t.Fatalf("enqueue: %v", r.Err)
		}
	}
}

func testStoryNoisyCustomer(t *testing.T, open Opener) {
	spec.Rule(t, "U02", "C06", "C08", "R01")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "reports", GroupMaxRunning: 2}); err != nil {
		t.Fatal(err)
	}
	e.batch("reports", 30, runnel.Group("acme"))
	e.batch("reports", 2, runnel.Group("bolt"))
	m := newMeter()
	w := runnel.NewWorker(e.s, e.ns, fast("reports"))
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error { return m.run(ctx, j.Group) })
	stop := runWorker(t, e, w)
	defer stop()

	waitFor(t, "both customers running", 10*time.Second, func() bool {
		a, _ := m.counts("acme")
		b, _ := m.counts("bolt")
		return a == 2 && b == 2
	})
	if waiting := e.count("reports", runnel.StateWaiting); waiting != 28 {
		t.Fatalf("%d jobs waiting while the quiet customer ran, want 28", waiting)
	}
	m.open("bolt", 2)
	m.open("acme", 30)
	waitFor(t, "every report", 20*time.Second, func() bool { return e.count("reports", runnel.StateCompleted) == 32 })
	if _, most := m.counts("acme"); most != 2 {
		t.Fatalf("the flooding customer ran %d at once with a cap of 2", most)
	}
}

func testStoryProviderOutage(t *testing.T, open Opener) {
	spec.Rule(t, "U03", "C06", "C08", "C12", "X10")
	e := setup(t, open)
	if err := e.c.UpdatePool(e.ctx, "gpu", func(p *runnel.PoolConfig) { p.Slots = 2 }); err != nil {
		t.Fatal(err)
	}
	e.batch("images", 3, runnel.Pool("gpu"))
	e.batch("videos", 3, runnel.Pool("gpu"))
	e.batch("thumbs", 2, runnel.Pool("cpu"))
	m := newMeter()
	cfg := fast("images", "videos", "thumbs")
	cfg.Concurrency = 6
	w := runnel.NewWorker(e.s, e.ns, cfg)
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error { return m.run(ctx, j.Pool) })
	stop := runWorker(t, e, w)
	defer stop()

	waitFor(t, "both pools busy", 10*time.Second, func() bool {
		g, _ := m.counts("gpu")
		c, _ := m.counts("cpu")
		return g == 2 && c == 2
	})

	if err := e.c.UpdatePool(e.ctx, "gpu", func(p *runnel.PoolConfig) { p.Paused = true }); err != nil {
		t.Fatal(err)
	}
	if err := e.c.UpdatePool(e.ctx, "gpu", func(p *runnel.PoolConfig) { p.Slots = 3 }); err != nil {
		t.Fatal(err)
	}
	pools, err := e.c.Pools(e.ctx)
	if err != nil || !pools["gpu"].Paused || pools["gpu"].Slots != 3 {
		t.Fatalf("the app's new slots undid the operator's pause: %+v %v", pools["gpu"], err)
	}
	m.open("gpu", 2)
	waitFor(t, "running gpu jobs finished", 10*time.Second, func() bool {
		return e.count("images", runnel.StateCompleted)+e.count("videos", runnel.StateCompleted) == 2
	})
	e.batch("thumbs", 2, runnel.Pool("cpu"))
	m.open("cpu", 4)
	waitFor(t, "the cpu pool kept working", 10*time.Second, func() bool { return e.count("thumbs", runnel.StateCompleted) == 4 })
	if g, most := m.counts("gpu"); g != 2 || most != 2 {
		t.Fatalf("the gpu pool started %d jobs, %d at once; want the 2 from before the pause", g, most)
	}

	if err := e.c.UpdatePool(e.ctx, "gpu", func(p *runnel.PoolConfig) { p.Paused = false }); err != nil {
		t.Fatal(err)
	}
	m.open("gpu", 4)
	waitFor(t, "every gpu job", 10*time.Second, func() bool {
		return e.count("images", runnel.StateCompleted)+e.count("videos", runnel.StateCompleted) == 6
	})
	if _, most := m.counts("gpu"); most > 3 {
		t.Fatalf("the gpu pool ran %d at once with 3 slots", most)
	}
}
