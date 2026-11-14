package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testWorkerResize(t *testing.T, open Opener) {
	spec.Rule(t, "R10")
	e := setup(t, open)
	batch := make([]*runnel.Job, 6)
	for i := range batch {
		batch[i], _ = runnel.NewJob("q", "k", nil)
	}
	if _, err := e.c.EnqueueJobs(e.ctx, batch); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	running, started, most, late := 0, 0, 0, 0
	gate := make(chan struct{}, len(batch))
	cfg := fast("q")
	cfg.Concurrency = 1
	w := runnel.NewWorker(e.s, e.ns, cfg)
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error {
		mu.Lock()
		running++
		started++
		most = max(most, running)
		if started > 3 {
			late = max(late, running)
		}
		mu.Unlock()
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})
	stop := runWorker(t, e, w)
	defer stop()
	count := func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		return started, most
	}
	waitFor(t, "one job running", 10*time.Second, func() bool { s, _ := count(); return s == 1 })
	if err := w.SetConcurrency(0); !errors.Is(err, runnel.ErrInvalid) || w.Concurrency() != 1 {
		t.Fatalf("concurrency 0: %v, now %d", err, w.Concurrency())
	}
	if err := w.SetConcurrency(3); err != nil || w.Concurrency() != 3 {
		t.Fatalf("grow: %v, now %d", err, w.Concurrency())
	}
	waitFor(t, "three jobs running", 10*time.Second, func() bool { s, _ := count(); return s == 3 })
	if err := w.SetConcurrency(1); err != nil {
		t.Fatal(err)
	}
	for range batch {
		gate <- struct{}{}
	}
	waitFor(t, "six jobs", 10*time.Second, func() bool {
		stats, _ := e.c.Stats(e.ctx)
		return len(stats) == 1 && stats[0].Counts[runnel.StateCompleted] == 6
	})
	mu.Lock()
	defer mu.Unlock()
	if most != 3 || late != 1 {
		t.Fatalf("ran %d at once after growing to 3, %d at once after shrinking to 1", most, late)
	}
}
