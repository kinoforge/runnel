package server_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
)

func TestRemoteSchedules(t *testing.T) {
	spec.Rule(t, "S12", "T02")
	srv, store := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/house", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := remote.New(srv.URL, key(t, srv, "house", "admin", "worker", "reader"), "house")
	if err := c.SetPool(ctx, runnel.PoolConfig{Name: "chores", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []runnel.Schedule{
		{Name: "sweep", Spec: "@every 1s", Queue: "house", Kind: "sweep", Pool: "chores"},
		{Name: "tick", Spec: "@every 1s", Queue: "house", Kind: "tick"},
	} {
		if err := c.SetSchedule(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.UpdateSchedule(ctx, "sweep", func(s *runnel.Schedule) { s.Timezone = "UTC" }); err != nil {
		t.Fatal(err)
	}
	all, err := c.Schedules(ctx)
	if err != nil || len(all) != 2 || all["sweep"].Timezone != "UTC" || all["sweep"].Pool != "chores" || all["tick"].Spec != "@every 1s" {
		t.Fatalf("schedules read back: %+v %v", all, err)
	}

	var running, most, sweeps, ticks atomic.Int32
	w := remote.NewWorker(c, runnel.WorkerConfig{Queues: []string{"house"}, Concurrency: 4, Lease: 5 * time.Second, PollInterval: 20 * time.Millisecond, ShutdownTimeout: time.Second})
	w.HandleFunc("sweep", func(ctx context.Context, j *runnel.Job) error {
		n := running.Add(1)
		defer running.Add(-1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		sweeps.Add(1)
		select {
		case <-ctx.Done():
		case <-time.After(1500 * time.Millisecond):
		}
		return nil
	})
	w.HandleFunc("tick", func(ctx context.Context, j *runnel.Job) error {
		ticks.Add(1)
		return nil
	})
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{}, 2)
	go func() { _ = w.Run(wctx); done <- struct{}{} }()
	go func() {
		_ = runnel.NewMaintainer(store, runnel.MaintainerConfig{Namespaces: []string{"house"}, Interval: 100 * time.Millisecond}).Run(wctx)
		done <- struct{}{}
	}()
	defer func() {
		stop()
		<-done
		<-done
	}()

	waitUntil(t, "the first tick", 10*time.Second, func() bool { return ticks.Load() >= 1 })
	first := ticks.Load()
	waitUntil(t, "three more ticks", 6*time.Second, func() bool { return ticks.Load() >= first+3 })
	waitUntil(t, "three sweeps", 15*time.Second, func() bool { return sweeps.Load() >= 3 })
	if m := most.Load(); m != 1 {
		t.Fatalf("a 1-slot pool ran %d sweeps at once", m)
	}

	if err := c.DeleteSchedule(ctx, "tick"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteSchedule(ctx, "tick"); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("deleting a missing schedule: %v, want ErrNotFound", err)
	}
	producer := remote.New(srv.URL, key(t, srv, "house", "producer"), "house")
	var apiErr *remote.APIError
	if err := producer.SetSchedule(ctx, runnel.Schedule{Name: "x", Spec: "@daily", Queue: "house", Kind: "x"}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden {
		t.Fatalf("producer set a schedule: %v", err)
	}
}

func waitUntil(t *testing.T, what string, within time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
