package storetest

import (
	"errors"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testPrune(t *testing.T, open Opener) {
	spec.Rule(t, "T03")
	e := setup(t, open)
	for range 3 {
		e.add("q", "k")
	}
	got := e.claim([]string{"q"}, 2)
	for _, j := range got {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	n, err := e.s.Prune(e.ctx, e.ns, runnel.PruneRequest{States: []runnel.State{runnel.StateCompleted}, Before: e.now().Add(time.Second), Limit: 10, Archive: true})
	if err != nil || n != 2 {
		t.Fatalf("pruned %d %v", n, err)
	}
	if _, err := e.c.Get(e.ctx, got[0].ID); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("pruned job still there")
	}
	stats, _ := e.c.Stats(e.ctx)
	if len(stats) != 1 || stats[0].Counts[runnel.StateWaiting] != 1 || stats[0].Counts[runnel.StateCompleted] != 0 {
		t.Fatalf("stats after prune %+v", stats)
	}
	if _, err := e.s.Prune(e.ctx, e.ns, runnel.PruneRequest{States: []runnel.State{runnel.StateWaiting}, Before: e.now(), Limit: 1}); err == nil {
		t.Fatalf("pruning waiting jobs allowed")
	}
}

func testEvents(t *testing.T, open Opener) {
	spec.Rule(t, "M01", "M05", "M06")
	e := setup(t, open)
	e.add("q", "k")
	e.add("q", "k")
	got := e.claim([]string{"q"}, 2)
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	if _, err := e.c.Cancel(e.ctx, got[1].ID); err != nil {
		t.Fatal(err)
	}
	if j, err := e.c.Cancel(e.ctx, got[1].ID); err != nil || j.State != runnel.StateCancelled {
		t.Fatalf("cancelling twice: %v", err)
	}
	if _, err := e.c.Cancel(e.ctx, got[0].ID); !errors.Is(err, runnel.ErrFinal) {
		t.Fatalf("cancelled a completed job: %v", err)
	}
	evs, err := e.c.Events(e.ctx, 0, 10)
	if err != nil || len(evs) != 2 {
		t.Fatalf("events %+v %v", evs, err)
	}
	if evs[0].State != runnel.StateCompleted || evs[1].State != runnel.StateCancelled || evs[1].Seq <= evs[0].Seq {
		t.Fatalf("event order %+v", evs)
	}
	if more, _ := e.c.Events(e.ctx, evs[0].Seq, 10); len(more) != 1 {
		t.Fatalf("after cursor %+v", more)
	}
	ok, err := e.s.SwapCursor(e.ctx, e.ns, "c", 0, 5)
	if err != nil || !ok {
		t.Fatalf("first swap %v %v", ok, err)
	}
	if ok, _ := e.s.SwapCursor(e.ctx, e.ns, "c", 0, 6); ok {
		t.Fatalf("stale swap won")
	}
	if ok, _ := e.s.SwapCursor(e.ctx, e.ns, "c", 5, 9); !ok {
		t.Fatalf("swap lost")
	}
	if v, _ := e.s.Cursor(e.ctx, e.ns, "c"); v != 9 {
		t.Fatalf("cursor %d", v)
	}
}

func testMaintainer(t *testing.T, open Opener) {
	spec.Rule(t, "L02", "T01")
	e := setup(t, open)
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	e.add("q", "k", runnel.MaxAttempts(3))
	got, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "ghost", Limit: 1, Lease: 50 * time.Millisecond, Now: e.now()})
	if err != nil || len(got) != 1 {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	j := e.get(got[0].ID)
	if j.State != runnel.StateWaiting || j.Attempt != 1 || len(j.Errors) != 1 {
		t.Fatalf("not reaped: %+v", j)
	}
	moved := e.add("slow", "k", runnel.StartWithin(50*time.Millisecond), runnel.Fallbacks(runnel.Target{Queue: "fast"}))
	time.Sleep(80 * time.Millisecond)
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	if j := e.get(moved.ID); j.Queue != "fast" || j.State != runnel.StateWaiting || !j.StartBy.IsZero() {
		t.Fatalf("not moved to fallback: %+v", j)
	}
}

func testCron(t *testing.T, open Opener) {
	spec.Rule(t, "T02")
	e := setup(t, open)
	if err := e.c.SetSchedule(e.ctx, runnel.Schedule{Name: "tick", Spec: "@every 1s", Queue: "cron", Kind: "tick"}); err != nil {
		t.Fatal(err)
	}
	m1 := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	m2 := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	began := time.Now()
	_ = m1.Tick(e.ctx, e.ns)
	deadline := began.Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_ = m1.Tick(e.ctx, e.ns)
		_ = m2.Tick(e.ctx, e.ns)
		time.Sleep(50 * time.Millisecond)
	}
	most := int(time.Since(began)/time.Second) + 1
	jobs, _, err := e.c.List(e.ctx, runnel.Query{Queue: "cron", Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) < 1 || len(jobs) > most {
		t.Fatalf("two maintainers fired an every-second schedule %d times in %v", len(jobs), time.Since(began))
	}
	seen := map[string]bool{}
	for _, j := range jobs {
		if seen[j.Meta["fire_at"]] {
			t.Fatalf("duplicate fire %s", j.Meta["fire_at"])
		}
		seen[j.Meta["fire_at"]] = true
	}
}

func testEventsOutlivePrune(t *testing.T, open Opener) {
	spec.Rule(t, "T04")
	e := setup(t, open)
	for range 4 {
		e.add("q", "k")
	}
	for _, j := range e.claim([]string{"q"}, 4) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	later := e.now().Add(time.Minute)
	if n, err := e.s.Prune(e.ctx, e.ns, runnel.PruneRequest{States: []runnel.State{runnel.StateCompleted}, Before: later, Limit: 10}); err != nil || n != 4 {
		t.Fatalf("pruned %d %v", n, err)
	}
	evs, err := e.c.Events(e.ctx, 0, 10)
	if err != nil || len(evs) != 4 {
		t.Fatalf("job prune took events with it: %d %v", len(evs), err)
	}
	if n, err := e.s.PruneEvents(e.ctx, e.ns, runnel.EventPruneRequest{UpTo: evs[1].Seq, Before: later, Limit: 10}); err != nil || n != 2 {
		t.Fatalf("pruned %d events up to the cursor, %v", n, err)
	}
	if n, _ := e.s.PruneEvents(e.ctx, e.ns, runnel.EventPruneRequest{Before: e.now().Add(-time.Hour), Limit: 10}); n != 0 {
		t.Fatalf("pruned %d fresh events", n)
	}
	left, _ := e.c.Events(e.ctx, 0, 10)
	if len(left) != 2 || left[0].Seq != evs[2].Seq {
		t.Fatalf("events left %+v", left)
	}
	if n, _ := e.s.PruneEvents(e.ctx, e.ns, runnel.EventPruneRequest{Before: later, Limit: 1}); n != 1 {
		t.Fatalf("limit ignored: %d", n)
	}
}

func testMaintainerEvents(t *testing.T, open Opener) {
	spec.Rule(t, "T03", "T05")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "short", Retention: runnel.Duration(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		e.add("short", "k")
	}
	for _, j := range e.claim([]string{"short"}, 3) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	all, _ := e.c.Events(e.ctx, 0, 10)
	time.Sleep(20 * time.Millisecond)
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}, Batch: 1, Webhooks: runnel.WebhookConfig{Enabled: true},
		Retention: runnel.Retention{Completed: time.Millisecond, Events: time.Millisecond}})
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	if stats, _ := e.c.Stats(e.ctx); len(stats) > 0 && stats[0].Counts[runnel.StateCompleted] != 0 {
		t.Fatalf("jobs not pruned %+v", stats)
	}
	cur, _ := e.s.Cursor(e.ctx, e.ns, "webhooks")
	left, _ := e.c.Events(e.ctx, 0, 10)
	if len(all) != 3 || cur != all[0].Seq || len(left) != 2 || left[0].Seq != all[1].Seq {
		t.Fatalf("webhook backlog lost: cursor %d, events before %d, after %+v", cur, len(all), left)
	}
}

func testWebhookJobs(t *testing.T, open Opener) {
	spec.Rule(t, "H01")
	e := setup(t, open)
	hooked := e.add("q", "k", runnel.Webhook("https://example.com/hook"))
	e.add("q", "k")
	for _, j := range e.claim([]string{"q"}, 2) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}, Webhooks: runnel.WebhookConfig{Enabled: true}})
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	cur, err := e.s.Cursor(e.ctx, e.ns, "webhooks")
	if err != nil || cur == 0 {
		t.Fatalf("webhook cursor %d %v", cur, err)
	}
	if ok, err := e.s.SwapCursor(e.ctx, e.ns, "webhooks", cur, 0); err != nil || !ok {
		t.Fatalf("rewind cursor %v %v", ok, err)
	}
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	hooks, _, err := e.c.List(e.ctx, runnel.Query{Queue: runnel.WebhookQueue, Limit: 10})
	if err != nil || len(hooks) != 1 {
		t.Fatalf("%d deliveries for one webhook seen twice, %v", len(hooks), err)
	}
	var task struct {
		URL   string
		Event string
		Job   struct{ ID string }
	}
	if err := hooks[0].Bind(&task); err != nil || hooks[0].Kind != runnel.WebhookKind || task.URL != "https://example.com/hook" ||
		task.Event != "job.completed" || task.Job.ID != hooked.ID {
		t.Fatalf("delivery %+v of %+v, %v", task, hooks[0], err)
	}
}
