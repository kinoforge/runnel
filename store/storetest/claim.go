package storetest

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testClaimOrder(t *testing.T, open Opener) {
	spec.Rule(t, "C01", "C02")
	e := setup(t, open)
	low := e.add("q", "k", runnel.Priority(0))
	high := e.add("q", "k", runnel.Priority(10))
	mid := e.add("q2", "k", runnel.Priority(5))
	e.add("other", "k", runnel.Priority(100))
	got := e.claim([]string{"q", "q2"}, 10)
	if want := []string{high.ID, mid.ID, low.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("order %v want %v", ids(got), want)
	}
	for _, j := range got {
		if j.State != runnel.StateRunning || j.Attempt != 1 || j.Worker != "w1" || j.LeaseUntil.IsZero() {
			t.Fatalf("claimed job not running %+v", j)
		}
	}
	if again := e.claim([]string{"q", "q2"}, 10); len(again) != 0 {
		t.Fatalf("claimed twice: %v", ids(again))
	}
}

func testTenantWaiting(t *testing.T, open Opener) {
	spec.Rule(t, "J08")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", TenantWaiting: 2}); err != nil {
		t.Fatal(err)
	}
	e.add("q", "k", runnel.Tenant("a"))
	e.add("q", "k", runnel.Tenant("a"))
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.Tenant("a")); !errors.Is(err, runnel.ErrTenantFull) {
		t.Fatalf("want tenant full, got %v", err)
	}
	e.add("q", "k", runnel.Tenant("b"))
	e.claim([]string{"q"}, 1)
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.Tenant("a")); err != nil {
		t.Fatalf("slot freed by claim, got %v", err)
	}
}

func testConcurrency(t *testing.T, open Opener) {
	spec.Rule(t, "C05")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		e.add("q", "k")
	}
	got := e.claim([]string{"q"}, 10)
	if len(got) != 2 {
		t.Fatalf("concurrency 2, claimed %d", len(got))
	}
	if more := e.claim([]string{"q"}, 10); len(more) != 0 {
		t.Fatalf("over the limit: %d", len(more))
	}
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	if more := e.claim([]string{"q"}, 10); len(more) != 1 {
		t.Fatalf("slot not freed: %d", len(more))
	}
}

func testSlots(t *testing.T, open Opener) {
	spec.Rule(t, "C06")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", TenantSlots: 1}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "gpu", Slots: 2}); err != nil {
		t.Fatal(err)
	}
	a1 := e.add("q", "k", runnel.Tenant("a"))
	e.add("q", "k", runnel.Tenant("a"))
	b1 := e.add("q", "k", runnel.Tenant("b"))
	got := e.claim([]string{"q"}, 10)
	if want := []string{a1.ID, b1.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("tenant slots: %v want %v", ids(got), want)
	}
	for range 3 {
		e.add("g", "k", runnel.Pool("gpu"))
	}
	if g := e.claim([]string{"g"}, 10); len(g) != 2 {
		t.Fatalf("pool slots 2, claimed %d", len(g))
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "api", TenantSlots: 1}); err != nil {
		t.Fatal(err)
	}
	p1 := e.add("h", "k", runnel.Pool("api"), runnel.Tenant("a"))
	e.add("h", "k", runnel.Pool("api"), runnel.Tenant("a"))
	p3 := e.add("h", "k", runnel.Pool("api"), runnel.Tenant("b"))
	if got, want := e.claim([]string{"h"}, 10), []string{p1.ID, p3.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("pool tenant slots: %v want %v", ids(got), want)
	}
}

func testRate(t *testing.T, open Opener) {
	spec.Rule(t, "C07")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Rate: 1, Burst: 2}); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		e.add("q", "k")
	}
	if got := e.claim([]string{"q"}, 10); len(got) != 2 {
		t.Fatalf("burst 2, claimed %d", len(got))
	}
	if got := e.claim([]string{"q"}, 10); len(got) != 0 {
		t.Fatalf("bucket empty, claimed %d", len(got))
	}
	time.Sleep(1100 * time.Millisecond)
	if got := e.claim([]string{"q"}, 10); len(got) < 1 || len(got) > 2 {
		t.Fatalf("refill 1 to 2, claimed %d", len(got))
	}
}

func testPaused(t *testing.T, open Opener) {
	spec.Rule(t, "C04")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Paused: true}); err != nil {
		t.Fatal(err)
	}
	e.add("q", "k")
	if got := e.claim([]string{"q"}, 10); len(got) != 0 {
		t.Fatalf("paused queue claimed")
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if got := e.claim([]string{"q"}, 10); len(got) != 1 {
		t.Fatalf("unpaused queue not claimed")
	}
}

func testPolling(t *testing.T, open Opener) {
	spec.Rule(t, "C09", "O07")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	e.add("q", "k")
	e.add("q", "k")
	j := e.claim([]string{"q"}, 5)[0]
	p := e.finish(j, runnel.Outcome{Kind: runnel.OutcomePending, Handle: "remote-1", Delay: runnel.Duration(time.Second)})
	if p.State != runnel.StatePolling {
		t.Fatalf("not polling: %s", p.State)
	}
	if got := e.claim([]string{"q"}, 5); len(got) != 0 {
		t.Fatalf("polling job must hold its slot, claimed %d", len(got))
	}
	time.Sleep(1100 * time.Millisecond)
	got := e.claim([]string{"q"}, 5)
	if len(got) != 1 || got[0].ID != j.ID || got[0].Handle != "remote-1" || got[0].Attempt != 1 {
		t.Fatalf("poll claim wrong: %+v", got)
	}
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone, Result: json.RawMessage(`{"ok":true}`)})
	if got := e.claim([]string{"q"}, 5); len(got) != 1 {
		t.Fatalf("slot not freed after poll done")
	}
	if r := e.get(j.ID); string(r.Result) != `{"ok":true}` || r.State != runnel.StateCompleted {
		t.Fatalf("result %s state %s", r.Result, r.State)
	}
}

func testConcurrentClaims(t *testing.T, open Opener) {
	spec.Rule(t, "C03")
	e := setup(t, open)
	jobs := make([]*runnel.Job, 60)
	for i := range jobs {
		jobs[i], _ = runnel.NewJob("q", "k", nil)
	}
	if _, err := e.c.EnqueueJobs(e.ctx, jobs); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Go(func() {
			for range 10 {
				got, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: fmt.Sprintf("w%d", w), Limit: 3, Lease: time.Minute, Now: e.now()})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, j := range got {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != 60 {
		t.Fatalf("claimed %d distinct of 60", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

func testQueueWaiting(t *testing.T, open Opener) {
	spec.Rule(t, "J04", "J08")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxWaiting: 2}); err != nil {
		t.Fatal(err)
	}
	batch := make([]*runnel.Job, 3)
	for i := range batch {
		batch[i], _ = runnel.NewJob("q", "k", nil)
	}
	res, err := e.c.EnqueueJobs(e.ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil || res[1].Err != nil || !errors.Is(res[2].Err, runnel.ErrQueueFull) {
		t.Fatalf("batch past the cap: %+v", res)
	}
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil); !errors.Is(err, runnel.ErrQueueFull) {
		t.Fatalf("want queue full, got %v", err)
	}
	if _, err := e.c.Enqueue(e.ctx, "other", "k", nil); err != nil {
		t.Fatalf("cap leaked to another queue: %v", err)
	}
	e.claim([]string{"q"}, 1)
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil); err != nil {
		t.Fatalf("running jobs count against the waiting cap: %v", err)
	}
}

func testClaimByID(t *testing.T, open Opener) {
	spec.Rule(t, "C10")
	e := setup(t, open)
	a, b, c := e.add("q", "k"), e.add("q", "k", runnel.Priority(-5)), e.add("q", "k")
	got, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, IDs: []string{b.ID}, Worker: "w1", Limit: 1, Lease: time.Minute, Now: e.now()})
	if err != nil || len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("claim by id: %v %v", ids(got), err)
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	got, _ = e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, IDs: []string{c.ID}, Worker: "w1", Limit: 1, Lease: time.Minute, Now: e.now()})
	if len(got) != 0 {
		t.Fatalf("claim by id ignored the queue limit: %v", ids(got))
	}
	if j := e.get(a.ID); j.State != runnel.StateWaiting {
		t.Fatalf("other job touched: %s", j.State)
	}
}

func testFair(t *testing.T, open Opener) {
	spec.Rule(t, "C08")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", TenantSlots: 1}); err != nil {
		t.Fatal(err)
	}
	big := make([]*runnel.Job, 400)
	for i := range big {
		big[i], _ = runnel.NewJob("q", "k", nil, runnel.Tenant("big"))
	}
	if _, err := e.c.EnqueueJobs(e.ctx, big); err != nil {
		t.Fatal(err)
	}
	for _, tn := range []string{"s1", "s2", "s3"} {
		e.add("q", "k", runnel.Tenant(tn))
	}
	got := e.claim([]string{"q"}, 10)
	tenants := map[string]int{}
	for _, j := range got {
		tenants[j.Tenant]++
	}
	if len(got) != 4 || tenants["big"] != 1 || tenants["s1"] != 1 || tenants["s2"] != 1 || tenants["s3"] != 1 {
		t.Fatalf("a capped tenant at the head starved the rest: %v", tenants)
	}
}

func testFairPool(t *testing.T, open Opener) {
	spec.Rule(t, "C08")
	e := setup(t, open)
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "gpu", Slots: 2}); err != nil {
		t.Fatal(err)
	}
	full := make([]*runnel.Job, 300)
	for i := range full {
		full[i], _ = runnel.NewJob("q", "k", nil, runnel.Pool("gpu"), runnel.Priority(10))
	}
	if _, err := e.c.EnqueueJobs(e.ctx, full); err != nil {
		t.Fatal(err)
	}
	e.add("q", "k")
	e.add("q", "k", runnel.Pool("cpu"))
	got := e.claim([]string{"q"}, 10)
	pools := map[string]int{}
	for _, j := range got {
		pools[j.Pool]++
	}
	if len(got) != 4 || pools["gpu"] != 2 || pools[""] != 1 || pools["cpu"] != 1 {
		t.Fatalf("a full pool at the head starved the rest: %v", pools)
	}
}

func testTenantRate(t *testing.T, open Opener) {
	spec.Rule(t, "C07")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", TenantRate: 0.01, TenantBurst: 1}); err != nil {
		t.Fatal(err)
	}
	a1 := e.add("q", "k", runnel.Tenant("a"))
	e.add("q", "k", runnel.Tenant("a"))
	b1 := e.add("q", "k", runnel.Tenant("b"))
	if got, want := e.claim([]string{"q"}, 10), []string{a1.ID, b1.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("tenant rate: %v want %v", ids(got), want)
	}
	if got := e.claim([]string{"q"}, 10); len(got) != 0 {
		t.Fatalf("tenant bucket empty, claimed %d", len(got))
	}
}
