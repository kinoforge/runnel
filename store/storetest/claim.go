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
	low := e.add("q", runnel.Priority(0))
	high := e.add("q", runnel.Priority(10))
	mid := e.add("q2", runnel.Priority(5))
	e.add("other", runnel.Priority(100))
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

func testGroupMaxQueued(t *testing.T, open Opener) {
	spec.Rule(t, "J08")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", GroupMaxQueued: 2}); err != nil {
		t.Fatal(err)
	}
	e.add("q", runnel.Group("a"))
	e.add("q", runnel.Group("a"))
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.Group("a")); !errors.Is(err, runnel.ErrGroupFull) {
		t.Fatalf("want group full, got %v", err)
	}
	e.add("q", runnel.Group("b"))
	e.claim([]string{"q"}, 1)
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.Group("a")); err != nil {
		t.Fatalf("slot freed by claim, got %v", err)
	}
}

func testMaxRunning(t *testing.T, open Opener) {
	spec.Rule(t, "C05")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxRunning: 2}); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		e.add("q")
	}
	got := e.claim([]string{"q"}, 10)
	if len(got) != 2 {
		t.Fatalf("max running 2, claimed %d", len(got))
	}
	if more := e.claim([]string{"q"}, 10); len(more) != 0 {
		t.Fatalf("over the limit: %d", len(more))
	}
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	if more := e.claim([]string{"q"}, 10); len(more) != 1 {
		t.Fatalf("slot not freed: %d", len(more))
	}
}

func testGroupAndPoolMaxRunning(t *testing.T, open Opener) {
	spec.Rule(t, "C06")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", GroupMaxRunning: 1}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "gpu", Slots: 2}); err != nil {
		t.Fatal(err)
	}
	a1 := e.add("q", runnel.Group("a"))
	e.add("q", runnel.Group("a"))
	b1 := e.add("q", runnel.Group("b"))
	got := e.claim([]string{"q"}, 10)
	if want := []string{a1.ID, b1.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("group max running: %v want %v", ids(got), want)
	}
	for range 3 {
		e.add("g", runnel.Pool("gpu"))
	}
	if g := e.claim([]string{"g"}, 10); len(g) != 2 {
		t.Fatalf("pool max running 2, claimed %d", len(g))
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "api", GroupSlots: 1}); err != nil {
		t.Fatal(err)
	}
	p1 := e.add("h", runnel.Pool("api"), runnel.Group("a"))
	e.add("h", runnel.Pool("api"), runnel.Group("a"))
	p3 := e.add("h", runnel.Pool("api"), runnel.Group("b"))
	if got, want := e.claim([]string{"h"}, 10), []string{p1.ID, p3.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("pool group max running: %v want %v", ids(got), want)
	}
}

func testRate(t *testing.T, open Opener) {
	spec.Rule(t, "C07")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Rate: 1, Burst: 2}); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		e.add("q")
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

func testPoolRate(t *testing.T, open Opener) {
	spec.Rule(t, "C11")
	e := setup(t, open)
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "api", Rate: 1, Burst: 2}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"a", "b", "a"} {
		e.add(q, runnel.Pool("api"))
	}
	free := e.add("b")
	got := e.claim([]string{"a", "b"}, 10)
	if len(got) != 3 || !slices.Contains(ids(got), free.ID) {
		t.Fatalf("pool burst 2 plus a job outside it: %v", ids(got))
	}
	if got := e.claim([]string{"a", "b"}, 10); len(got) != 0 {
		t.Fatalf("pool bucket empty, claimed %d", len(got))
	}
}

func testPoolPause(t *testing.T, open Opener) {
	spec.Rule(t, "C12")
	e := setup(t, open)
	until := e.now().Add(1500 * time.Millisecond)
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "api", PausedUntil: until}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "off", Paused: true}); err != nil {
		t.Fatal(err)
	}
	held := e.add("q", runnel.Pool("api"))
	e.add("q", runnel.Pool("off"))
	free := e.add("q", runnel.Pool("other"))
	if got := e.claim([]string{"q"}, 10); !slices.Equal(ids(got), []string{free.ID}) {
		t.Fatalf("paused pools: %v want %v", ids(got), free.ID)
	}
	time.Sleep(until.Sub(e.now()) + 1100*time.Millisecond)
	if got := e.claim([]string{"q"}, 10); !slices.Equal(ids(got), []string{held.ID}) {
		t.Fatalf("after paused_until: %v want %v", ids(got), held.ID)
	}
}

func testPaused(t *testing.T, open Opener) {
	spec.Rule(t, "C04")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Paused: true}); err != nil {
		t.Fatal(err)
	}
	e.add("q")
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
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxRunning: 1}); err != nil {
		t.Fatal(err)
	}
	e.add("q")
	e.add("q")
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

func testMaxQueued(t *testing.T, open Opener) {
	spec.Rule(t, "J04", "J08")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxQueued: 2}); err != nil {
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
		t.Fatalf("running jobs count against the max queued cap: %v", err)
	}
}

func testClaimByID(t *testing.T, open Opener) {
	spec.Rule(t, "C10")
	e := setup(t, open)
	a, b, c := e.add("q"), e.add("q", runnel.Priority(-5)), e.add("q")
	got, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, IDs: []string{b.ID}, Worker: "w1", Limit: 1, Lease: time.Minute, Now: e.now()})
	if err != nil || len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("claim by id: %v %v", ids(got), err)
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxRunning: 1}); err != nil {
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
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", GroupMaxRunning: 1}); err != nil {
		t.Fatal(err)
	}
	big := make([]*runnel.Job, 400)
	for i := range big {
		big[i], _ = runnel.NewJob("q", "k", nil, runnel.Group("big"))
	}
	if _, err := e.c.EnqueueJobs(e.ctx, big); err != nil {
		t.Fatal(err)
	}
	for _, tn := range []string{"s1", "s2", "s3"} {
		e.add("q", runnel.Group(tn))
	}
	got := e.claim([]string{"q"}, 10)
	groups := map[string]int{}
	for _, j := range got {
		groups[j.Group]++
	}
	if len(got) != 4 || groups["big"] != 1 || groups["s1"] != 1 || groups["s2"] != 1 || groups["s3"] != 1 {
		t.Fatalf("a capped group at the head starved the rest: %v", groups)
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
	e.add("q")
	e.add("q", runnel.Pool("cpu"))
	got := e.claim([]string{"q"}, 10)
	pools := map[string]int{}
	for _, j := range got {
		pools[j.Pool]++
	}
	if len(got) != 4 || pools["gpu"] != 2 || pools[""] != 1 || pools["cpu"] != 1 {
		t.Fatalf("a full pool at the head starved the rest: %v", pools)
	}
}

func testGroupRate(t *testing.T, open Opener) {
	spec.Rule(t, "C07")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", GroupRate: 0.01, GroupBurst: 1}); err != nil {
		t.Fatal(err)
	}
	a1 := e.add("q", runnel.Group("a"))
	e.add("q", runnel.Group("a"))
	b1 := e.add("q", runnel.Group("b"))
	if got, want := e.claim([]string{"q"}, 10), []string{a1.ID, b1.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("group rate: %v want %v", ids(got), want)
	}
	if got := e.claim([]string{"q"}, 10); len(got) != 0 {
		t.Fatalf("group bucket empty, claimed %d", len(got))
	}
}
