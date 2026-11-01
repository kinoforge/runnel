package storetest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

// Opener returns the store under test. It may return the same store every time.
type Opener func(t *testing.T) runnel.Store

type env struct {
	t   *testing.T
	s   runnel.Store
	ns  string
	c   *runnel.Client
	ctx context.Context
}

func setup(t *testing.T, open Opener) *env {
	t.Helper()
	s := open(t)
	ns := fmt.Sprintf("t%d", rand.Int64N(1<<40))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return &env{t: t, s: s, ns: ns, c: runnel.NewClient(s, ns), ctx: ctx}
}

func (e *env) now() time.Time { return runnel.ClockOf(e.s).Now() }

func (e *env) add(queue, kind string, opts ...runnel.Option) runnel.Inserted {
	e.t.Helper()
	res, err := e.c.Enqueue(e.ctx, queue, kind, map[string]any{"n": 1}, opts...)
	if err != nil {
		e.t.Fatalf("enqueue: %v", err)
	}
	return res
}

func (e *env) claim(queues []string, limit int) []*runnel.Job {
	e.t.Helper()
	jobs, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: queues, Worker: "w1", Limit: limit, Lease: 30 * time.Second, Now: e.now()})
	if err != nil {
		e.t.Fatalf("claim: %v", err)
	}
	return jobs
}

func (e *env) finish(j *runnel.Job, o runnel.Outcome) *runnel.Job {
	e.t.Helper()
	n := runnel.Decide(j, o, e.now())
	ok, err := e.s.Apply(e.ctx, e.ns, []runnel.Change{{Job: n}}, e.now())
	if err != nil || !ok[0] {
		e.t.Fatalf("apply: %v %v", ok, err)
	}
	return n
}

func (e *env) get(id string) *runnel.Job {
	e.t.Helper()
	j, err := e.c.Get(e.ctx, id)
	if err != nil {
		e.t.Fatalf("get %s: %v", id, err)
	}
	return j
}

func ids(jobs []*runnel.Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

// Run checks a store against the behaviour every Runnel store must have. Each
// case uses its own random namespace.
func Run(t *testing.T, open Opener) {
	tests := map[string]func(*testing.T, Opener){
		"InsertGetRoundTrip":         testRoundTrip,
		"ClaimOrder":                 testClaimOrder,
		"ScheduledNotEarly":          testScheduled,
		"UniqueDedupe":               testUnique,
		"UniqueWindow":               testUniqueWindow,
		"TenantWaitingCap":           testTenantWaiting,
		"QueueConcurrency":           testConcurrency,
		"TenantAndPoolSlots":         testSlots,
		"RateLimit":                  testRate,
		"PausedQueue":                testPaused,
		"ExtendAndLost":              testExtend,
		"VersionGuard":               testVersionGuard,
		"LeaseGuard":                 testLeaseGuard,
		"WorkflowCascadeDone":        testCascadeDone,
		"WorkflowCascadeFail":        testCascadeFail,
		"DependencyRejects":          testDependencyRejects,
		"PollingBypassesLimits":      testPolling,
		"DueStart":                   testDueStart,
		"PruneArchive":               testPrune,
		"EventsCursors":              testEvents,
		"ConfigsCRUD":                testConfigs,
		"StatsAndList":               testStatsList,
		"ConcurrentClaimsExclusive":  testConcurrentClaims,
		"WorkerEndToEnd":             testWorkerEndToEnd,
		"WorkerRetryFallbackDead":    testWorkerFallback,
		"WorkerBusyKeepsWait":        testWorkerBusy,
		"WorkerAsyncPoll":            testWorkerAsync,
		"WorkerFlowResults":          testWorkerFlow,
		"WorkerCancelRunning":        testWorkerCancel,
		"MaintainerReapAndStart":     testMaintainer,
		"MaintainerCron":             testCron,
		"StoreTime":                  testStoreTime,
		"EventsOutliveJobPrune":      testEventsOutlivePrune,
		"MaintainerKeepsUndelivered": testMaintainerEvents,
		"QueueWaitingCap":            testQueueWaiting,
		"ClaimByID":                  testClaimByID,
		"WaitForResult":              testWait,
		"WorkerDirectStart":          testDirect,
		"FairClaimPastCappedTenant":  testFair,
		"FairClaimPastFullPool":      testFairPool,
		"EnqueueInsideTransaction":   testEnqueueTx,
		"NamespacesIsolated":         testNamespaces,
		"MigrateAgain":               testMigrateAgain,
		"TenantRateLimit":            testTenantRate,
		"WorkerConcurrency":          testWorkerConcurrency,
		"WebhookDeliveryJobs":        testWebhookJobs,
	}
	for _, n := range slices.Sorted(maps.Keys(tests)) {
		t.Run(n, func(t *testing.T) { tests[n](t, open) })
	}
}

func testRoundTrip(t *testing.T, open Opener) {
	spec.Rule(t, "J01", "J03")
	e := setup(t, open)
	res := e.add("q", "k", runnel.Priority(3), runnel.Tenant("acme"), runnel.Pool("p1"), runnel.MaxAttempts(7),
		runnel.Meta(map[string]string{"a": "b"}), runnel.Fallbacks(runnel.Target{Queue: "q2"}), runnel.StartWithin(time.Minute),
		runnel.Webhook("https://example.com/h"), runnel.Timeout(time.Second))
	j := e.get(res.ID)
	if j.State != runnel.StateWaiting || j.Priority != 3 || j.Tenant != "acme" || j.Pool != "p1" || j.MaxAttempts != 7 {
		t.Fatalf("bad job %+v", j)
	}
	if j.Meta["a"] != "b" || len(j.Fallbacks) != 1 || j.Webhook == "" || j.Timeout.D() != time.Second {
		t.Fatalf("data lost %+v", j)
	}
	if j.StartBy.IsZero() || j.StartBy.Sub(j.WaitingSince) != time.Minute {
		t.Fatalf("start_by %v waiting %v", j.StartBy, j.WaitingSince)
	}
	var p map[string]int
	if err := j.Bind(&p); err != nil || p["n"] != 1 {
		t.Fatalf("payload %s %v", j.Payload, err)
	}
	if _, err := e.c.Get(e.ctx, runnel.NewID()); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	if plain := e.get(e.add("q", "k").ID); plain.MaxAttempts != runnel.DefaultMaxAttempts {
		t.Fatalf("default attempts %d", plain.MaxAttempts)
	}
}

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

func testScheduled(t *testing.T, open Opener) {
	spec.Rule(t, "J05")
	e := setup(t, open)
	e.add("q", "k", runnel.In(time.Hour))
	if got := e.claim([]string{"q"}, 5); len(got) != 0 {
		t.Fatalf("future job claimed")
	}
	soon := e.add("q", "k", runnel.In(300*time.Millisecond))
	time.Sleep(400 * time.Millisecond)
	if got := e.claim([]string{"q"}, 5); len(got) != 1 || got[0].ID != soon.ID {
		t.Fatalf("due job not claimed: %v", ids(got))
	}
}

func testUnique(t *testing.T, open Opener) {
	spec.Rule(t, "J06")
	e := setup(t, open)
	a := e.add("q", "k", runnel.Unique("order-1", 0))
	b := e.add("q", "k", runnel.Unique("order-1", 0))
	if !b.Duplicate || b.ID != a.ID {
		t.Fatalf("not deduped: %+v %+v", a, b)
	}
	got := e.claim([]string{"q"}, 5)
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	c := e.add("q", "k", runnel.Unique("order-1", 0))
	if c.Duplicate || c.ID == a.ID {
		t.Fatalf("unique not released after finish: %+v", c)
	}
}

func testUniqueWindow(t *testing.T, open Opener) {
	spec.Rule(t, "J07")
	e := setup(t, open)
	a := e.add("q", "k", runnel.Unique("w", time.Hour))
	got := e.claim([]string{"q"}, 5)
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	b := e.add("q", "k", runnel.Unique("w", time.Hour))
	if !b.Duplicate || b.ID != a.ID {
		t.Fatalf("window not kept after finish: %+v", b)
	}
	e.add("q", "k", runnel.Unique("short", 200*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	if d := e.add("q", "k", runnel.Unique("short", 200*time.Millisecond)); d.Duplicate {
		t.Fatalf("expired window still deduped")
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

func testExtend(t *testing.T, open Opener) {
	spec.Rule(t, "L01")
	e := setup(t, open)
	a, b := e.add("q", "k"), e.add("q", "k")
	got := e.claim([]string{"q"}, 2)
	if _, err := e.c.Cancel(e.ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	until := e.now().Add(time.Hour).Truncate(time.Millisecond)
	lost, err := e.s.Extend(e.ctx, e.ns, "w1", ids(got), until)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(lost, []string{b.ID}) {
		t.Fatalf("lost %v want %v", lost, b.ID)
	}
	if j := e.get(a.ID); !j.LeaseUntil.Equal(until) {
		t.Fatalf("lease %v want %v", j.LeaseUntil, until)
	}
	if lost, _ := e.s.Extend(e.ctx, e.ns, "other", []string{a.ID}, until); len(lost) != 1 {
		t.Fatalf("other worker extended")
	}
}

func testVersionGuard(t *testing.T, open Opener) {
	spec.Rule(t, "L04")
	e := setup(t, open)
	e.add("q", "k")
	j := e.claim([]string{"q"}, 1)[0]
	stale := j.Clone()
	e.finish(j, runnel.Outcome{Kind: runnel.OutcomeRetry, Error: "x"})
	n := runnel.Decide(stale, runnel.Outcome{Kind: runnel.OutcomeDone}, e.now())
	ok, err := e.s.Apply(e.ctx, e.ns, []runnel.Change{{Job: n}}, e.now())
	if err != nil || ok[0] {
		t.Fatalf("stale apply accepted: %v %v", ok, err)
	}
}

func testLeaseGuard(t *testing.T, open Opener) {
	spec.Rule(t, "L03")
	e := setup(t, open)
	e.add("q", "k")
	j := e.claim([]string{"q"}, 1)[0]
	n := runnel.Decide(j, runnel.Outcome{Kind: runnel.OutcomeRetry}, e.now())
	ok, err := e.s.Apply(e.ctx, e.ns, []runnel.Change{{Job: n, LeaseBefore: e.now()}}, e.now())
	if err != nil || ok[0] {
		t.Fatalf("live lease reaped: %v %v", ok, err)
	}
}

func testCascadeDone(t *testing.T, open Opener) {
	spec.Rule(t, "F01")
	e := setup(t, open)
	f := runnel.NewFlow()
	mk := func(k string) *runnel.Job { j, _ := runnel.NewJob("q", k, nil); return j }
	f.Add("a", mk("a")).Add("b", mk("b")).Add("c", mk("c"), "a", "b")
	res, err := e.c.EnqueueFlow(e.ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	if c := e.get(res["c"].ID); c.State != runnel.StateBlocked || c.Pending != 2 {
		t.Fatalf("c not blocked: %s %d", c.State, c.Pending)
	}
	got := e.claim([]string{"q"}, 10)
	if len(got) != 2 {
		t.Fatalf("only roots claimable, got %d", len(got))
	}
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	if c := e.get(res["c"].ID); c.State != runnel.StateBlocked || c.Pending != 1 {
		t.Fatalf("c after one: %s %d", c.State, c.Pending)
	}
	e.finish(got[1], runnel.Outcome{Kind: runnel.OutcomeDone})
	if c := e.get(res["c"].ID); c.State != runnel.StateWaiting {
		t.Fatalf("c not released: %s", c.State)
	}
	if last := e.claim([]string{"q"}, 10); len(last) != 1 || last[0].ID != res["c"].ID {
		t.Fatalf("c not claimable")
	}
	late, err := e.c.Enqueue(e.ctx, "q", "late", nil, runnel.After(res["a"].ID))
	if err != nil {
		t.Fatal(err)
	}
	if j := e.get(late.ID); j.State != runnel.StateWaiting {
		t.Fatalf("dependency already done, got %s", j.State)
	}
}

func testCascadeFail(t *testing.T, open Opener) {
	spec.Rule(t, "F02", "F03")
	e := setup(t, open)
	f := runnel.NewFlow()
	mk := func(k string) *runnel.Job { j, _ := runnel.NewJob("q", k, nil, runnel.MaxAttempts(1)); return j }
	f.Add("a", mk("a")).Add("b", mk("b"), "a").Add("c", mk("c"), "b")
	res, err := e.c.EnqueueFlow(e.ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	a := e.claim([]string{"q"}, 10)
	if len(a) != 1 {
		t.Fatalf("claimed %d", len(a))
	}
	e.finish(a[0], runnel.Outcome{Kind: runnel.OutcomeFail, Error: "boom"})
	for _, n := range []string{"b", "c"} {
		if j := e.get(res[n].ID); j.State != runnel.StateCancelled {
			t.Fatalf("%s not cancelled: %s", n, j.State)
		}
	}
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.After(res["a"].ID)); !errors.Is(err, runnel.ErrDependency) {
		t.Fatalf("want dependency error, got %v", err)
	}
	g := runnel.NewFlow()
	g.Add("x", mk("x")).Add("y", mk("y"), "x").Add("z", mk("z"), "y")
	held, err := e.c.EnqueueFlow(e.ctx, g)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.Cancel(e.ctx, held["x"].ID); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"y", "z"} {
		if j := e.get(held[n].ID); j.State != runnel.StateCancelled {
			t.Fatalf("%s not cancelled with x: %s", n, j.State)
		}
	}
}

func testDependencyRejects(t *testing.T, open Opener) {
	spec.Rule(t, "F02", "F04", "F05")
	e := setup(t, open)
	if _, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.After(runnel.NewID())); !errors.Is(err, runnel.ErrDependency) {
		t.Fatalf("missing dependency accepted: %v", err)
	}
	f := runnel.NewFlow()
	j1, _ := runnel.NewJob("q", "k", nil)
	j2, _ := runnel.NewJob("q", "k", nil)
	f.Add("a", j1, "b").Add("b", j2, "a")
	if _, err := e.c.EnqueueFlow(e.ctx, f); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("cycle accepted: %v", err)
	}
	dup := e.add("q", "k", runnel.Unique("shared", 0))
	f2 := runnel.NewFlow()
	s1, _ := runnel.NewJob("q", "k", nil, runnel.Unique("shared", 0))
	s2, _ := runnel.NewJob("q", "k2", nil)
	f2.Add("s1", s1).Add("s2", s2, "s1")
	res, err := e.c.EnqueueFlow(e.ctx, f2)
	if err != nil {
		t.Fatal(err)
	}
	if !res["s1"].Duplicate || res["s1"].ID != dup.ID {
		t.Fatalf("flow step not deduped: %+v", res["s1"])
	}
	if j := e.get(res["s2"].ID); !slices.Equal(j.DependsOn, []string{dup.ID}) || j.State != runnel.StateBlocked {
		t.Fatalf("dependency not remapped: %v %s", j.DependsOn, j.State)
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

func testDueStart(t *testing.T, open Opener) {
	spec.Rule(t, "T01", "L02")
	e := setup(t, open)
	a := e.add("q", "k", runnel.StartWithin(100*time.Millisecond), runnel.Fallbacks(runnel.Target{Queue: "q2"}))
	e.add("q", "k", runnel.StartWithin(100*time.Millisecond))
	time.Sleep(150 * time.Millisecond)
	due, err := e.s.Due(e.ctx, e.ns, runnel.DueStart, e.now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids(due), []string{a.ID}) {
		t.Fatalf("due start %v want %v", ids(due), a.ID)
	}
	j := e.claim([]string{"q"}, 1)[0]
	_ = j
	time.Sleep(10 * time.Millisecond)
	lease, err := e.s.Due(e.ctx, e.ns, runnel.DueLease, e.now().Add(time.Hour), 10)
	if err != nil || len(lease) != 1 {
		t.Fatalf("due lease %v %v", ids(lease), err)
	}
}

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

func testConfigs(t *testing.T, open Opener) {
	spec.Rule(t, "X03")
	e := setup(t, open)
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "p", Slots: 3}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "p", Slots: 4}); err != nil {
		t.Fatal(err)
	}
	pools, err := e.c.Pools(e.ctx)
	if err != nil || pools["p"].Slots != 4 || len(pools) != 1 {
		t.Fatalf("pools %+v %v", pools, err)
	}
	if err := e.c.DeletePool(e.ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if pools, _ := e.c.Pools(e.ctx); len(pools) != 0 {
		t.Fatalf("pool not deleted")
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "bad name"}); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("bad queue name accepted")
	}
	if err := runnel.PutNamespace(e.ctx, e.s, runnel.Namespace{Name: e.ns, WebhookSecret: "s"}); err != nil {
		t.Fatal(err)
	}
	all, err := runnel.Namespaces(e.ctx, e.s)
	if err != nil || all[e.ns].WebhookSecret != "s" {
		t.Fatalf("namespace %+v %v", all[e.ns], err)
	}
	_ = runnel.DeleteNamespace(e.ctx, e.s, e.ns)
}

func testStatsList(t *testing.T, open Opener) {
	spec.Rule(t, "M03", "M04")
	e := setup(t, open)
	var all []string
	for i := range 7 {
		q := "a"
		if i%2 == 1 {
			q = "b"
		}
		all = append(all, e.add(q, "k", runnel.Tenant("t")).ID)
	}
	jobs, next, err := e.c.List(e.ctx, runnel.Query{Limit: 3})
	if err != nil || len(jobs) != 3 || next == "" {
		t.Fatalf("page1 %d %q %v", len(jobs), next, err)
	}
	slices.Reverse(all)
	if !slices.Equal(ids(jobs), all[:3]) {
		t.Fatalf("list order %v want %v", ids(jobs), all[:3])
	}
	rest, _, err := e.c.List(e.ctx, runnel.Query{Limit: 10, Cursor: next})
	if err != nil || !slices.Equal(ids(rest), all[3:]) {
		t.Fatalf("page2 %v %v", ids(rest), err)
	}
	onlyB, _, _ := e.c.List(e.ctx, runnel.Query{Queue: "b", State: runnel.StateWaiting, Limit: 10})
	if len(onlyB) != 3 {
		t.Fatalf("filter b %d", len(onlyB))
	}
	stats, err := e.c.Stats(e.ctx)
	if err != nil || len(stats) != 2 || stats[0].Counts[runnel.StateWaiting]+stats[1].Counts[runnel.StateWaiting] != 7 {
		t.Fatalf("stats %+v %v", stats, err)
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

func testStoreTime(t *testing.T, open Opener) {
	spec.Rule(t, "K01")
	e := setup(t, open)
	src, ok := e.s.(runnel.TimeSource)
	if !ok {
		t.Skip("store has no clock of its own")
	}
	at, err := src.Time(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(at); d > 5*time.Second || d < -5*time.Second {
		t.Fatalf("store time %v is %v away from local time", at, d)
	}
	if d := runnel.ClockOf(e.s).Now().Sub(at); d > 5*time.Second || d < -5*time.Second {
		t.Fatalf("store clock drifts %v from the store", d)
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

func testWait(t *testing.T, open Opener) {
	spec.Rule(t, "W01", "W02")
	e := setup(t, open)
	res := e.add("q", "k")
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
	other := e.add("q", "k")
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

func testEnqueueTx(t *testing.T, open Opener) {
	spec.Rule(t, "J09", "J03")
	e := setup(t, open)
	dbs, ok := e.s.(interface{ DB() *sql.DB })
	if _, tx := e.s.(runnel.TxInserter); !ok || !tx {
		t.Skip("store has no SQL transactions")
	}
	db := dbs.DB()
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxAttempts: 9}); err != nil {
		t.Fatal(err)
	}
	enqueue := func(commit bool) string {
		tx, err := db.BeginTx(e.ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		j, _ := runnel.NewJob("q", "k", nil)
		res, err := e.c.EnqueueTx(e.ctx, tx, j)
		if err != nil || res[0].Err != nil {
			_ = tx.Rollback()
			t.Fatalf("enqueue in tx: %v %+v", err, res)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		return res[0].ID
	}
	gone := enqueue(false)
	if _, err := e.c.Get(e.ctx, gone); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("rolled back job exists: %v", err)
	}
	kept := enqueue(true)
	if j := e.get(kept); j.State != runnel.StateWaiting || j.MaxAttempts != 9 {
		t.Fatalf("committed job %+v", j)
	}
	if got := e.claim([]string{"q"}, 5); len(got) != 1 || got[0].ID != kept {
		t.Fatalf("claimed %v", ids(got))
	}
}

func testNamespaces(t *testing.T, open Opener) {
	spec.Rule(t, "X01")
	e := setup(t, open)
	other := runnel.NewClient(e.s, e.ns+"x")
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "p", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	res := e.add("q", "k")
	for _, j := range e.claim([]string{"q"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	e.add("q", "k")
	if _, err := e.s.SwapCursor(e.ctx, e.ns, "c", 0, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Get(e.ctx, res.ID); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("job read from another namespace: %v", err)
	}
	jobs, _, err1 := other.List(e.ctx, runnel.Query{})
	stats, err2 := other.Stats(e.ctx)
	pools, err3 := other.Pools(e.ctx)
	evs, err4 := other.Events(e.ctx, 0, 10)
	cur, err5 := e.s.Cursor(e.ctx, other.Namespace(), "c")
	claimed, err6 := e.s.Claim(e.ctx, other.Namespace(), runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w2", Limit: 5, Lease: time.Minute, Now: e.now()})
	if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
		t.Fatal(err)
	}
	if len(jobs)+len(stats)+len(pools)+len(evs)+len(claimed) != 0 || cur != 0 {
		t.Fatalf("namespace leak: jobs %d, stats %d, pools %d, events %d, claimed %d, cursor %d", len(jobs), len(stats), len(pools), len(evs), len(claimed), cur)
	}
}

func testMigrateAgain(t *testing.T, open Opener) {
	spec.Rule(t, "X02")
	e := setup(t, open)
	res := e.add("q", "k")
	if err := e.s.Migrate(e.ctx); err != nil {
		t.Fatalf("migrating a migrated store: %v", err)
	}
	if j := e.get(res.ID); j.State != runnel.StateWaiting {
		t.Fatalf("job after migrate: %s", j.State)
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
