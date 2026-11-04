package storetest

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testExtend(t *testing.T, open Opener) {
	spec.Rule(t, "L01")
	e := setup(t, open)
	a, b := e.add("q"), e.add("q")
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
	e.add("q")
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
	e.add("q")
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

func testDueStart(t *testing.T, open Opener) {
	spec.Rule(t, "T01", "L02")
	e := setup(t, open)
	a := e.add("q", runnel.StartWithin(100*time.Millisecond), runnel.Fallbacks(runnel.Target{Queue: "q2"}))
	e.add("q", runnel.StartWithin(100*time.Millisecond))
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

func testConfigs(t *testing.T, open Opener) {
	spec.Rule(t, "X03")
	e := setup(t, open)
	if err := e.c.SetResource(e.ctx, runnel.ResourceConfig{Name: "p", MaxRunning: 3}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.SetResource(e.ctx, runnel.ResourceConfig{Name: "p", MaxRunning: 4}); err != nil {
		t.Fatal(err)
	}
	resources, err := e.c.Resources(e.ctx)
	if err != nil || resources["p"].MaxRunning != 4 || len(resources) != 1 {
		t.Fatalf("resources %+v %v", resources, err)
	}
	if err := e.c.DeleteResource(e.ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if resources, _ := e.c.Resources(e.ctx); len(resources) != 0 {
		t.Fatalf("resource not deleted")
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
		all = append(all, e.add(q, runnel.Tenant("t")).ID)
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
