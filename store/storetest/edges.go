package storetest

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func (e *env) total() int64 {
	e.t.Helper()
	stats, err := e.c.Stats(e.ctx)
	if err != nil {
		e.t.Fatalf("stats: %v", err)
	}
	var n int64
	for _, s := range stats {
		for _, c := range s.Counts {
			n += c
		}
	}
	return n
}

func (e *env) tick() {
	e.t.Helper()
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	if err := m.Tick(e.ctx, e.ns); err != nil {
		e.t.Fatalf("tick: %v", err)
	}
}

func testClaimTies(t *testing.T, open Opener) {
	spec.Rule(t, "C01", "C02")
	e := setup(t, open)
	t0 := e.now()
	later := e.add("a", runnel.Priority(1), runnel.At(t0.Add(300*time.Millisecond)))
	sooner := e.add("b", runnel.Priority(1))
	older := e.add("a", runnel.Priority(1), runnel.At(t0.Add(150*time.Millisecond)))
	waitFor(t, "the clock to move on", time.Second, func() bool { return e.now().Sub(t0) >= 5*time.Millisecond })
	newer := e.add("b", runnel.Priority(1), runnel.At(t0.Add(150*time.Millisecond)))
	low := e.add("b", runnel.Priority(-1000))
	e.add("a", runnel.Priority(1000), runnel.At(t0.Add(time.Hour)))
	top := e.add("b", runnel.Priority(1000), runnel.At(t0.Add(300*time.Millisecond)))
	waitFor(t, "every job due", 5*time.Second, func() bool { return e.now().Sub(t0) > 350*time.Millisecond })
	got := e.claim([]string{"a", "b"}, 10)
	if want := []string{top.ID, sooner.ID, older.ID, newer.ID, later.ID, low.ID}; !slices.Equal(ids(got), want) {
		t.Fatalf("claim order %v, want %v", ids(got), want)
	}
}

func testListPaging(t *testing.T, open Opener) {
	spec.Rule(t, "M03")
	e := setup(t, open)
	e.batch("big", 520)
	page, next, err := e.c.List(e.ctx, runnel.Query{})
	if err != nil || len(page) != 50 || next == "" {
		t.Fatalf("default page: %d %q %v", len(page), next, err)
	}
	page, _, err = e.c.List(e.ctx, runnel.Query{Limit: 10000})
	if err != nil || len(page) != 500 {
		t.Fatalf("page past the cap: %d %v", len(page), err)
	}
	seen := map[string]bool{}
	cursor, pages := "", 0
	for {
		page, next, err := e.c.List(e.ctx, runnel.Query{Queue: "big", Limit: 104, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for i, j := range page {
			if seen[j.ID] {
				t.Fatalf("job %s listed twice", j.ID)
			}
			seen[j.ID] = true
			if i > 0 && j.ID > page[i-1].ID {
				t.Fatalf("page not newest first at %d", i)
			}
		}
		if next == "" {
			break
		}
		if pages > 10 {
			t.Fatal("paging never ends")
		}
		cursor = next
	}
	if len(seen) != 520 {
		t.Fatalf("pages held %d of 520 jobs", len(seen))
	}
	match := e.add("mix", runnel.Group("g1"))
	e.add("mix", runnel.Group("g2"))
	other := e.add("mix", runnel.Group("g1"))
	e.claim([]string{"mix"}, 1)
	if _, err := e.c.Cancel(e.ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	for _, q := range []runnel.Query{
		{Queue: "mix", Group: "g1", State: runnel.StateRunning},
		{Queue: "mix", Kind: "k", Group: "g1", State: runnel.StateRunning},
	} {
		got, next, err := e.c.List(e.ctx, q)
		if err != nil || next != "" || len(got) != 1 || got[0].ID != match.ID {
			t.Errorf("filter %+v: %v %q %v", q, ids(got), next, err)
		}
	}
	if got, _, err := e.c.List(e.ctx, runnel.Query{Queue: "mix", Kind: "other"}); err != nil || len(got) != 0 {
		t.Errorf("kind filter: %v %v", ids(got), err)
	}
	if _, _, err := e.c.List(e.ctx, runnel.Query{State: "sleeping"}); !errors.Is(err, runnel.ErrInvalid) {
		t.Errorf("unknown state: %v", err)
	}
	if _, _, err := e.c.List(e.ctx, runnel.Query{Cursor: "not-an-id"}); !errors.Is(err, runnel.ErrInvalid) {
		t.Errorf("bad cursor: %v", err)
	}
}

func testBatchBounds(t *testing.T, open Opener) {
	spec.Rule(t, "J02", "J04")
	e := setup(t, open)
	jobs := func(n int) []*runnel.Job {
		out := make([]*runnel.Job, n)
		for i := range out {
			out[i], _ = runnel.NewJob("q", "k", nil)
		}
		return out
	}
	if _, err := e.c.EnqueueJobs(e.ctx, jobs(1001)); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("1001 jobs in one call: %v", err)
	}
	mixed := jobs(3)
	mixed[1].Kind = "bad kind"
	if _, err := e.c.EnqueueJobs(e.ctx, mixed); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("batch with a bad job: %v", err)
	}
	if n := e.total(); n != 0 {
		t.Fatalf("refused batches wrote %d jobs", n)
	}
	res, err := e.c.EnqueueJobs(e.ctx, jobs(1000))
	if err != nil || len(res) != 1000 {
		t.Fatalf("1000 jobs: %d %v", len(res), err)
	}
	unique := map[string]bool{}
	for _, r := range res {
		if r.Err != nil || r.Duplicate || unique[r.ID] {
			t.Fatalf("answer %+v", r)
		}
		unique[r.ID] = true
	}
	parts := jobs(3)
	parts[1].DependsOn = []string{runnel.NewID()}
	res, err = e.c.EnqueueJobs(e.ctx, parts)
	if err != nil || res[0].Err != nil || !errors.Is(res[1].Err, runnel.ErrDependency) || res[2].Err != nil {
		t.Fatalf("batch with a missing dependency: %+v %v", res, err)
	}
	if n := e.total(); n != 1002 {
		t.Fatalf("%d jobs stored, want 1002", n)
	}
}

func (e *env) inState(st runnel.State) *runnel.Job {
	e.t.Helper()
	q := "s-" + string(st)
	switch st {
	case runnel.StateWaiting:
		return e.get(e.add(q).ID)
	case runnel.StateBlocked:
		return e.get(e.add(q, runnel.After(e.add(q+"-parent").ID)).ID)
	case runnel.StateCancelled:
		j, err := e.c.Cancel(e.ctx, e.add(q).ID)
		if err != nil {
			e.t.Fatal(err)
		}
		return j
	case runnel.StateExpired:
		id := e.add(q, runnel.StartTimeout(10*time.Millisecond)).ID
		waitFor(e.t, "expiry", 5*time.Second, func() bool { e.tick(); return e.get(id).State == runnel.StateExpired })
		return e.get(id)
	}
	e.add(q)
	j := e.claim([]string{q}, 1)[0]
	switch st {
	case runnel.StateCompleted:
		return e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	case runnel.StateDead:
		return e.finish(j, runnel.Outcome{Kind: runnel.OutcomeFail, Error: "x"})
	case runnel.StatePolling:
		return e.finish(j, runnel.Outcome{Kind: runnel.OutcomePending, Handle: "h", Delay: runnel.Duration(time.Hour)})
	}
	return j
}

var everyState = []runnel.State{runnel.StateWaiting, runnel.StateBlocked, runnel.StateRunning, runnel.StatePolling,
	runnel.StateCompleted, runnel.StateDead, runnel.StateCancelled, runnel.StateExpired}

func testCancelRetryMatrix(t *testing.T, open Opener) {
	spec.Rule(t, "M01", "M02", "O11")
	e := setup(t, open)
	for _, st := range everyState {
		j := e.inState(st)
		if j.State != st {
			t.Fatalf("set up %s, got %s", st, j.State)
		}
		got, err := e.c.Cancel(e.ctx, j.ID)
		switch st {
		case runnel.StateCompleted, runnel.StateDead, runnel.StateExpired:
			if !errors.Is(err, runnel.ErrFinal) || e.get(j.ID).State != st {
				t.Errorf("cancel %s: %v, now %s", st, err, e.get(j.ID).State)
			}
		default:
			now := e.get(j.ID)
			if err != nil || got.State != runnel.StateCancelled || now.State != runnel.StateCancelled || now.Worker != "" || !now.LeaseUntil.IsZero() || now.FinishedAt.IsZero() {
				t.Errorf("cancel %s: %v, now %+v", st, err, now)
			}
		}
	}
	for _, st := range everyState {
		j := e.inState(st)
		got, err := e.c.Retry(e.ctx, j.ID)
		switch st {
		case runnel.StateDead, runnel.StateCancelled, runnel.StateExpired:
			now := e.get(j.ID)
			if err != nil || got.State != runnel.StateWaiting || now.State != runnel.StateWaiting || now.Attempt != 0 || !now.FinishedAt.IsZero() || len(now.Errors) != len(j.Errors) {
				t.Errorf("retry %s: %v, now %+v", st, err, now)
			}
		default:
			if !errors.Is(err, runnel.ErrNotFinal) || e.get(j.ID).State != st {
				t.Errorf("retry %s: %v, now %s", st, err, e.get(j.ID).State)
			}
		}
	}
}

func testOutcomesDropLease(t *testing.T, open Opener) {
	spec.Rule(t, "O11")
	e := setup(t, open)
	for _, o := range []runnel.Outcome{
		{Kind: runnel.OutcomeDone},
		{Kind: runnel.OutcomeRetry, Error: "x"},
		{Kind: runnel.OutcomeFail, Error: "x"},
		{Kind: runnel.OutcomeBusy},
		{Kind: runnel.OutcomePending, Handle: "h"},
		{Kind: runnel.OutcomeRelease},
	} {
		q := "o-" + string(o.Kind)
		e.add(q, runnel.MaxAttempts(3))
		j := e.claim([]string{q}, 1)[0]
		e.finish(j, o)
		got := e.get(j.ID)
		if got.Worker != "" || !got.LeaseUntil.IsZero() || got.State.Active() && got.State != runnel.StatePolling {
			t.Errorf("%s: state %s worker %q lease %v", o.Kind, got.State, got.Worker, got.LeaseUntil)
		}
		if got.State.Final() == got.FinishedAt.IsZero() {
			t.Errorf("%s: state %s finished at %v", o.Kind, got.State, got.FinishedAt)
		}
	}
}

func testFlowRefusals(t *testing.T, open Opener) {
	spec.Rule(t, "F04")
	e := setup(t, open)
	mk := func() *runnel.Job { j, _ := runnel.NewJob("q", "k", nil); return j }
	huge := runnel.NewFlow()
	for i := range 1001 {
		huge.Add(fmt.Sprintf("s%d", i), mk())
	}
	for name, f := range map[string]*runnel.Flow{
		"empty":         runnel.NewFlow(),
		"unknown step":  runnel.NewFlow().Add("a", mk()).Add("b", mk(), "nope"),
		"cycle":         runnel.NewFlow().Add("a", mk(), "c").Add("b", mk(), "a").Add("c", mk(), "b"),
		"self":          runnel.NewFlow().Add("a", mk(), "a"),
		"duplicate":     runnel.NewFlow().Add("a", mk()).Add("a", mk()),
		"bad step name": runnel.NewFlow().Add("a b", mk()),
		"no job":        runnel.NewFlow().Add("a", nil),
		"1001 steps":    huge,
	} {
		if _, err := e.c.EnqueueFlow(e.ctx, f); !errors.Is(err, runnel.ErrInvalid) {
			t.Errorf("%s flow: %v", name, err)
		}
	}
	if n := e.total(); n != 0 {
		t.Fatalf("refused flows wrote %d jobs", n)
	}
}

func testClaimByIDLimits(t *testing.T, open Opener) {
	spec.Rule(t, "C10", "C06", "C12")
	e := setup(t, open)
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", GroupMaxRunning: 1}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "stopped", Paused: true}); err != nil {
		t.Fatal(err)
	}
	e.add("q", runnel.Group("g"))
	e.claim([]string{"q"}, 1)
	capped := e.add("q", runnel.Group("g"))
	paused := e.add("q", runnel.Pool("stopped"))
	future := e.add("q", runnel.At(e.now().Add(time.Hour)))
	elsewhere := e.add("other")
	byID := func(id string) []*runnel.Job {
		got, err := e.s.Claim(e.ctx, e.ns, runnel.ClaimRequest{Queues: []string{"q"}, IDs: []string{id}, Worker: "w1", Limit: 1, Lease: time.Minute, Now: e.now()})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	for name, id := range map[string]string{"group at its cap": capped.ID, "paused pool": paused.ID, "not due": future.ID,
		"queue not claimed": elsewhere.ID, "unknown": runnel.NewID()} {
		if got := byID(id); len(got) != 0 {
			t.Errorf("claim by id of a job of a %s: %v", name, ids(got))
		}
	}
	free := e.add("q", runnel.Group("h"))
	if got := byID(free.ID); len(got) != 1 || got[0].ID != free.ID {
		t.Fatalf("claim by id of a free job: %v", ids(got))
	}
}

func testStartTimeoutAfterRetry(t *testing.T, open Opener) {
	spec.Rule(t, "T01", "O02")
	e := setup(t, open)
	res := e.add("q", runnel.StartTimeout(300*time.Millisecond), runnel.MaxAttempts(3))
	j := e.claim([]string{"q"}, 1)[0]
	retried := e.finish(j, runnel.Outcome{Kind: runnel.OutcomeRetry, Error: "provider busy", Delay: runnel.Duration(50 * time.Millisecond)})
	if retried.State != runnel.StateWaiting || retried.StartBy.IsZero() || !retried.WaitingSince.Equal(retried.RunAt) ||
		!retried.StartBy.Equal(retried.RunAt.Add(300*time.Millisecond)) {
		t.Fatalf("retried job waiting since %v run at %v start by %v", retried.WaitingSince, retried.RunAt, retried.StartBy)
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", Paused: true}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the retried job to expire", 5*time.Second, func() bool { e.tick(); return e.get(res.ID).State == runnel.StateExpired })
	got := e.get(res.ID)
	if got.Attempt != 1 || len(got.Errors) != 2 || !strings.Contains(got.Errors[1].Error, "not started within") {
		t.Fatalf("expired retried job attempt=%d errors=%+v", got.Attempt, got.Errors)
	}
}
