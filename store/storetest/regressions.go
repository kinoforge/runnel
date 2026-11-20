package storetest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func (e *env) deliveries() []callbackTask {
	e.t.Helper()
	hooks, _, err := e.c.List(e.ctx, runnel.Query{Queue: runnel.CallbackQueue, Limit: 50})
	if err != nil {
		e.t.Fatal(err)
	}
	out := make([]callbackTask, len(hooks))
	for i, h := range hooks {
		if err := h.Bind(&out[i]); err != nil {
			e.t.Fatal(err)
		}
	}
	return out
}

type callbackTask struct {
	Job struct {
		ID     string
		Result json.RawMessage
	}
}

func testCallbackLargeResult(t *testing.T, open Opener) {
	spec.Rule(t, "H01")
	e := setup(t, open)
	big := []byte(`"` + strings.Repeat("r", 1<<20-16) + `"`)
	finishWith := func(result []byte) string {
		id := e.add("q", runnel.Callback("https://example.com/hook")).ID
		for _, j := range e.claim([]string{"q"}, 1) {
			e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone, Result: result})
		}
		return id
	}
	tick := func(cfg runnel.CallbackConfig) {
		cfg.Enabled = true
		m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}, Callbacks: cfg})
		if err := m.Tick(e.ctx, e.ns); err != nil {
			t.Fatal(err)
		}
	}
	trimmed := finishWith(big)
	small := finishWith([]byte(`{"ok":true}`))
	tick(runnel.CallbackConfig{MaxPayload: 512 << 10})
	kept := finishWith(big)
	tick(runnel.CallbackConfig{})
	got := map[string]json.RawMessage{}
	for _, d := range e.deliveries() {
		got[d.Job.ID] = d.Job.Result
	}
	if len(got) != 3 {
		t.Fatalf("%d deliveries for 3 callbacks", len(got))
	}
	if r, ok := got[trimmed]; !ok || r != nil {
		t.Fatalf("a delivery over callbacks.max_payload kept a %d byte result", len(r))
	}
	if !bytes.Equal(got[small], []byte(`{"ok":true}`)) || len(got[kept]) != len(big) {
		t.Fatalf("results that fit were not kept: %q, %d bytes", got[small], len(got[kept]))
	}
	if cur, _ := e.s.Cursor(e.ctx, e.ns, "callbacks"); cur == 0 {
		t.Fatal("the callback cursor did not move past the large result")
	}
}

func testCronFarBehind(t *testing.T, open Opener) {
	spec.Rule(t, "T02")
	e := setup(t, open)
	s := runnel.Schedule{Name: "tick", Spec: "*/5 * * * *", Queue: "house", Kind: "k", Meta: map[string]string{"schedule": "fake", "fire_at": "fake", "x": "1"}}
	if err := e.c.SetSchedule(e.ctx, s); err != nil {
		t.Fatal(err)
	}
	now := e.now()
	if ok, err := e.s.SwapCursor(e.ctx, e.ns, "cron:tick", 0, runnel.Millis(now.Add(-30*24*time.Hour))); err != nil || !ok {
		t.Fatalf("set the cursor back: %v %v", ok, err)
	}
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	jobs, _, err := e.c.List(e.ctx, runnel.Query{Queue: "house", Limit: 10})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("%d jobs after 30 days behind, want 1: %v", len(jobs), err)
	}
	j := jobs[0]
	at, err := time.Parse(time.RFC3339, j.Meta["fire_at"])
	if err != nil || at.Minute()%5 != 0 || at.Second() != 0 || at.After(e.now()) || at.Before(now.Add(-5*time.Minute)) {
		t.Fatalf("fired for %q, want the last 5 minute tick before %s: %v", j.Meta["fire_at"], now, err)
	}
	if j.Meta["schedule"] != "tick" || j.Meta["x"] != "1" {
		t.Fatalf("meta %v", j.Meta)
	}
	bad := s
	bad.Meta = metaN(31)
	if err := e.c.SetSchedule(e.ctx, bad); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("a schedule with no room for its own meta was taken: %v", err)
	}
	bad.Meta, bad.MaxAttempts = nil, -1
	if err := e.c.SetSchedule(e.ctx, bad); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("negative max_attempts taken: %v", err)
	}
}

func metaN(n int) map[string]string {
	m := map[string]string{}
	for i := range n {
		m[string(rune('a'+i%26))+strings.Repeat("k", i/26+1)] = "v"
	}
	return m
}

func testRetryBlockedEnded(t *testing.T, open Opener) {
	spec.Rule(t, "M02", "T06")
	e := setup(t, open)
	parent := e.add("q")
	cancelled := e.add("q", runnel.After(parent.ID))
	expired := e.add("q", runnel.After(parent.ID), runnel.TTL(time.Second))
	if _, err := e.c.Cancel(e.ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	if j := e.get(expired.ID); j.State != runnel.StateExpired {
		t.Fatalf("blocked job past its deadline is %s", j.State)
	}
	for _, id := range []string{cancelled.ID, expired.ID} {
		if _, err := e.c.Retry(e.ctx, id); !errors.Is(err, runnel.ErrDependency) {
			t.Fatalf("retry before the dependency completed: %v", err)
		}
	}
	for _, j := range e.claim([]string{"q"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	for _, id := range []string{cancelled.ID, expired.ID} {
		j, err := e.c.Retry(e.ctx, id)
		if err != nil || j.State != runnel.StateWaiting || j.Pending != 0 {
			t.Fatalf("retry of %s once its dependency completed: %+v %v", id, j, err)
		}
	}
}

func testPruneSkipsRetried(t *testing.T, open Opener) {
	spec.Rule(t, "T03", "L04")
	e := setup(t, open)
	const n = 40
	for range n {
		e.add("q")
	}
	jobs := e.claim([]string{"q"}, n)
	for _, j := range jobs {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeFail, Error: "x"})
	}
	retried := make(chan string, n)
	go func() {
		defer close(retried)
		for _, j := range jobs[:n/2] {
			if _, err := e.c.Retry(e.ctx, j.ID); err == nil {
				retried <- j.ID
			}
		}
	}()
	for range 20 {
		if _, err := e.s.Prune(e.ctx, e.ns, runnel.PruneRequest{States: []runnel.State{runnel.StateDead}, Before: e.now().Add(time.Hour), Limit: 3}); err != nil {
			t.Fatal(err)
		}
	}
	for id := range retried {
		j, err := e.c.Get(e.ctx, id)
		if err != nil {
			t.Fatalf("retried job %s was pruned: %v", id, err)
		}
		if j.State != runnel.StateWaiting {
			t.Fatalf("retried job %s is %s", id, j.State)
		}
	}
}

func testUniqueHolderEnds(t *testing.T, open Opener) {
	spec.Rule(t, "J06")
	e := setup(t, open)
	for round := range 15 {
		q := "u" + string(rune('a'+round))
		holder := e.add(q, runnel.Unique("k"+q, 0))
		held := e.claim([]string{q}, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			e.finish(held[0], runnel.Outcome{Kind: runnel.OutcomeDone})
		}()
		next := e.add(q, runnel.Unique("k"+q, 0))
		<-done
		if next.Duplicate {
			if next.ID != holder.ID {
				t.Fatalf("duplicate of %s, want %s", next.ID, holder.ID)
			}
			continue
		}
		third := e.add(q, runnel.Unique("k"+q, 0))
		if !third.Duplicate || third.ID != next.ID {
			t.Fatalf("round %d: the key of waiting job %s is not held: %+v", round, next.ID, third)
		}
	}
}

func testClaimByIDDue(t *testing.T, open Opener) {
	spec.Rule(t, "C10", "C09")
	e := setup(t, open)
	poll := e.add("q")
	for _, j := range e.claim([]string{"q"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomePending, Handle: "h", Delay: runnel.Duration(time.Millisecond)})
	}
	later := e.add("q", runnel.In(500*time.Millisecond))
	time.Sleep(600 * time.Millisecond)
	req := runnel.ClaimRequest{Queues: []string{"q", "q"}, Worker: "w1", Limit: 5, Lease: 30 * time.Second, Now: e.now(),
		IDs: []string{poll.ID, later.ID, poll.ID, later.ID}}
	got, err := e.s.Claim(e.ctx, e.ns, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("claim by id of a due polling and a due delayed job: %v", ids(got))
	}
	if n := e.count("q", runnel.StateRunning); n != 2 {
		t.Fatalf("%d running after a claim with repeated ids, want 2", n)
	}
}

func testPausedPoolSwap(t *testing.T, open Opener) {
	spec.Rule(t, "X09", "C12")
	e := setup(t, open)
	until := e.now().Add(time.Hour).Truncate(time.Millisecond).UTC()
	if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "p", Slots: 2, PausedUntil: until}); err != nil {
		t.Fatal(err)
	}
	raw, err := e.s.Configs(e.ctx, e.ns, runnel.ConfigPool)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw["p"]), "_ms") {
		t.Fatalf("stored setting leaks store fields: %s", raw["p"])
	}
	ok, err := e.s.SwapConfig(e.ctx, e.ns, runnel.ConfigPool, "p", raw["p"], []byte(`{"name":"p","slots":3}`))
	if err != nil || !ok {
		t.Fatalf("swap from the value read lost: %v %v", ok, err)
	}
	e.add("q", runnel.Pool("p"))
	if got := e.claim([]string{"q"}, 1); len(got) != 1 {
		t.Fatal("a pool swapped to unpaused still starts nothing")
	}
}

func testDelayedBlockedStartBy(t *testing.T, open Opener) {
	spec.Rule(t, "T01", "F01")
	e := setup(t, open)
	parent := e.add("q")
	child := e.add("q", runnel.After(parent.ID), runnel.In(time.Hour), runnel.StartTimeout(time.Minute))
	for _, j := range e.claim([]string{"q"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	j := e.get(child.ID)
	if j.State != runnel.StateWaiting || j.RunAt.Before(e.now().Add(50*time.Minute)) {
		t.Fatalf("released child %s runs at %s", j.State, j.RunAt)
	}
	if want := j.RunAt.Add(time.Minute); !j.StartBy.Equal(want) {
		t.Fatalf("start by %s, want %s", j.StartBy, want)
	}
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	if j := e.get(child.ID); j.State != runnel.StateWaiting {
		t.Fatalf("a delayed job expired before its start time: %s", j.State)
	}
}

func testPruneByState(t *testing.T, open Opener) {
	spec.Rule(t, "T03")
	e := setup(t, open)
	for range 30 {
		e.add("old")
	}
	for _, j := range e.claim([]string{"old"}, 30) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeFail, Error: "x"})
	}
	for range 3 {
		e.add("new")
	}
	for _, j := range e.claim([]string{"new"}, 3) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	n, err := e.s.Prune(e.ctx, e.ns, runnel.PruneRequest{States: []runnel.State{runnel.StateCompleted}, Before: e.now().Add(time.Hour), Limit: 2})
	if err != nil || n != 2 {
		t.Fatalf("pruned %d completed jobs behind 30 older dead ones, want 2: %v", n, err)
	}
	if left := e.count("new", runnel.StateCompleted); left != 1 {
		t.Fatalf("%d completed left, want 1", left)
	}
	if dead := e.count("old", runnel.StateDead); dead != 30 {
		t.Fatalf("pruning completed jobs removed dead ones: %d left", dead)
	}
}

func testEnqueueTxReadsSettings(t *testing.T, open Opener) {
	spec.Rule(t, "J09")
	e := setup(t, open)
	dbs, ok := e.s.(interface{ DB() *sql.DB })
	if _, tx := e.s.(runnel.TxInserter); !ok || !tx {
		t.Skip("store has no SQL transactions")
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxAttempts: 9}); err != nil {
		t.Fatal(err)
	}
	e.add("q")
	other := runnel.NewClient(e.s, e.ns)
	if err := other.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxAttempts: 4}); err != nil {
		t.Fatal(err)
	}
	tx, err := dbs.DB().BeginTx(e.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := runnel.NewJob("q", "k", nil)
	res, err := e.c.EnqueueTx(e.ctx, tx, j)
	if err != nil || res[0].Err != nil {
		_ = tx.Rollback()
		t.Fatalf("enqueue in tx: %v %+v", err, res)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := e.get(res[0].ID).MaxAttempts; got != 4 {
		t.Fatalf("job in a transaction took max attempts %d from a cached setting, want 4 read in the transaction", got)
	}
}

type failOnce struct {
	runnel.Store
	failed bool
}

func (f *failOnce) Insert(ctx context.Context, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	if !f.failed {
		f.failed = true
		return nil, errors.New("connection reset")
	}
	return f.Store.Insert(ctx, ns, jobs, now)
}

func testFlowRetry(t *testing.T, open Opener) {
	spec.Rule(t, "F04")
	e := setup(t, open)
	c := runnel.NewClient(&failOnce{Store: e.s}, e.ns)
	a, _ := runnel.NewJob("q", "k", nil)
	b, _ := runnel.NewJob("q", "k", nil)
	f := runnel.NewFlow().Add("a", a).Add("b", b, "a")
	if _, err := c.EnqueueFlow(e.ctx, f); err == nil {
		t.Fatal("the first try did not fail")
	}
	res, err := c.EnqueueFlow(e.ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.get(res["b"].ID).DependsOn; len(got) != 1 || got[0] != res["a"].ID {
		t.Fatalf("after a retried flow, b depends on %v, want [%s]", got, res["a"].ID)
	}
}
