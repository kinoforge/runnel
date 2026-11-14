package storetest

import (
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testRoundTrip(t *testing.T, open Opener) {
	spec.Rule(t, "J01", "J03")
	e := setup(t, open)
	res := e.add("q", runnel.Priority(3), runnel.Group("acme"), runnel.Pool("p1"), runnel.MaxAttempts(7),
		runnel.Meta(map[string]string{"a": "b"}), runnel.StartTimeout(time.Minute),
		runnel.Callback("https://example.com/h"), runnel.Timeout(time.Second))
	j := e.get(res.ID)
	if j.State != runnel.StateWaiting || j.Priority != 3 || j.Group != "acme" || j.Pool != "p1" || j.MaxAttempts != 7 {
		t.Fatalf("bad job %+v", j)
	}
	if j.Meta["a"] != "b" || j.CallbackURL == "" || j.Timeout.D() != time.Second {
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
	if plain := e.get(e.add("q").ID); plain.MaxAttempts != runnel.DefaultMaxAttempts {
		t.Fatalf("default attempts %d", plain.MaxAttempts)
	}
}

func testScheduled(t *testing.T, open Opener) {
	spec.Rule(t, "J05")
	e := setup(t, open)
	e.add("q", runnel.In(time.Hour))
	if got := e.claim([]string{"q"}, 5); len(got) != 0 {
		t.Fatalf("future job claimed")
	}
	soon := e.add("q", runnel.In(300*time.Millisecond))
	time.Sleep(400 * time.Millisecond)
	if got := e.claim([]string{"q"}, 5); len(got) != 1 || got[0].ID != soon.ID {
		t.Fatalf("due job not claimed: %v", ids(got))
	}
}

func testUnique(t *testing.T, open Opener) {
	spec.Rule(t, "J06")
	e := setup(t, open)
	a := e.add("q", runnel.Unique("order-1", 0))
	b := e.add("q", runnel.Unique("order-1", 0))
	if !b.Duplicate || b.ID != a.ID {
		t.Fatalf("not deduped: %+v %+v", a, b)
	}
	got := e.claim([]string{"q"}, 5)
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	c := e.add("q", runnel.Unique("order-1", 0))
	if c.Duplicate || c.ID == a.ID {
		t.Fatalf("unique not released after finish: %+v", c)
	}
}

func testUniqueWindow(t *testing.T, open Opener) {
	spec.Rule(t, "J07")
	e := setup(t, open)
	a := e.add("q", runnel.Unique("w", time.Hour))
	got := e.claim([]string{"q"}, 5)
	e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeDone})
	b := e.add("q", runnel.Unique("w", time.Hour))
	if !b.Duplicate || b.ID != a.ID {
		t.Fatalf("window not kept after finish: %+v", b)
	}
	e.add("q", runnel.Unique("short", 200*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	if d := e.add("q", runnel.Unique("short", 200*time.Millisecond)); d.Duplicate {
		t.Fatalf("expired window still deduped")
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
	dup := e.add("q", runnel.Unique("shared", 0))
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
	res := e.add("q")
	for _, j := range e.claim([]string{"q"}, 1) {
		e.finish(j, runnel.Outcome{Kind: runnel.OutcomeDone})
	}
	e.add("q")
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
	res := e.add("q")
	if err := e.s.Migrate(e.ctx); err != nil {
		t.Fatalf("migrating a migrated store: %v", err)
	}
	if j := e.get(res.ID); j.State != runnel.StateWaiting {
		t.Fatalf("job after migrate: %s", j.State)
	}
}

func testStorage(t *testing.T, open Opener) {
	spec.Rule(t, "X08")
	e := setup(t, open)
	sr, ok := e.s.(runnel.StorageReporter)
	if !ok {
		t.Skip("store reports no storage")
	}
	e.add("q")
	info, err := sr.Storage(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Kind == "" || info.Bytes < 0 {
		t.Fatalf("storage %+v", info)
	}
}
