package storetest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testSwapConfig(t *testing.T, open Opener) {
	spec.Rule(t, "X09")
	e := setup(t, open)
	a, b, c := []byte(`{"name":"p","slots":1}`), []byte(`{"name":"p","slots":2}`), []byte(`{"name":"p","slots":3}`)
	swap := func(ns string, old, next []byte) bool {
		t.Helper()
		ok, err := e.s.SwapConfig(e.ctx, ns, runnel.ConfigPool, "p", old, next)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	stored := func() []byte {
		t.Helper()
		raw, err := e.s.Configs(e.ctx, e.ns, runnel.ConfigPool)
		if err != nil {
			t.Fatal(err)
		}
		return raw["p"]
	}
	if !swap(e.ns, nil, a) {
		t.Fatal("create lost")
	}
	if swap(e.ns, nil, b) {
		t.Fatal("create over an existing value won")
	}
	cur := stored()
	if !swap(e.ns, cur, b) {
		t.Fatal("swap from the current value lost")
	}
	if swap(e.ns, cur, c) {
		t.Fatal("swap from a stale value won")
	}
	if !swap(e.ns, b, b) {
		t.Fatal("swap to the same value lost")
	}
	if got := stored(); !bytes.Equal(got, b) {
		t.Fatalf("stored %s", got)
	}
	other := e.ns + "x"
	if swap(other, b, c) {
		t.Fatal("swap won in a namespace without the value")
	}
	if !swap(other, nil, c) {
		t.Fatal("create lost in another namespace")
	}
	if got := stored(); !bytes.Equal(got, b) {
		t.Fatalf("other namespace changed this one: %s", got)
	}
}

func testUpdateConfig(t *testing.T, open Opener) {
	spec.Rule(t, "X10")
	e := setup(t, open)
	calls := 0
	err := e.c.UpdatePool(e.ctx, "p", func(p *runnel.PoolConfig) {
		calls++
		if calls == 1 {
			if err := e.c.SetPool(e.ctx, runnel.PoolConfig{Name: "p", Paused: true, Rate: 2}); err != nil {
				t.Fatal(err)
			}
		}
		p.Slots = 5
	})
	if err != nil || calls != 2 {
		t.Fatalf("update %v after %d calls", err, calls)
	}
	pools, err := e.c.Pools(e.ctx)
	if got := pools["p"]; err != nil || got.Slots != 5 || !got.Paused || got.Rate != 2 {
		t.Fatalf("pool %+v %v", got, err)
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", MaxRunning: 3}); err != nil {
		t.Fatal(err)
	}
	if err := e.c.UpdateQueue(e.ctx, "q", func(q *runnel.QueueConfig) { q.Paused = true }); err != nil {
		t.Fatal(err)
	}
	err = e.c.UpdateQueue(e.ctx, "q", func(q *runnel.QueueConfig) { q.MaxRunning = -1 })
	if !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("bad update: %v", err)
	}
	queues, err := e.c.Queues(e.ctx)
	if got := queues["q"]; err != nil || got.MaxRunning != 3 || !got.Paused {
		t.Fatalf("queue %+v %v", got, err)
	}
}

func testQueueCallback(t *testing.T, open Opener) {
	spec.Rule(t, "J03")
	e := setup(t, open)
	const hook, own = "https://example.com/q", "https://example.com/own"
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", CallbackURL: hook}); err != nil {
		t.Fatal(err)
	}
	if got := e.get(e.add("q").ID).CallbackURL; got != hook {
		t.Fatalf("queue default callback %q", got)
	}
	if got := e.get(e.add("q", runnel.Callback(own)).ID).CallbackURL; got != own {
		t.Fatalf("own callback %q", got)
	}
	if got := e.get(e.add("other").ID).CallbackURL; got != "" {
		t.Fatalf("other queue callback %q", got)
	}
	if err := e.c.SetQueue(e.ctx, runnel.QueueConfig{Name: "q", CallbackURL: "ftp://example.com"}); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("bad queue callback: %v", err)
	}
}
