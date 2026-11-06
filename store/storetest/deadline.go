package storetest

import (
	"errors"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func testDeadline(t *testing.T, open Opener) {
	spec.Rule(t, "T06")
	e := setup(t, open)
	m := runnel.NewMaintainer(e.s, runnel.MaintainerConfig{Namespaces: []string{e.ns}})
	ttl := runnel.TTL(60 * time.Millisecond)
	parent := e.add("p")
	late := []string{e.add("q", ttl).ID, e.add("r", ttl).ID, e.add("p", ttl, runnel.After(parent.ID)).ID}
	free := e.add("q")
	running := e.claim([]string{"r"}, 1)
	if len(running) != 1 || running[0].Deadline.IsZero() {
		t.Fatalf("claimed %+v", running)
	}
	time.Sleep(90 * time.Millisecond)
	if err := m.Tick(e.ctx, e.ns); err != nil {
		t.Fatal(err)
	}
	for _, id := range late {
		j := e.get(id)
		if j.State != runnel.StateExpired || len(j.Errors) == 0 || j.Errors[len(j.Errors)-1].Error != "deadline passed" {
			t.Fatalf("%s past its deadline: %s %+v", id, j.State, j.Errors)
		}
	}
	if j := e.get(free.ID); j.State != runnel.StateWaiting {
		t.Fatalf("job without deadline %s", j.State)
	}
	lost, err := e.s.Extend(e.ctx, e.ns, "w1", []string{running[0].ID}, e.now().Add(time.Minute))
	if err != nil || len(lost) != 1 {
		t.Fatalf("heartbeat past the deadline kept the lease: %v %v", lost, err)
	}
	testDeadlineRetry(t, e)
	testDeadlineLimits(t, e)
}

func testDeadlineRetry(t *testing.T, e *env) {
	j := e.add("s", runnel.TTL(time.Minute), runnel.MaxAttempts(3))
	got := e.claim([]string{"s"}, 1)
	if len(got) != 1 {
		t.Fatalf("claimed %d", len(got))
	}
	n := e.finish(got[0], runnel.Outcome{Kind: runnel.OutcomeRetry, Error: "x", Delay: runnel.Duration(time.Hour)})
	if n.State != runnel.StateExpired || e.get(j.ID).State != runnel.StateExpired {
		t.Fatalf("retry after the deadline %s", n.State)
	}
	again, err := e.c.Retry(e.ctx, j.ID)
	if err != nil || again.State != runnel.StateWaiting || again.Deadline.IsZero() {
		t.Fatalf("retry of a job before its deadline %+v %v", again, err)
	}
}

func testDeadlineLimits(t *testing.T, e *env) {
	_, err := e.c.Enqueue(e.ctx, "q", "k", nil, runnel.TTL(time.Second), runnel.StartTimeout(time.Minute))
	if !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("start timeout past the deadline: %v", err)
	}
	cfg := runnel.QueueConfig{Name: "d", Timeout: runnel.Duration(time.Hour), TTL: runnel.Duration(time.Minute)}
	if err := e.c.SetQueue(e.ctx, cfg); err != nil {
		t.Fatal(err)
	}
	j := e.get(e.add("d").ID)
	if left := j.Deadline.Sub(j.CreatedAt); left != time.Minute || j.Timeout != 0 {
		t.Fatalf("queue defaults: deadline in %v, timeout %v", left, j.Timeout)
	}
}
