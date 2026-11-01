package runnel

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestMux(t *testing.T) {
	spec.Rule(t, "R03", "R04")
	var order []string
	named := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, j *Job) error {
				order = append(order, name)
				return next(ctx, j)
			}
		}
	}
	m := NewMux(named("outer"), named("inner"))
	m.Handle("ok", HandlerFunc(func(context.Context, *Job) error {
		order = append(order, "handler")
		return nil
	}))
	m.Handle("boom", HandlerFunc(func(context.Context, *Job) error { panic("boom") }))
	if err := m.Run(context.Background(), &Job{Kind: "ok"}); err != nil || !slices.Equal(order, []string{"outer", "inner", "handler"}) {
		t.Fatalf("middleware order %v, %v", order, err)
	}
	err := m.Run(context.Background(), &Job{Kind: "boom"})
	if o := OutcomeOf(err, nil); err == nil || o.Kind != OutcomeRetry || !strings.Contains(o.Error, "panic: boom") {
		t.Fatalf("panic became %+v", o)
	}
}

func TestClassify(t *testing.T) {
	spec.Rule(t, "R05", "R06")
	j := &Job{Timeout: Duration(10 * time.Millisecond)}
	ctx, stop := Attempt(context.Background(), j, nil)
	defer stop()
	<-ctx.Done()
	if o := Classify(ctx, j, ctx.Err()); !errors.Is(context.Cause(ctx), ErrTimeout) || o.Kind != OutcomeRetry || !strings.Contains(o.Error, "timed out") {
		t.Fatalf("timeout became %+v", o)
	}
	base, cancel := context.WithCancelCause(context.Background())
	cancel(ErrShutdown)
	if o := Classify(base, j, base.Err()); o.Kind != OutcomeRelease {
		t.Fatalf("shutdown became %+v", o)
	}
}

func TestWorkerNeedsQueuesAndHandlers(t *testing.T) {
	spec.Rule(t, "R07")
	if err := NewSourceWorker(nil, WorkerConfig{}).Run(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("worker without queues: %v", err)
	}
	if err := NewSourceWorker(nil, WorkerConfig{Queues: []string{"q"}}).Run(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("worker without handlers: %v", err)
	}
}

func TestEnqueueLimits(t *testing.T) {
	spec.Rule(t, "J04", "F04")
	c := NewClient(nil, "app")
	ctx := context.Background()
	jobs := make([]*Job, 1001)
	for i := range jobs {
		jobs[i] = &Job{Queue: "q", Kind: "k"}
	}
	if _, err := c.EnqueueJobs(ctx, jobs); !errors.Is(err, ErrInvalid) {
		t.Fatalf("1001 jobs in one call: %v", err)
	}
	if _, err := c.EnqueueFlow(ctx, NewFlow()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("flow without steps: %v", err)
	}
	if _, err := c.EnqueueFlow(ctx, NewFlow().Add("a", &Job{Queue: "q", Kind: "k"}, "missing")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("step after an unknown step: %v", err)
	}
}
