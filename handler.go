package runnel

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// Handler runs one attempt of a job. Its error picks the outcome: see OutcomeOf.
type Handler interface {
	Work(ctx context.Context, j *Job) error
}

// Poller is implemented by handlers that start outside tasks with Pending. Poll
// checks the task named by the job's Handle.
type Poller interface {
	Poll(ctx context.Context, j *Job) error
}

type HandlerFunc func(ctx context.Context, j *Job) error

func (f HandlerFunc) Work(ctx context.Context, j *Job) error { return f(ctx, j) }

type asyncHandler struct{ submit, poll HandlerFunc }

func (a asyncHandler) Work(ctx context.Context, j *Job) error { return a.submit(ctx, j) }

func (a asyncHandler) Poll(ctx context.Context, j *Job) error { return a.poll(ctx, j) }

// Async builds a handler from a function that starts an outside task and returns
// Pending, and one that polls it.
func Async(submit, poll HandlerFunc) Handler { return asyncHandler{submit: submit, poll: poll} }

// Middleware wraps every handler a worker runs.
type Middleware func(next HandlerFunc) HandlerFunc

// Observer hears about every attempt a worker starts and finishes, for metrics
// or tracing.
type Observer interface {
	Started(j *Job)
	Finished(j *Job, o Outcome, took time.Duration)
}

// Mux routes jobs to handlers by kind.
type Mux struct {
	handlers   map[string]Handler
	middleware []Middleware
}

// NewMux returns a mux that wraps handlers in mw, outermost first.
func NewMux(mw ...Middleware) *Mux { return &Mux{handlers: map[string]Handler{}, middleware: mw} }

func (m *Mux) Handle(kind string, h Handler) { m.handlers[kind] = h }

func (m *Mux) Len() int { return len(m.handlers) }

// Run calls the job's handler, or its poller for a polling job. A panic becomes
// an error.
func (m *Mux) Run(ctx context.Context, j *Job) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	h, ok := m.handlers[j.Kind]
	if !ok {
		return Fail(fmt.Errorf("%w %q", ErrNoHandler, j.Kind))
	}
	fn := HandlerFunc(h.Work)
	if j.Polling() {
		p, ok := h.(Poller)
		if !ok {
			return Fail(fmt.Errorf("kind %q has a handle but no poller", j.Kind))
		}
		fn = p.Poll
	}
	for i := len(m.middleware) - 1; i >= 0; i-- {
		fn = m.middleware[i](fn)
	}
	return fn(ctx, j)
}
