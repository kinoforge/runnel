package runnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerConfig configures a worker. Concurrency is how many jobs it runs at once
// (default 10). Lease is how long a claim lasts without a heartbeat (default
// 30s); heartbeats run every third of it. ShutdownTimeout bounds how long Run
// waits for running jobs after its context ends.
type WorkerConfig struct {
	ID              string
	Queues          []string
	Concurrency     int
	Lease           time.Duration
	PollInterval    time.Duration
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
	Observer        Observer
	Middleware      []Middleware
}

type inflight struct {
	job    *Job
	cancel context.CancelCauseFunc
	lost   atomic.Bool
}

// Worker claims jobs from a source and runs them with its handlers, heartbeating
// their leases and reporting outcomes.
type Worker struct {
	src Source
	cfg WorkerConfig
	log *slog.Logger
	mux *Mux

	mu    sync.Mutex
	runs  map[string]*inflight
	held  int
	base  context.Context
	wg    *sync.WaitGroup
	freed chan struct{}
}

// NewWorker returns a worker for namespace ns of store s.
func NewWorker(s Store, ns string, cfg WorkerConfig) *Worker {
	return NewSourceWorker(NewStoreSource(s, ns), cfg)
}

// NewSourceWorker returns a worker that reads from src.
func NewSourceWorker(src Source, cfg WorkerConfig) *Worker {
	if cfg.ID == "" {
		cfg.ID = defaultWorkerID()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	cfg.Lease = max(cfg.Lease, 3*time.Second)
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if ss, ok := src.(*storeSource); ok {
		ss.log = cfg.Logger
	}
	return &Worker{src: src, cfg: cfg, log: cfg.Logger, mux: NewMux(cfg.Middleware...), runs: map[string]*inflight{}}
}

// ID returns the worker's id, which leases are held under.
func (w *Worker) ID() string { return w.cfg.ID }

// Handle registers the handler for a kind.
func (w *Worker) Handle(kind string, h Handler) *Worker {
	w.mux.Handle(kind, h)
	return w
}

// HandleFunc registers a function as the handler for a kind.
func (w *Worker) HandleFunc(kind string, fn HandlerFunc) *Worker { return w.Handle(kind, fn) }

// Run claims and runs jobs until ctx ends, then waits up to ShutdownTimeout for
// running jobs before releasing them.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.cfg.Queues) == 0 {
		return invalid("worker needs at least one queue")
	}
	if w.mux.Len() == 0 {
		return invalid("worker has no handlers")
	}
	base, stopAll := context.WithCancelCause(context.WithoutCancel(ctx))
	defer stopAll(nil)
	wake := w.src.Wake(ctx)
	freed := make(chan struct{}, 1)
	stopBeat := make(chan struct{})
	beatDone := make(chan struct{})
	go w.heartbeat(base, stopBeat, beatDone)
	var wg sync.WaitGroup
	w.mu.Lock()
	w.base, w.wg, w.freed = base, &wg, freed
	w.mu.Unlock()
	pause := 100 * time.Millisecond
	for ctx.Err() == nil {
		free := w.reserve(w.cfg.Concurrency)
		if free > 0 {
			jobs, err := w.src.Claim(ctx, w.cfg.ID, w.cfg.Queues, free, w.cfg.Lease)
			if err != nil {
				w.unreserve(free)
				if ctx.Err() != nil {
					break
				}
				w.log.Error("runnel: claim failed", "err", err)
				sleepCtx(ctx, pause)
				pause = min(pause*2, 30*time.Second)
				continue
			}
			pause = 100 * time.Millisecond
			for _, j := range jobs {
				w.start(base, j, &wg, freed)
			}
			w.unreserve(free)
			if len(jobs) == free {
				continue
			}
		}
		var freedC <-chan struct{}
		if free <= 0 {
			freedC = freed
		}
		t := time.NewTimer(w.cfg.PollInterval)
		select {
		case <-ctx.Done():
		case <-wake:
		case <-freedC:
		case <-t.C:
		}
		t.Stop()
	}
	w.mu.Lock()
	w.base, w.wg, w.freed = nil, nil, nil
	w.mu.Unlock()
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(w.cfg.ShutdownTimeout):
		stopAll(ErrShutdown)
		<-drained
	}
	close(stopBeat)
	<-beatDone
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (w *Worker) reserve(most int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := max(min(most, w.cfg.Concurrency-len(w.runs)-w.held), 0)
	w.held += n
	return n
}

func (w *Worker) unreserve(n int) {
	w.mu.Lock()
	w.held -= n
	w.mu.Unlock()
}

func (w *Worker) track(base context.Context, j *Job) (*inflight, context.Context) {
	jctx, cancel := context.WithCancelCause(base)
	r := &inflight{job: j, cancel: cancel}
	w.mu.Lock()
	w.runs[j.ID] = r
	w.mu.Unlock()
	return r, jctx
}

func (w *Worker) start(base context.Context, j *Job, wg *sync.WaitGroup, freed chan struct{}) {
	r, jctx := w.track(base, j)
	wg.Go(func() { w.runOne(base, jctx, r, freed) })
}

func (w *Worker) runOne(base, jctx context.Context, r *inflight, freed chan struct{}) {
	defer func() {
		w.mu.Lock()
		delete(w.runs, r.job.ID)
		w.mu.Unlock()
		r.cancel(nil)
		select {
		case freed <- struct{}{}:
		default:
		}
	}()
	w.execute(base, jctx, r)
}

// Do enqueues j and waits for it to finish, as Client.EnqueueAndWait does.
// When a slot of this worker and the job's limits allow, the first attempt
// runs at once on the calling goroutine instead of going through the claim
// loop. The job is an ordinary job: a failed attempt is retried by the
// workers like any other, and Do keeps waiting until it is final or ctx ends.
// The worker must be running, read from a store and serve the job's queue.
func (w *Worker) Do(ctx context.Context, j *Job) (*Job, error) {
	ss, ok := w.src.(*storeSource)
	if !ok {
		return nil, invalid("direct start needs a worker that reads from a store")
	}
	if _, ok := w.mux.handlers[j.Kind]; !ok {
		return nil, fmt.Errorf("%w %q", ErrNoHandler, j.Kind)
	}
	if !slices.Contains(w.cfg.Queues, j.Queue) {
		return nil, invalid("worker does not serve queue %q", j.Queue)
	}
	w.mu.Lock()
	base, wg, freed := w.base, w.wg, w.freed
	if wg != nil {
		wg.Add(1)
	}
	w.mu.Unlock()
	if base == nil {
		return nil, invalid("worker is not running")
	}
	defer wg.Done()
	c := ss.client()
	res, err := c.EnqueueJobs(ctx, []*Job{j})
	if err != nil {
		return nil, err
	}
	if res[0].Err != nil {
		return nil, res[0].Err
	}
	if !res[0].Duplicate && base.Err() == nil && w.reserve(1) == 1 {
		got, err := ss.store.Claim(ctx, ss.ns, ClaimRequest{Queues: []string{j.Queue}, IDs: []string{res[0].ID}, Worker: w.cfg.ID,
			Limit: 1, Lease: w.cfg.Lease, Now: ss.clock.Now()})
		var r *inflight
		var jctx context.Context
		if err == nil && len(got) == 1 {
			r, jctx = w.track(base, got[0])
		}
		w.unreserve(1)
		if err != nil {
			return nil, err
		}
		if r != nil {
			w.runOne(base, jctx, r, freed)
		}
	}
	return c.Wait(ctx, res[0].ID)
}

func (w *Worker) execute(base, jctx context.Context, r *inflight) {
	orig := r.job
	work := orig.Clone()
	began := time.Now()
	ctx, stop := Attempt(jctx, work, w.src)
	defer stop()
	if w.cfg.Observer != nil {
		w.cfg.Observer.Started(orig)
	}
	err := w.mux.Run(ctx, work)
	if r.lost.Load() {
		w.log.Warn("runnel: lease lost, result dropped", "job", orig.ID, "kind", orig.Kind)
		return
	}
	o := Classify(ctx, work, err)
	done := orig.Clone()
	done.Payload, done.Meta, done.Result = work.Payload, work.Meta, work.Result
	pause := 200 * time.Millisecond
	for i := range 6 {
		ferr := w.src.Finish(base, w.cfg.ID, done, o)
		if ferr == nil || errors.Is(ferr, ErrConflict) {
			if ferr != nil {
				w.log.Warn("runnel: job changed while running, result dropped", "job", orig.ID)
			}
			break
		}
		w.log.Error("runnel: finishing job failed", "job", orig.ID, "try", i+1, "err", ferr)
		sleepCtx(base, pause)
		pause *= 2
	}
	if w.cfg.Observer != nil {
		w.cfg.Observer.Finished(done, o, time.Since(began))
	}
}

func (w *Worker) heartbeat(ctx context.Context, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(w.cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		w.mu.Lock()
		ids := slices.Collect(maps.Keys(w.runs))
		w.mu.Unlock()
		if len(ids) == 0 {
			continue
		}
		hctx, cancel := context.WithTimeout(ctx, w.cfg.Lease/3)
		lost, err := w.src.Extend(hctx, w.cfg.ID, ids, w.cfg.Lease)
		cancel()
		if err != nil {
			w.log.Error("runnel: heartbeat failed", "err", err)
			continue
		}
		w.mu.Lock()
		for _, id := range lost {
			if r := w.runs[id]; r != nil {
				r.lost.Store(true)
				r.cancel(ErrLost)
			}
		}
		w.mu.Unlock()
	}
}
