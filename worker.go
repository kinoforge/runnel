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

const (
	defaultConcurrency     = 10
	defaultLease           = 30 * time.Second
	minLease               = 3 * time.Second
	defaultPollInterval    = time.Second
	defaultShutdownTimeout = 30 * time.Second
	claimRetryMin          = 100 * time.Millisecond
	claimRetryMax          = 30 * time.Second
	finishTries            = 6
	finishRetryMin         = 200 * time.Millisecond
	gatherShare            = 4
	gatherWait             = 2 * time.Millisecond
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

	slots atomic.Int64
	mu    sync.Mutex
	runs  map[string]*inflight
	held  int
	base  context.Context //nolint:containedctx // Do runs jobs under the context of the active Run
	wg    *sync.WaitGroup
	freed chan struct{}
}

func NewWorker(s Store, ns string, cfg WorkerConfig) *Worker {
	return NewSourceWorker(NewStoreSource(s, ns), cfg)
}

func NewSourceWorker(src Source, cfg WorkerConfig) *Worker {
	if cfg.ID == "" {
		cfg.ID = defaultWorkerID()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	if cfg.Lease <= 0 {
		cfg.Lease = defaultLease
	}
	cfg.Lease = max(cfg.Lease, minLease)
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if ss, ok := src.(*storeSource); ok {
		ss.log = cfg.Logger
	}
	w := &Worker{src: src, cfg: cfg, log: cfg.Logger, mux: NewMux(cfg.Middleware...), runs: map[string]*inflight{}}
	w.slots.Store(int64(cfg.Concurrency))
	return w
}

// ID returns the worker's id, which leases are held under.
func (w *Worker) ID() string { return w.cfg.ID }

func (w *Worker) Handle(kind string, h Handler) *Worker {
	w.mux.Handle(kind, h)
	return w
}

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
	pause := claimRetryMin
	for ctx.Err() == nil {
		size := w.Concurrency()
		free := w.reserve(size)
		if want := size / gatherShare; free > 0 && free < want && w.busy() {
			w.unreserve(free)
			w.gather(ctx, freed, want)
			free = w.reserve(size)
		}
		if free > 0 {
			jobs, err := w.src.Claim(ctx, w.cfg.ID, w.cfg.Queues, free, w.cfg.Lease)
			if err != nil {
				w.unreserve(free)
				if ctx.Err() != nil {
					break
				}
				w.log.Error("runnel: claim failed", "err", err)
				sleepCtx(ctx, pause)
				pause = min(pause*2, claimRetryMax)
				continue
			}
			pause = claimRetryMin
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
	n := max(min(most, w.Concurrency()-len(w.runs)-w.held), 0)
	w.held += n
	return n
}

func (w *Worker) busy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.runs) > 0
}

func (w *Worker) gather(ctx context.Context, freed <-chan struct{}, want int) {
	t := time.NewTimer(gatherWait)
	defer t.Stop()
	for w.free() < want {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			return
		case <-freed:
		}
	}
}

func (w *Worker) free() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Concurrency() - len(w.runs) - w.held
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
	fin := context.WithoutCancel(base)
	pause := finishRetryMin
	for i := range finishTries {
		ferr := w.src.Finish(fin, w.cfg.ID, done, o)
		if ferr == nil || errors.Is(ferr, ErrConflict) {
			if ferr != nil {
				w.log.Warn("runnel: job changed while running, result dropped", "job", orig.ID)
			}
			break
		}
		w.log.Error("runnel: finishing job failed", "job", orig.ID, "try", i+1, "err", ferr)
		sleepCtx(fin, pause)
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
		sent := maps.Clone(w.runs)
		w.mu.Unlock()
		ids := slices.Collect(maps.Keys(sent))
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
			if r := w.runs[id]; r != nil && r == sent[id] {
				r.lost.Store(true)
				r.cancel(ErrLost)
			}
		}
		w.mu.Unlock()
	}
}
