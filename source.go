package runnel

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	finishBatch   = 500
	finishTimeout = 30 * time.Second
)

// Source is where a worker gets jobs: a store directly, or a server through the
// remote package.
type Source interface {
	Claim(ctx context.Context, worker string, queues []string, limit int, lease time.Duration) ([]*Job, error)
	Extend(ctx context.Context, worker string, ids []string, lease time.Duration) ([]string, error)
	Finish(ctx context.Context, worker string, j *Job, o Outcome) error
	Jobs(ctx context.Context, ids []string) ([]*Job, error)
	Wake(ctx context.Context) <-chan struct{}
}

type storeSource struct {
	store Store
	ns    string
	clock Clock
	log   *slog.Logger
	once  sync.Once
	c     *Client

	mu      sync.Mutex
	pending []finishing
	running bool
}

type finishing struct {
	change Change
	done   chan error
}

func NewStoreSource(s Store, ns string) Source {
	return &storeSource{store: s, ns: ns, clock: ClockOf(s), log: slog.Default()}
}

func (s *storeSource) Claim(ctx context.Context, worker string, queues []string, limit int, lease time.Duration) ([]*Job, error) {
	return s.store.Claim(ctx, s.ns, ClaimRequest{Queues: queues, Worker: worker, Limit: limit, Lease: lease, Now: s.clock.Now()})
}

func (s *storeSource) Clock() Clock { return s.clock }

func (s *storeSource) Extend(ctx context.Context, worker string, ids []string, lease time.Duration) ([]string, error) {
	return s.store.Extend(ctx, s.ns, worker, ids, s.clock.Now().Add(lease))
}

func (s *storeSource) Finish(ctx context.Context, worker string, j *Job, o Outcome) error {
	if j.Worker != worker || j.State != StateRunning {
		return ErrConflict
	}
	f := finishing{change: Change{Job: Decide(j, o, s.clock.Now()), From: StateRunning}, done: make(chan error, 1)}
	s.mu.Lock()
	s.pending = append(s.pending, f)
	if !s.running {
		s.running = true
		go s.finisher()
	}
	s.mu.Unlock()
	select {
	case err := <-f.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *storeSource) finisher() {
	for {
		s.mu.Lock()
		n := min(len(s.pending), finishBatch)
		batch := make([]finishing, n)
		copy(batch, s.pending[:n])
		clear(s.pending[:n])
		s.pending = s.pending[n:]
		if len(s.pending) == 0 {
			s.pending = nil
		}
		if len(batch) == 0 {
			s.running = false
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		if s.apply(batch) != nil && len(batch) > 1 {
			for _, f := range batch {
				_ = s.apply([]finishing{f})
			}
		}
	}
}

func (s *storeSource) apply(batch []finishing) error {
	changes := make([]Change, len(batch))
	for i, f := range batch {
		changes[i] = f.change
	}
	ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
	defer cancel()
	ok, err := s.store.Apply(ctx, s.ns, changes, s.clock.Now())
	if err != nil && len(batch) > 1 {
		return err
	}
	for i, f := range batch {
		switch {
		case err != nil:
			f.done <- err
		case !ok[i]:
			f.done <- ErrConflict
		default:
			f.done <- nil
		}
	}
	return err
}

func (s *storeSource) client() *Client {
	s.once.Do(func() { s.c = NewClient(s.store, s.ns) })
	return s.c
}

func (s *storeSource) Jobs(ctx context.Context, ids []string) ([]*Job, error) {
	return s.store.Get(ctx, s.ns, ids)
}

func (s *storeSource) Wake(ctx context.Context) <-chan struct{} {
	w, ok := s.store.(Waker)
	if !ok {
		return nil
	}
	ch, err := w.Subscribe(ctx, s.ns)
	if err != nil {
		s.log.Warn("runnel: wake subscription failed, polling only", "err", err)
		return nil
	}
	return ch
}
