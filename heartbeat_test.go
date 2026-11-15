package runnel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

type staleBeat struct {
	mu       sync.Mutex
	queue    chan *Job
	extended chan struct{}
	release  chan struct{}
	calls    int
}

func (s *staleBeat) Claim(ctx context.Context, worker string, queues []string, limit int, lease time.Duration) ([]*Job, error) {
	select {
	case j := <-s.queue:
		c := j.Clone()
		c.State, c.Worker = StateRunning, worker
		return []*Job{c}, nil
	default:
		return nil, nil
	}
}

func (s *staleBeat) Extend(ctx context.Context, worker string, ids []string, lease time.Duration) ([]string, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	s.mu.Unlock()
	if !first {
		return nil, nil
	}
	close(s.extended)
	<-s.release
	return ids, nil
}

func (s *staleBeat) Finish(context.Context, string, *Job, Outcome) error { return nil }

func (s *staleBeat) Jobs(context.Context, []string) ([]*Job, error) { return nil, nil }

func (s *staleBeat) Wake(context.Context) <-chan struct{} { return nil }

func TestStaleHeartbeatSparesTheNextRun(t *testing.T) {
	spec.Rule(t, "L01", "L05")
	src := &staleBeat{queue: make(chan *Job, 2), extended: make(chan struct{}), release: make(chan struct{})}
	j := &Job{ID: NewID(), Queue: "q", Kind: "k", MaxAttempts: 5}
	src.queue <- j
	second := make(chan error, 1)
	var runs int
	w := NewSourceWorker(src, WorkerConfig{Queues: []string{"q"}, Concurrency: 1, Lease: 60 * time.Millisecond, PollInterval: 5 * time.Millisecond, ShutdownTimeout: time.Second})
	w.HandleFunc("k", func(ctx context.Context, _ *Job) error {
		runs++
		if runs == 1 {
			<-src.extended
			return nil
		}
		select {
		case <-ctx.Done():
			second <- context.Cause(ctx)
		case <-time.After(300 * time.Millisecond):
			second <- nil
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	<-src.extended
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		idle := len(w.runs) == 0
		w.mu.Unlock()
		if idle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first run never ended")
		}
		time.Sleep(time.Millisecond)
	}
	src.queue <- j
	for {
		w.mu.Lock()
		again := w.runs[j.ID] != nil
		w.mu.Unlock()
		if again {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second run never started")
		}
		time.Sleep(time.Millisecond)
	}
	close(src.release)
	if err := <-second; err != nil {
		t.Fatalf("a heartbeat sent during the first run cancelled the second with %v", err)
	}
}
