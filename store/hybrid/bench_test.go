package hybrid_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const (
	benchBatch   = 100
	benchWorkers = 4
	benchSlots   = 25
)

type finished struct {
	n    atomic.Int64
	goal int64
	done chan struct{}
	once sync.Once
}

func (f *finished) Started(*runnel.Job) {}

func (f *finished) Finished(*runnel.Job, runnel.Outcome, time.Duration) {
	if f.n.Add(1) == f.goal {
		f.once.Do(func() { close(f.done) })
	}
}

func BenchmarkRun(b *testing.B) {
	modes := []struct {
		name string
		sync wal.SyncMode
		disk bool
	}{{"memory", 0, false}, {"off", wal.SyncOff, true}, {"periodic", wal.SyncPeriodic, true}, {"each", wal.SyncEach, true}}
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			s := hybrid.Memory()
			if m.disk {
				var err error
				if s, err = hybrid.Open(hybrid.Options{Dir: b.TempDir(), Sync: m.sync}); err != nil {
					b.Fatal(err)
				}
			}
			defer s.Close()
			runJobs(b, s)
		})
	}
}

func runJobs(b *testing.B, s *hybrid.Store) {
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	for i := 0; i < b.N; i += benchBatch {
		jobs := make([]*runnel.Job, 0, benchBatch)
		for range min(benchBatch, b.N-i) {
			j, _ := runnel.NewJob("q", "noop", map[string]int{"n": i})
			jobs = append(jobs, j)
		}
		if _, err := c.EnqueueJobs(ctx, jobs); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	f := &finished{goal: int64(b.N), done: make(chan struct{})}
	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range benchWorkers {
		w := runnel.NewWorker(s, ns, runnel.WorkerConfig{Queues: []string{"q"}, Concurrency: benchSlots, Observer: f,
			PollInterval: 50 * time.Millisecond})
		w.HandleFunc("noop", func(context.Context, *runnel.Job) error { return nil })
		wg.Go(func() { _ = w.Run(wctx) })
	}
	<-f.done
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
	stop()
	wg.Wait()
}

func BenchmarkCycle(b *testing.B) {
	for _, m := range []struct {
		name string
		sync wal.SyncMode
		disk bool
	}{{"memory", 0, false}, {"off", wal.SyncOff, true}, {"periodic", wal.SyncPeriodic, true}} {
		b.Run(m.name, func(b *testing.B) {
			s := hybrid.Memory()
			if m.disk {
				var err error
				if s, err = hybrid.Open(hybrid.Options{Dir: b.TempDir(), Sync: m.sync}); err != nil {
					b.Fatal(err)
				}
			}
			defer s.Close()
			cycle(b, s)
		})
	}
}

func cycle(b *testing.B, s *hybrid.Store) {
	ctx := context.Background()
	c := runnel.NewClient(s, ns)
	payload := map[string]string{"to": "user@example.com", "subject": "hello", "body": "a short message body"}
	for i := 0; i < b.N; i += benchBatch {
		jobs := make([]*runnel.Job, 0, benchBatch)
		for range min(benchBatch, b.N-i) {
			j, _ := runnel.NewJob("q", "noop", payload)
			jobs = append(jobs, j)
		}
		if _, err := c.EnqueueJobs(ctx, jobs); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	var left atomic.Int64
	left.Store(int64(b.N))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if left.Load() <= 0 {
				continue
			}
			now := time.Now()
			got, err := s.Claim(ctx, ns, runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: benchSlots, Lease: time.Minute, Now: now})
			if err != nil {
				b.Error(err)
				return
			}
			changes := make([]runnel.Change, len(got))
			for i, j := range got {
				changes[i] = runnel.Change{Job: runnel.Decide(j, runnel.Outcome{Kind: runnel.OutcomeDone}, now), From: runnel.StateRunning}
			}
			if len(changes) > 0 {
				if _, err := s.Apply(ctx, ns, changes, now); err != nil {
					b.Error(err)
					return
				}
			}
			left.Add(-int64(len(got)))
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
}
