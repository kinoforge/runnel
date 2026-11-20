package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	maxBenchBatch  = 1000
	benchPoll      = 50 * time.Millisecond
	benchShutdown  = 5 * time.Second
	latencyRound   = 10 * time.Microsecond
	cleanupHorizon = 24 * time.Hour
	cleanupBatch   = 5000
)

type counter struct {
	n    atomic.Int64
	goal int64
	done chan struct{}
	once sync.Once
}

func (c *counter) Started(*runnel.Job) {}

func (c *counter) Finished(_ *runnel.Job, _ runnel.Outcome, _ time.Duration) {
	if c.n.Add(1) == c.goal {
		c.once.Do(func() { close(c.done) })
	}
}

func bench(ctx context.Context, args []string) error {
	c := newCmd("bench")
	total := c.fs.Int("jobs", 20000, "jobs to enqueue and run")
	workers := c.fs.Int("workers", 4, "workers")
	concurrency := c.fs.Int("concurrency", 25, "slots per worker")
	batch := c.fs.Int("batch", 100, "jobs per enqueue call")
	groups := c.fs.Int("groups", 0, "spread jobs over this many groups")
	groupMaxRunning := c.fs.Int("group-max-running", 0, "queue group_max_running")
	samples := c.fs.Int("latency", 200, "single jobs timed from enqueue to result")
	size := c.fs.Int("payload", 0, "payload bytes per job")
	if _, err := c.parse(args, 0); err != nil {
		return err
	}
	if *total <= 0 || *workers <= 0 || *concurrency <= 0 || *batch <= 0 || *batch > maxBenchBatch || *size < 0 {
		return fmt.Errorf("jobs, workers and concurrency must be positive, batch 1 to %d and payload not negative", maxBenchBatch)
	}
	var payload any
	if *size > 0 {
		payload = strings.Repeat("x", *size)
	}
	cfg, err := c.settings()
	if err != nil {
		return err
	}
	s, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	ns := "bench-" + strconv.FormatInt(time.Now().UnixNano()%1e12, 36)
	cl := runnel.NewClient(s, ns)
	defer cleanup(context.WithoutCancel(ctx), s, ns)
	if err := cl.SetQueue(ctx, runnel.QueueConfig{Name: "bench", GroupMaxRunning: *groupMaxRunning}); err != nil {
		return err
	}

	began := time.Now()
	for i := 0; i < *total; i += *batch {
		jobs := make([]*runnel.Job, 0, *batch)
		for k := i; k < min(i+*batch, *total); k++ {
			var opts []runnel.Option
			if *groups > 0 {
				opts = append(opts, runnel.Group("t"+strconv.Itoa(rand.IntN(*groups))))
			}
			j, _ := runnel.NewJob("bench", "noop", payload, opts...)
			jobs = append(jobs, j)
		}
		if _, err := cl.EnqueueJobs(ctx, jobs); err != nil {
			return err
		}
	}
	enqueued := time.Since(began)

	count := &counter{goal: int64(*total), done: make(chan struct{})}
	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	pool := make([]*runnel.Worker, *workers)
	began = time.Now()
	for i := range pool {
		w := runnel.NewWorker(s, ns, runnel.WorkerConfig{Queues: []string{"bench"}, Concurrency: *concurrency,
			PollInterval: benchPoll, Observer: count, ShutdownTimeout: benchShutdown})
		w.HandleFunc("noop", func(context.Context, *runnel.Job) error { return nil })
		pool[i] = w
		wg.Go(func() {
			_ = w.Run(wctx)
		})
	}
	select {
	case <-count.done:
	case <-ctx.Done():
		stop()
		wg.Wait()
		return ctx.Err()
	}
	ran := time.Since(began)

	lat := make([]time.Duration, 0, *samples)
	for range *samples {
		t0 := time.Now()
		j, _ := runnel.NewJob("bench", "noop", nil)
		if _, err := pool[0].Do(ctx, j); err != nil {
			stop()
			wg.Wait()
			return err
		}
		lat = append(lat, time.Since(t0))
	}
	lat2 := make([]time.Duration, 0, *samples)
	for range *samples {
		t0 := time.Now()
		if _, err := cl.EnqueueAndWait(ctx, "bench", "noop", nil); err != nil {
			stop()
			wg.Wait()
			return err
		}
		lat2 = append(lat2, time.Since(t0))
	}
	stop()
	wg.Wait()

	tw := table()
	fmt.Fprintf(tw, "store\t%s\n", redact(cfg.Store))
	fmt.Fprintf(tw, "jobs\t%d (batch %d, payload %d bytes, %d groups, group max running %d)\n", *total, *batch, *size, *groups, *groupMaxRunning)
	fmt.Fprintf(tw, "workers\t%d x %d slots\n", *workers, *concurrency)
	fmt.Fprintf(tw, "enqueue\t%.0f jobs/s\n", float64(*total)/enqueued.Seconds())
	fmt.Fprintf(tw, "run\t%.0f jobs/s (claim, handle, finish)\n", float64(*total)/ran.Seconds())
	if *samples > 0 {
		fmt.Fprintf(tw, "direct start\tp50 %s  p99 %s\n", pct(lat, 50), pct(lat, 99))
		fmt.Fprintf(tw, "enqueue and wait\tp50 %s  p99 %s\n", pct(lat2, 50), pct(lat2, 99))
	}
	return tw.Flush()
}

func pct(d []time.Duration, p int) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[min(len(s)-1, len(s)*p/100)].Round(latencyRound)
}

func redact(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.User != nil {
		return u.Redacted()
	}
	if i := strings.LastIndexByte(raw, '@'); i >= 0 {
		if k := strings.LastIndexByte(raw[:i], '/'); k >= 0 {
			return raw[:k+1] + "***" + raw[i:]
		}
		return "***" + raw[i:]
	}
	return raw
}

func cleanup(ctx context.Context, s runnel.Store, ns string) {
	far := time.Now().Add(cleanupHorizon)
	final := []runnel.State{runnel.StateCompleted, runnel.StateDead, runnel.StateCancelled, runnel.StateExpired}
	for {
		n, err := s.Prune(ctx, ns, runnel.PruneRequest{States: final, Before: far, Limit: cleanupBatch})
		if err != nil || n == 0 {
			break
		}
	}
	for {
		n, err := s.PruneEvents(ctx, ns, runnel.EventPruneRequest{Before: far, Limit: cleanupBatch})
		if err != nil || n == 0 {
			break
		}
	}
	_ = s.DeleteConfig(ctx, ns, runnel.ConfigQueue, "bench")
}
