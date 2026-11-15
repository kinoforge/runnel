package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
)

type meter struct {
	mu      sync.Mutex
	running map[string]int
	most    map[string]int
	started map[string]int
	gates   map[string]chan struct{}
}

func newMeter() *meter {
	return &meter{running: map[string]int{}, most: map[string]int{}, started: map[string]int{}, gates: map[string]chan struct{}{}}
}

func (m *meter) gate(key string) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.gates[key]
	if g == nil {
		g = make(chan struct{}, 1000)
		m.gates[key] = g
	}
	return g
}

func (m *meter) open(key string, n int) {
	g := m.gate(key)
	for range n {
		g <- struct{}{}
	}
}

func (m *meter) run(ctx context.Context, key string) error {
	m.mu.Lock()
	m.running[key]++
	m.started[key]++
	m.most[key] = max(m.most[key], m.running[key])
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.running[key]--
		m.mu.Unlock()
	}()
	select {
	case <-m.gate(key):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *meter) counts(key string) (started, most int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.started[key], m.most[key]
}

func (e *env) state(id string, want runnel.State) func() bool {
	return func() bool { return e.get(id).State == want }
}

func (e *env) count(queue string, want runnel.State) int64 {
	e.t.Helper()
	stats, err := e.c.Stats(e.ctx)
	if err != nil {
		e.t.Fatalf("stats: %v", err)
	}
	for _, s := range stats {
		if s.Queue == queue {
			return s.Counts[want]
		}
	}
	return 0
}

func (e *env) events(id string) []runnel.Event {
	e.t.Helper()
	var out []runnel.Event
	after := int64(0)
	for {
		page, err := e.c.Events(e.ctx, after, 500)
		if err != nil {
			e.t.Fatalf("events: %v", err)
		}
		for _, ev := range page {
			if ev.JobID == id {
				out = append(out, ev)
			}
		}
		if len(page) < 500 {
			return out
		}
		after = page[len(page)-1].Seq
	}
}

func (e *env) maintain(t *testing.T, cfg runnel.MaintainerConfig) func() {
	cfg.Namespaces = []string{e.ns}
	if cfg.Interval == 0 {
		cfg.Interval = 50 * time.Millisecond
	}
	m := runnel.NewMaintainer(e.s, cfg)
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := m.Run(ctx); err != nil {
			t.Errorf("maintainer: %v", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func quick() runnel.Option {
	return runnel.WithBackoff(runnel.Backoff{Base: runnel.Duration(10 * time.Millisecond), Jitter: -1})
}
