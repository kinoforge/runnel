package hybrid

import (
	"context"
	"sync"
)

const endedBuffer = 256

// Sends never block: a full channel drops the signal.
type hub struct {
	mu   sync.Mutex
	wake map[string]map[chan struct{}]struct{}
	ends map[string]map[chan string]struct{}
}

func (h *hub) subscribe(ctx context.Context, ns string) <-chan struct{} {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.wake == nil {
		h.wake = map[string]map[chan struct{}]struct{}{}
	}
	add(h.wake, ns, ch)
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.mu.Lock()
		delete(h.wake[ns], ch)
		h.mu.Unlock()
	}()
	return ch
}

func (h *hub) ended(ctx context.Context, ns string) <-chan string {
	ch := make(chan string, endedBuffer)
	h.mu.Lock()
	if h.ends == nil {
		h.ends = map[string]map[chan string]struct{}{}
	}
	add(h.ends, ns, ch)
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.mu.Lock()
		delete(h.ends[ns], ch)
		h.mu.Unlock()
	}()
	return ch
}

func add[T any](m map[string]map[chan T]struct{}, ns string, ch chan T) {
	if m[ns] == nil {
		m[ns] = map[chan T]struct{}{}
	}
	m[ns][ch] = struct{}{}
}

func (h *hub) notify(ns string, wake bool, ends []uid) {
	if !wake && len(ends) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if wake {
		for ch := range h.wake[ns] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	if len(h.ends[ns]) == 0 {
		return
	}
	for _, u := range ends {
		id := u.String()
		for ch := range h.ends[ns] {
			select {
			case ch <- id:
			default:
			}
		}
	}
}
