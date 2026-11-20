package server

import (
	"context"
	"sync"

	"github.com/kinoforge/runnel"
)

type hub struct {
	mu     sync.Mutex
	ch     chan struct{}
	cancel context.CancelFunc
}

func (h *hub) wait() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ch
}

func (h *hub) fire() {
	h.mu.Lock()
	close(h.ch)
	h.ch = make(chan struct{})
	h.mu.Unlock()
}

func (h *hub) stop() {
	if h.cancel != nil {
		h.cancel()
	}
}

func (s *Server) hub(ns string) *hub {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hubs[ns]
	if h != nil {
		return h
	}
	h = &hub{ch: make(chan struct{})}
	if s.closed.Load() {
		return h
	}
	s.hubs[ns] = h
	if wk, ok := s.store.(runnel.Waker); ok {
		hctx, cancel := context.WithCancel(context.Background())
		h.cancel = cancel
		go func() {
			defer func() {
				cancel()
				s.mu.Lock()
				if s.hubs[ns] == h {
					delete(s.hubs, ns)
				}
				s.mu.Unlock()
			}()
			ch, err := wk.Subscribe(hctx, ns)
			if err != nil {
				return
			}
			for range ch {
				h.fire()
			}
		}()
	}
	return h
}
