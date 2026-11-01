package server

import (
	"context"
	"sync"

	"github.com/kinoforge/runnel"
)

type hub struct {
	mu sync.Mutex
	ch chan struct{}
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

func (s *Server) hub(ns string) *hub {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hubs[ns]
	if h != nil {
		return h
	}
	h = &hub{ch: make(chan struct{})}
	s.hubs[ns] = h
	if wk, ok := s.store.(runnel.Waker); ok {
		go func() {
			ch, err := wk.Subscribe(context.Background(), ns)
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
