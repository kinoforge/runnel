package sqlstore

import (
	"context"
	"time"
)

const (
	notifyBatchDelay = 2 * time.Millisecond
	notifyTimeout    = 5 * time.Second
)

func (s *Store) notify(ns string, wake bool, ends []string) {
	if s.d.Notify == nil || (!wake && len(ends) == 0) {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if wake {
		s.wakes[ns] = true
	}
	if len(ends) > 0 {
		s.ends[ns] = append(s.ends[ns], ends...)
	}
	if s.kick == nil {
		s.kick, s.stopped = make(chan struct{}, 1), make(chan struct{})
		go s.flushLoop(s.kick, s.stopped)
	}
	kick := s.kick
	s.mu.Unlock()
	select {
	case kick <- struct{}{}:
	default:
	}
}

func (s *Store) flushLoop(kick chan struct{}, stopped chan struct{}) {
	defer close(stopped)
	for range kick {
		time.Sleep(notifyBatchDelay)
		s.flush()
	}
	s.flush()
}

func (s *Store) flush() {
	s.mu.Lock()
	wakes, ends := s.wakes, s.ends
	s.wakes, s.ends = map[string]bool{}, map[string][]string{}
	s.mu.Unlock()
	if len(wakes) == 0 && len(ends) == 0 {
		return
	}
	list := make([]string, 0, len(wakes))
	for ns := range wakes {
		list = append(list, ns)
	}
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()
	_ = s.d.Notify(ctx, s.db, list, ends)
}

// Close sends pending notifications and closes the pool.
func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true
	kick, stopped := s.kick, s.stopped
	s.kick = nil
	s.mu.Unlock()
	if kick != nil {
		close(kick)
		<-stopped
	}
	return s.db.Close()
}
