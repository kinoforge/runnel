package hybrid

import (
	"sync"

	"github.com/kinoforge/runnel/store/hybrid/wal"
)

// Calls queued on a namespace run together under one hold of its lock, instead of passing the lock between goroutines.
type call struct {
	fn     func(*tx) error
	t      *tx
	err    error
	pos    wal.Pos
	logErr error
	turn   chan bool
}

type queue struct {
	mu      sync.Mutex
	calls   []*call
	leading bool
}

func (q *queue) join(c *call) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls = append(q.calls, c)
	if q.leading {
		return false
	}
	q.leading = true
	return true
}

func (q *queue) take() []*call {
	q.mu.Lock()
	defer q.mu.Unlock()
	b := q.calls
	q.calls = nil
	return b
}

func (q *queue) pass() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.calls) == 0 {
		q.leading = false
		return
	}
	q.calls[0].turn <- true
}

func (s *Store) run(sp *space, c *call) {
	if sp.queue.join(c) || <-c.turn {
		s.round(sp, c)
	}
}

func (s *Store) round(sp *space, self *call) {
	batch := sp.queue.take()
	defer func() {
		sp.queue.pass()
		for _, c := range batch {
			if c != self {
				c.turn <- false
			}
		}
	}()
	func() {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		for _, c := range batch {
			s.apply(sp, c)
		}
	}()
}

func (s *Store) apply(sp *space, c *call) {
	c.t = newTx(sp, s.log != nil)
	c.err = c.fn(c.t)
	if c.err != nil {
		if c.t.changed() {
			c.logErr = c.err
		}
		return
	}
	if c.t.changed() {
		if c.pos, c.logErr = s.log.Append(c.t.rec.b); c.logErr == nil {
			s.place(sp, c.t, c.pos)
		}
	}
}
