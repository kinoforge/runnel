package hybrid

import (
	"sync"

	"github.com/kinoforge/runnel/store/hybrid/wal"
)

// call is one store call on a namespace. Calls that arrive while another runs
// are queued and run together by one goroutine under one hold of the
// namespace lock, instead of passing the lock from goroutine to goroutine.
type call struct {
	fn     func(*tx) error
	t      *tx
	err    error
	pos    wal.Pos
	logErr error
	// turn receives true when the call is to lead the next round, or false
	// once another round ran it.
	turn chan bool
}

// queue holds the calls waiting for a namespace.
type queue struct {
	mu      sync.Mutex
	calls   []*call
	leading bool
}

// join queues c and reports whether its goroutine leads the next round.
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

// pass ends a round: the first call queued meanwhile leads the next one.
func (q *queue) pass() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.calls) == 0 {
		q.leading = false
		return
	}
	q.calls[0].turn <- true
}

// run waits for c's turn and returns once a round ran it.
func (s *Store) run(sp *space, c *call) {
	if sp.queue.join(c) || <-c.turn {
		s.round(sp, c)
	}
}

// round runs the queued calls of sp, which include self, then hands the lead
// on before releasing the calls it ran.
func (s *Store) round(sp *space, self *call) {
	batch := sp.queue.take()
	sp.mu.Lock()
	for _, c := range batch {
		s.apply(sp, c)
	}
	sp.mu.Unlock()
	sp.queue.pass()
	for _, c := range batch {
		if c != self {
			c.turn <- false
		}
	}
}

// apply runs c's function and logs the ops it made.
func (s *Store) apply(sp *space, c *call) {
	c.t = newTx(sp, s.log != nil)
	c.err = c.fn(c.t)
	if c.t.changed() {
		if c.pos, c.logErr = s.log.Append(c.t.rec.b); c.logErr == nil {
			s.place(sp, c.t, c.pos)
		}
	}
}
