package cold

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// Waiting jobs are kept in two orders per queue. Ready ones in claim order:
// higher priority, then earlier run time, then id. Future ones in run time
// order, until Wake moves those due to the ready order. A deadline entry per
// job with a start deadline lets the caller rebuild its deadline index.

const signBit = 1 << 63

// Next names a waiting job by its place in claim order.
type Next struct {
	Prio  int32
	RunAt int64
	ID    ID
}

// Head is the front of a queue's waiting jobs: the first ready one, and the
// run time of the first future one.
type Head struct {
	Ready    Next
	HasReady bool
	Due      int64
	HasDue   bool
}

// Empty reports whether the queue has no waiting job here.
func (h Head) Empty() bool { return !h.HasReady && !h.HasDue }

func queuePrefix(kind byte, ns, queue string) []byte {
	return appendStr(prefix(kind, ns, 1+len(queue)+28), queue)
}

func queueSpan(kind byte, ns, queue string) ([]byte, []byte) {
	lo := queuePrefix(kind, ns, queue)
	return lo, after(lo)
}

// after returns the first key past every key starting with p.
func after(p []byte) []byte {
	b := bytes.Clone(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return b[:i+1]
		}
	}
	return nil
}

func readyKey(ns, queue string, n Next) []byte {
	b := queuePrefix(kReady, ns, queue)
	b = binary.BigEndian.AppendUint32(b, ^(uint32(n.Prio) ^ 1<<31))
	b = binary.BigEndian.AppendUint64(b, uint64(n.RunAt)^signBit)
	return append(b, n.ID[:]...)
}

func futureKey(ns, queue string, at int64, id ID) []byte {
	b := binary.BigEndian.AppendUint64(queuePrefix(kFuture, ns, queue), uint64(at)^signBit)
	return append(b, id[:]...)
}

// whenKey orders the jobs of ns by the time at, for the start and deadline
// indexes.
func whenKey(kind byte, ns string, at int64, id ID) []byte {
	return append(binary.BigEndian.AppendUint64(prefix(kind, ns, 24), uint64(at)), id[:]...)
}

func orderKey(j *Job) []byte {
	if j.Ready {
		return readyKey(j.NS, j.Queue, Next{Prio: j.Prio, RunAt: j.RunAt, ID: j.ID})
	}
	return futureKey(j.NS, j.Queue, j.RunAt, j.ID)
}

func decodeReady(k []byte) Next {
	t := k[len(k)-28:]
	return Next{Prio: int32(^binary.BigEndian.Uint32(t) ^ 1<<31), RunAt: int64(binary.BigEndian.Uint64(t[4:]) ^ signBit), ID: idOf(k)}
}

func futureAt(k []byte) int64 { return int64(binary.BigEndian.Uint64(k[len(k)-24:]) ^ signBit) }

// Front returns the head of queue's waiting jobs.
func (s *Store) Front(ns, queue string) (Head, error) {
	var h Head
	err := s.scan(kReady, ns, queue, func(it *pebble.Iterator) {
		if it.First() {
			h.Ready, h.HasReady = decodeReady(it.Key()), true
		}
	})
	if err != nil {
		return h, err
	}
	err = s.scan(kFuture, ns, queue, func(it *pebble.Iterator) {
		if it.First() {
			h.Due, h.HasDue = futureAt(it.Key()), true
		}
	})
	return h, err
}

// Take returns the first ready jobs of queue, up to limit of them or until
// they hold maxBytes, which stay stored until the caller deletes them, and the
// head that follows them.
func (s *Store) Take(ns, queue string, limit, maxBytes int) ([]Job, Head, error) {
	var jobs []Job
	var h Head
	var bad error
	size := 0
	err := s.scan(kReady, ns, queue, func(it *pebble.Iterator) {
		for ok := it.First(); ok; ok = it.Next() {
			if len(jobs) == limit || (len(jobs) > 0 && size >= maxBytes) {
				h.Ready, h.HasReady = decodeReady(it.Key()), true
				return
			}
			j, err := decodeJob(ns, idOf(it.Key()), it.Value(), true)
			if err != nil {
				bad = err
				return
			}
			jobs = append(jobs, j)
			size += len(it.Value())
		}
	})
	if err = errors.Join(err, bad); err != nil {
		return nil, h, err
	}
	front, err := s.Front(ns, queue)
	h.Due, h.HasDue = front.Due, front.HasDue
	return jobs, h, err
}

// Wake moves up to limit future jobs of queue due at now to the ready order
// and returns how many it moved.
func (s *Store) Wake(ns, queue string, now int64, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []Job
	var bad error
	err := s.scan(kFuture, ns, queue, func(it *pebble.Iterator) {
		for ok := it.First(); ok && len(due) < limit && futureAt(it.Key()) <= now; ok = it.Next() {
			j, err := decodeJob(ns, idOf(it.Key()), it.Value(), true)
			if err != nil {
				bad = err
				return
			}
			due = append(due, j)
		}
	})
	if err = errors.Join(err, bad); err != nil || len(due) == 0 {
		return 0, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	for i := range due {
		j := &due[i]
		old := orderKey(j)
		j.Ready = true
		rec := encodeJob(j)
		if err := errors.Join(b.Delete(old, nil), b.Set(orderKey(j), rec, nil), b.Set(jobKey(ns, j.ID), rec, nil)); err != nil {
			return 0, err
		}
	}
	return len(due), b.Commit(pebble.NoSync)
}

// Starts calls fn with the start deadline of every waiting job that has one.
func (s *Store) Starts(fn func(ns string, at int64, id ID)) error { return s.times(kStart, fn) }

// Deadlines calls fn with the deadline of every waiting job that has one.
func (s *Store) Deadlines(fn func(ns string, at int64, id ID)) error { return s.times(kDeadline, fn) }

func (s *Store) times(kind byte, fn func(ns string, at int64, id ID)) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{kind}, UpperBound: []byte{kind + 1}})
	if err != nil {
		return err
	}
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		i := bytes.IndexByte(k, 0)
		if i < 1 || len(k) != i+1+24 {
			return errors.Join(errBadKey, it.Close())
		}
		fn(string(k[1:i]), int64(binary.BigEndian.Uint64(k[i+1:])), idOf(k))
	}
	return errors.Join(it.Error(), it.Close())
}

func (s *Store) scan(kind byte, ns, queue string, fn func(*pebble.Iterator)) error {
	lo, hi := queueSpan(kind, ns, queue)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	fn(it)
	return errors.Join(it.Error(), it.Close())
}
