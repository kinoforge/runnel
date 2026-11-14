package cold

import (
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

const (
	kEvent byte = 'e'
	kLast  byte = 'l'
)

// Event is an event as the cold store keeps it; the caller encodes Data.
type Event struct {
	Seq  int64
	Data []byte
}

func eventKey(ns string, seq int64) []byte {
	return binary.BigEndian.AppendUint64(prefix(kEvent, ns, 8), uint64(seq))
}

// PutEvents adds events of ns, which come after every event stored for it.
func (s *Store) PutEvents(ns string, evs []Event) error {
	if len(evs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	for _, e := range evs {
		if err := b.Set(eventKey(ns, e.Seq), e.Data, nil); err != nil {
			return err
		}
	}
	if err := b.Set(prefix(kLast, ns, 0), encodeInt(evs[len(evs)-1].Seq), nil); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// Events returns up to limit events of ns after sequence number after.
func (s *Store) Events(ns string, after int64, limit int) ([]Event, error) {
	it, err := s.eventIter(ns)
	if err != nil {
		return nil, err
	}
	var out []Event
	for ok := it.SeekGE(eventKey(ns, after+1)); ok && len(out) < limit; ok = it.Next() {
		out = append(out, Event{Seq: seqOf(it.Key()), Data: append([]byte(nil), it.Value()...)})
	}
	return out, errors.Join(it.Error(), it.Close())
}

// DropOldEvents deletes the oldest events of ns, up to limit of them, as long
// as keep accepts them, and returns the sequence number of the last deleted,
// or zero.
func (s *Store) DropOldEvents(ns string, limit int, keep func(seq int64, data []byte) bool) (int64, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, err := s.eventIter(ns)
	if err != nil {
		return 0, 0, err
	}
	var last int64
	n := 0
	for ok := it.First(); ok && n < limit && keep(seqOf(it.Key()), it.Value()); ok = it.Next() {
		last = seqOf(it.Key())
		n++
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil || n == 0 {
		return 0, 0, err
	}
	return last, n, s.dropEvents(ns, last)
}

// DropEvents deletes the events of ns up to and including seq.
func (s *Store) DropEvents(ns string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropEvents(ns, seq)
}

func (s *Store) dropEvents(ns string, seq int64) error {
	return s.db.DeleteRange(prefix(kEvent, ns, 0), eventKey(ns, seq+1), pebble.NoSync)
}

// LastEvents returns, by namespace, the newest sequence number moved here.
func (s *Store) LastEvents() (map[string]int64, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{kLast}, UpperBound: []byte{kLast + 1}})
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		out[string(k[1:len(k)-1])] = decodeInt(it.Value())
	}
	return out, errors.Join(it.Error(), it.Close())
}

func (s *Store) eventIter(ns string) (*pebble.Iterator, error) {
	lo, hi := span(kEvent, ns)
	return s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
}

func seqOf(k []byte) int64 { return int64(binary.BigEndian.Uint64(k[len(k)-8:])) }
