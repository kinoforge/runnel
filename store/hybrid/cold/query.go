package cold

import (
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// Filter limits a listing to jobs with these names; an empty name matches any.
type Filter struct{ Queue, Kind, Group string }

func (f Filter) match(q, k, t string) bool {
	return (f.Queue == "" || f.Queue == q) && (f.Kind == "" || f.Kind == k) && (f.Group == "" || f.Group == t)
}

// List returns the ids of up to limit jobs in state that match f, newest
// first, after cursor when it is given. No limit means all.
func (s *Store) List(ns string, state uint8, f Filter, cursor *ID, limit int) ([]ID, error) {
	lo := append(prefix(kState, ns, 1), state)
	hi := append(prefix(kState, ns, 1), state+1)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	var ok bool
	if cursor != nil {
		ok = it.SeekLT(stateKey(ns, state, *cursor))
	} else {
		ok = it.Last()
	}
	var out []ID
	for ; ok && (limit <= 0 || len(out) < limit); ok = it.Prev() {
		q, k, t, _, good := readNames(it.Value())
		if !good {
			return nil, errors.Join(errBadKey, it.Close())
		}
		if f.match(q, k, t) {
			out = append(out, idOf(it.Key()))
		}
	}
	return out, errors.Join(it.Error(), it.Close())
}

// MaxID returns the largest job id ever stored for ns that is still there.
func (s *Store) MaxID(ns string) (ID, bool, error) {
	lo, hi := span(kJob, ns)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return ID{}, false, err
	}
	var id ID
	ok := it.Last()
	if ok {
		id = idOf(it.Key())
	}
	return id, ok, errors.Join(it.Error(), it.Close())
}

// Oldest returns the ids of up to limit jobs finished before before, oldest
// first, that keep accepts.
func (s *Store) Oldest(ns string, before int64, limit int, keep func(state uint8, queue string) bool) ([]ID, error) {
	lo := prefix(kTime, ns, 8)
	hi := binary.BigEndian.AppendUint64(prefix(kTime, ns, 8), uint64(before))
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	var out []ID
	for ok := it.First(); ok && len(out) < limit; ok = it.Next() {
		st, q, good := decodeWhen(it.Value())
		if !good {
			return nil, errors.Join(errBadKey, it.Close())
		}
		if keep(st, q) {
			out = append(out, idOf(it.Key()))
		}
	}
	return out, errors.Join(it.Error(), it.Close())
}
