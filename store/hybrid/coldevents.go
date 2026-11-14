package hybrid

import (
	"math"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

func (s *Store) retireEvents(sp *space, cutoff int64) error {
	for {
		evs := sp.retirableEvents(cutoff, s.opt.RetireBatch)
		if len(evs) == 0 {
			return nil
		}
		if err := s.cold.PutEvents(sp.ns, evs); err != nil {
			return err
		}
		last := evs[len(evs)-1].Seq
		sp.mu.Lock()
		sp.dropEvents(last)
		sp.coldSeq = last
		sp.mu.Unlock()
		if len(evs) < s.opt.RetireBatch {
			return nil
		}
	}
}

func (sp *space) retirableEvents(cutoff int64, n int) []cold.Event {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var out []cold.Event
	for i := 0; i < len(sp.events) && i < n && sp.events[i].at < cutoff; i++ {
		e := &sp.events[i]
		var enc enc
		enc.int(e.at)
		enc.bytes(e.id[:])
		enc.str(sp.syms.name(e.queue))
		enc.str(sp.syms.name(e.kind))
		enc.byte(e.state)
		out = append(out, cold.Event{Seq: e.seq, Data: enc.b})
	}
	return out
}

func coldEvent(ns string, e cold.Event) (runnel.Event, error) {
	d := newDec(e.Data)
	at := d.int()
	id := readID(d)
	out := runnel.Event{Seq: e.Seq, Namespace: ns, JobID: id.String(), Queue: d.str(), Kind: d.str(), At: fromMs(at)}
	if st := d.byte(); st < stCount {
		out.State = stateNames[st]
	}
	return out, d.err
}

func (s *Store) olderEvents(ns string, after, head int64, limit int, mem []runnel.Event) ([]runnel.Event, error) {
	if s.cold == nil || limit <= 0 || after+1 >= head {
		return mem, nil
	}
	evs, err := s.cold.Events(ns, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]runnel.Event, 0, len(evs)+len(mem))
	for _, e := range evs {
		if e.Seq >= head {
			break
		}
		ev, err := coldEvent(ns, e)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	out = append(out, mem...)
	return out[:min(len(out), limit)], nil
}

func (s *Store) pruneColdEvents(ns string, before, upTo int64, limit int) (int, bool, error) {
	if s.cold == nil {
		return 0, true, nil
	}
	stopped := false
	last, n, err := s.cold.DropOldEvents(ns, limit, func(seq int64, data []byte) bool {
		at := newDec(data).int()
		keep := at < before && (upTo <= 0 || seq <= upTo)
		stopped = stopped || !keep
		return keep
	})
	if err != nil || n == 0 {
		return 0, !stopped && n < limit, err
	}
	err = s.write(ns, func(t *tx) error {
		t.dropEvents(last)
		return nil
	})
	return n, !stopped && n < limit, err
}

func (sp *space) eventHead() int64 {
	if len(sp.events) == 0 {
		return math.MaxInt64
	}
	return sp.events[0].seq
}
