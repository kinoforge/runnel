package hybrid

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
)

// full returns the whole job h stands for, reading its body.
func (s *Store) full(sp *space, h *job) (*runnel.Job, error) {
	b, err := sp.body(h, s.files)
	if err != nil {
		return nil, fmt.Errorf("hybrid: job %s: %w", h.id, err)
	}
	return sp.syms.full(sp.ns, h, b)
}

func (s *Store) Get(_ context.Context, ns string, ids []string) ([]*runnel.Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []*runnel.Job
	err := s.read(ns, func(sp *space) error {
		seen := make(map[uid]bool, len(ids))
		for _, raw := range ids {
			id, ok := parseID(raw)
			h, found := sp.get(id)
			if !ok || !found || seen[id] {
				continue
			}
			seen[id] = true
			j, err := s.full(sp, &h)
			if err != nil {
				return err
			}
			out = append(out, j)
		}
		return nil
	})
	return out, err
}

// filter holds the symbols a query matches; ok is false when a name it asks for
// was never used, so nothing can match.
type filter struct {
	queue, kind, tenant sym
	ok                  bool
}

func (sp *space) filter(q runnel.Query) filter {
	qs, ok1 := sp.syms.find(q.Queue)
	ks, ok2 := sp.syms.find(q.Kind)
	ts, ok3 := sp.syms.find(q.Tenant)
	return filter{queue: qs, kind: ks, tenant: ts, ok: ok1 && ok2 && ok3}
}

func (f filter) match(h *job) bool {
	return (f.queue == 0 || h.queue == f.queue) && (f.kind == 0 || h.kind == f.kind) && (f.tenant == 0 || h.tenant == f.tenant)
}

func (s *Store) List(_ context.Context, ns string, q runnel.Query) ([]*runnel.Job, string, error) {
	var out []*runnel.Job
	next := ""
	err := s.read(ns, func(sp *space) error {
		f := sp.filter(q)
		t := sp.all
		if q.State != "" {
			t = sp.byState[stateCode(q.State)]
		}
		if !f.ok || t == sp.byState[0] {
			return nil
		}
		cur, hasCur := parseID(q.Cursor)
		if q.Cursor != "" && !hasCur {
			return fmt.Errorf("%w: bad cursor", runnel.ErrInvalid)
		}
		var err error
		visit := func(id uid) bool {
			h, _ := sp.get(id)
			if (hasCur && id == cur) || !f.match(&h) {
				return true
			}
			var j *runnel.Job
			if j, err = s.full(sp, &h); err != nil {
				return false
			}
			out = append(out, j)
			if len(out) == q.Limit {
				next = j.ID
				return false
			}
			return true
		}
		if hasCur {
			t.DescendLessOrEqual(cur, visit)
		} else {
			t.Descend(visit)
		}
		return err
	})
	return out, next, err
}

func (s *Store) Due(_ context.Context, ns string, kind runnel.DueKind, now time.Time, limit int) ([]*runnel.Job, error) {
	at := ms(now)
	var out []*runnel.Job
	err := s.read(ns, func(sp *space) error {
		var t *timeTree
		var due func(int64) bool
		switch kind {
		case runnel.DueLease:
			t, due = sp.leases, func(v int64) bool { return v < at }
		case runnel.DueStart:
			t, due = sp.startBy, func(v int64) bool { return v <= at }
		default:
			return fmt.Errorf("runnel: unknown due kind %d", kind)
		}
		var err error
		t.Ascend(func(k timeKey) bool {
			if !due(k.at) || len(out) >= limit {
				return false
			}
			h, _ := sp.get(k.id)
			var j *runnel.Job
			if j, err = s.full(sp, &h); err != nil {
				return false
			}
			out = append(out, j)
			return true
		})
		return err
	})
	return out, err
}

func (s *Store) Events(_ context.Context, ns string, after int64, limit int) ([]runnel.Event, error) {
	var out []runnel.Event
	err := s.read(ns, func(sp *space) error {
		i, _ := slices.BinarySearchFunc(sp.events, after+1, cmpEvent)
		end := min(len(sp.events), i+max(limit, 0))
		out = slices.Clone(sp.events[i:end])
		return nil
	})
	return out, err
}

func (s *Store) Stats(_ context.Context, ns string) ([]runnel.QueueStats, error) {
	var out []runnel.QueueStats
	err := s.read(ns, func(sp *space) error {
		byName := map[string]*[stCount]int64{}
		for q, c := range sp.counts {
			byName[sp.syms.name(q)] = c
		}
		for _, name := range slices.Sorted(maps.Keys(byName)) {
			counts := map[runnel.State]int64{}
			for st, n := range byName[name] {
				if n > 0 {
					counts[stateNames[st]] = n
				}
			}
			if len(counts) > 0 {
				out = append(out, runnel.QueueStats{Queue: name, Counts: counts})
			}
		}
		return nil
	})
	return out, err
}

func (s *Store) Cursor(_ context.Context, ns, name string) (int64, error) {
	var v int64
	err := s.read(ns, func(sp *space) error {
		v = sp.cursors[name]
		return nil
	})
	return v, err
}

func (s *Store) Configs(_ context.Context, ns string, kind runnel.ConfigKind) (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.read(ns, func(sp *space) error {
		for name, v := range sp.configs[kind] {
			out[name] = slices.Clone(v)
		}
		return nil
	})
	return out, err
}

func (s *Store) PutConfig(_ context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	return s.write(ns, func(t *tx) error {
		t.putConfig(kind, name, slices.Clone(value))
		return nil
	})
}

func (s *Store) DeleteConfig(_ context.Context, ns string, kind runnel.ConfigKind, name string) error {
	return s.write(ns, func(t *tx) error {
		if _, ok := t.sp.configs[kind][name]; ok {
			t.delConfig(kind, name)
		}
		return nil
	})
}
