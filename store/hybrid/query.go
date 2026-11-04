package hybrid

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
)

func (s *Store) Get(_ context.Context, ns string, ids []string) ([]*runnel.Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []*runnel.Job
	err := s.read(ns, func(sp *space) error {
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if j := sp.jobs[id]; j != nil && !seen[id] {
				seen[id] = true
				out = append(out, j.Clone())
			}
		}
		return nil
	})
	return out, err
}

func (s *Store) List(_ context.Context, ns string, q runnel.Query) ([]*runnel.Job, string, error) {
	var out []*runnel.Job
	next := ""
	err := s.read(ns, func(sp *space) error {
		t := sp.all
		if q.State != "" {
			if t = sp.byState[q.State]; t == nil {
				return nil
			}
		}
		visit := func(id string) bool {
			if id == q.Cursor {
				return true
			}
			j := sp.jobs[id]
			if (q.Queue != "" && j.Queue != q.Queue) || (q.Kind != "" && j.Kind != q.Kind) || (q.Tenant != "" && j.Tenant != q.Tenant) {
				return true
			}
			out = append(out, j.Clone())
			if len(out) == q.Limit {
				next = id
				return false
			}
			return true
		}
		if q.Cursor == "" {
			t.Descend(visit)
		} else {
			t.DescendLessOrEqual(q.Cursor, visit)
		}
		return nil
	})
	return out, next, err
}

func (s *Store) Due(_ context.Context, ns string, kind runnel.DueKind, now time.Time, limit int) ([]*runnel.Job, error) {
	at := runnel.Millis(now)
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
		t.Ascend(func(k timeKey) bool {
			if !due(k.at) || len(out) >= limit {
				return false
			}
			out = append(out, sp.jobs[k.id].Clone())
			return true
		})
		return nil
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
		for _, q := range slices.Sorted(maps.Keys(sp.counts)) {
			counts := map[runnel.State]int64{}
			for st, n := range sp.counts[q] {
				if n > 0 {
					counts[st] = n
				}
			}
			if len(counts) > 0 {
				out = append(out, runnel.QueueStats{Queue: q, Counts: counts})
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
