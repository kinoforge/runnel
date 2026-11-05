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
// readGap is how far apart two bodies may lie and still be read at once.
const readGap = 4 << 10

func (s *Store) full(sp *space, h *job) (*runnel.Job, error) {
	b, err := sp.body(h, s.files)
	if err != nil {
		return nil, fmt.Errorf("hybrid: job %s: %w", h.id, err)
	}
	return sp.syms.full(sp.ns, h, b)
}

// rebuild makes whole jobs of drafts once the namespace lock is released,
// reading the bodies that were not cached. A body a snapshot moved meanwhile
// is looked up again under the lock.
func (s *Store) rebuild(ns string, ds []draft) ([]*runnel.Job, error) {
	if len(ds) == 0 {
		return nil, nil
	}
	s.fill(ds)
	out := make([]*runnel.Job, len(ds))
	for i := range ds {
		d := &ds[i]
		if d.body == nil {
			if err := s.fetch(ns, d); err != nil {
				return nil, fmt.Errorf("hybrid: job %s: %w", d.h.id, err)
			}
		}
		j, err := d.job(ns)
		if err != nil {
			return nil, fmt.Errorf("hybrid: job %s: %w", d.h.id, err)
		}
		out[i] = j
	}
	return out, nil
}

// fill reads the bodies drafts lack in disk order, one read for each run of
// bodies lying close together, as jobs enqueued together do. A failed read
// leaves the bodies for fetch.
func (s *Store) fill(ds []draft) {
	if s.files == nil {
		return
	}
	var todo []int
	for i := range ds {
		if ds[i].body == nil && ds[i].h.body.n > 0 {
			todo = append(todo, i)
		}
	}
	slices.SortFunc(todo, func(a, b int) int { return cmpRef(ds[a].h.body, ds[b].h.body) })
	for k := 0; k < len(todo); {
		first := ds[todo[k]].h.body
		end := first.off + int64(first.n)
		m := k + 1
		for ; m < len(todo); m++ {
			r := ds[todo[m]].h.body
			if r.file != first.file || r.off > end+readGap || r.off+int64(r.n)-first.off > chunkBytes {
				break
			}
			end = max(end, r.off+int64(r.n))
		}
		buf := make([]byte, end-first.off)
		if n, err := s.files.readAt(first.file, first.off, buf); err == nil && n == len(buf) {
			for _, i := range todo[k:m] {
				r := ds[i].h.body
				ds[i].body = buf[r.off-first.off : r.off-first.off+int64(r.n)]
			}
		}
		k = m
	}
}

func (s *Store) fetch(ns string, d *draft) error {
	if s.files != nil && d.h.body.n > 0 {
		if b, err := s.files.read(d.h.body); err == nil {
			d.body = b
			return nil
		}
	}
	return s.read(ns, func(sp *space) error {
		h := d.h
		h.body, _ = sp.bodyRef(h.id)
		b, err := sp.body(&h, s.files)
		d.body = b
		return err
	})
}

func (s *Store) Get(_ context.Context, ns string, ids []string) ([]*runnel.Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []draft
	err := s.read(ns, func(sp *space) error {
		seen := make(map[uid]bool, len(ids))
		for _, raw := range ids {
			id, ok := parseID(raw)
			h, found := sp.get(id)
			if !ok || !found || seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, sp.draft(&h))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.rebuild(ns, out)
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
	var out []draft
	next := ""
	err := s.read(ns, func(sp *space) error {
		f := sp.filter(q)
		trees := sp.byState[1:]
		if q.State != "" {
			st := stateCode(q.State)
			trees = sp.byState[st : st+1]
			if st == 0 {
				return nil
			}
		}
		if !f.ok {
			return nil
		}
		cur, hasCur := parseID(q.Cursor)
		if q.Cursor != "" && !hasCur {
			return fmt.Errorf("%w: bad cursor", runnel.ErrInvalid)
		}
		ids := sp.newest(trees, f, cur, hasCur, q.Limit)
		for _, id := range ids {
			h, _ := sp.get(id)
			out = append(out, sp.draft(&h))
		}
		if q.Limit > 0 && len(ids) == q.Limit {
			next = ids[len(ids)-1].String()
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	jobs, err := s.rebuild(ns, out)
	return jobs, next, err
}

// newest returns the ids of up to limit jobs in trees that match f, newest
// first, starting after cursor cur when given. No limit means all.
func (sp *space) newest(trees []*idTree, f filter, cur uid, hasCur bool, limit int) []uid {
	var ids []uid
	for _, t := range trees {
		n := 0
		visit := func(id uid) bool {
			if hasCur && id == cur {
				return true
			}
			if h, _ := sp.get(id); !f.match(&h) {
				return true
			}
			ids = append(ids, id)
			n++
			return limit <= 0 || n < limit
		}
		if hasCur {
			t.DescendLessOrEqual(cur, visit)
		} else {
			t.Descend(visit)
		}
	}
	if len(trees) > 1 {
		slices.SortFunc(ids, func(a, b uid) int { return cmpID(b, a) })
	}
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

func (s *Store) Due(_ context.Context, ns string, kind runnel.DueKind, now time.Time, limit int) ([]*runnel.Job, error) {
	at := ms(now)
	var out []draft
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
			h, _ := sp.get(k.id)
			out = append(out, sp.draft(&h))
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.rebuild(ns, out)
}

func (s *Store) Events(_ context.Context, ns string, after int64, limit int) ([]runnel.Event, error) {
	var out []runnel.Event
	err := s.read(ns, func(sp *space) error {
		i, _ := slices.BinarySearchFunc(sp.events, after+1, cmpEvent)
		end := min(len(sp.events), i+max(limit, 0))
		out = make([]runnel.Event, 0, end-i)
		for k := i; k < end; k++ {
			out = append(out, sp.event(&sp.events[k]))
		}
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
