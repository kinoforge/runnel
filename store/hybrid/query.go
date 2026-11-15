package hybrid

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/kinoforge/runnel"
)

const readGap = 4 << 10

func (s *Store) full(sp *space, h *job) (*runnel.Job, error) {
	b, err := sp.body(h, s.files)
	if err != nil {
		return nil, fmt.Errorf("hybrid: job %s: %w", h.id, err)
	}
	return sp.syms.full(sp.ns, h, b)
}

// A body a snapshot moved meanwhile is looked up again under the lock.
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
	var want []uid
	var ds []draft
	err := s.read(ns, func(sp *space) error {
		seen := make(map[uid]bool, len(ids))
		for _, raw := range ids {
			id, ok := parseID(raw)
			if !ok || seen[id] {
				continue
			}
			seen[id] = true
			want = append(want, id)
			if h, found := sp.get(id); found {
				ds = append(ds, sp.draft(&h))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.gather(ns, want, ds)
}

type filter struct {
	queue, kind, group sym
	ok                 bool
}

func (sp *space) filter(q runnel.Query) filter {
	qs, ok1 := sp.syms.find(q.Queue)
	ks, ok2 := sp.syms.find(q.Kind)
	ts, ok3 := sp.syms.find(q.Group)
	return filter{queue: qs, kind: ks, group: ts, ok: ok1 && ok2 && ok3}
}

func (f filter) match(h *job) bool {
	return (f.queue == 0 || h.queue == f.queue) && (f.kind == 0 || h.kind == f.kind) && (f.group == 0 || h.group == f.group)
}

func (s *Store) List(_ context.Context, ns string, q runnel.Query) ([]*runnel.Job, string, error) {
	st := stateCode(q.State)
	if q.State != "" && st == 0 {
		return nil, "", nil
	}
	cur, hasCur := parseID(q.Cursor)
	if q.Cursor != "" && !hasCur {
		return nil, "", fmt.Errorf("%w: bad cursor", runnel.ErrInvalid)
	}
	var ram []uid
	var ds []draft
	err := s.read(ns, func(sp *space) error {
		f := sp.filter(q)
		if !f.ok {
			return nil
		}
		trees := sp.byState[1:]
		if st != 0 {
			trees = sp.byState[st : st+1]
		}
		ram = sp.newest(trees, f, cur, hasCur, q.Limit)
		for _, id := range ram {
			h, _ := sp.get(id)
			ds = append(ds, sp.draft(&h))
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	ids, err := s.coldNewest(ns, st, q, cur, hasCur, ram)
	if err != nil {
		return nil, "", err
	}
	jobs, err := s.gather(ns, ids, ds)
	next := ""
	if q.Limit > 0 && len(ids) == q.Limit {
		next = ids[len(ids)-1].String()
	}
	return jobs, next, err
}

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
	var ds []draft
	var want []timeKey
	err := s.read(ns, func(sp *space) error {
		t := dueTree(sp, kind)
		if t == nil {
			return fmt.Errorf("runnel: unknown due kind %d", kind)
		}
		due := func(v int64) bool { return v <= at }
		if kind == runnel.DueLease {
			due = func(v int64) bool { return v < at }
		}
		t.Ascend(func(k timeKey) bool {
			if !due(k.at) || len(want) >= limit {
				return false
			}
			want = append(want, k)
			if h, ok := sp.get(k.id); ok {
				ds = append(ds, sp.draft(&h))
			}
			return true
		})
		return nil
	})
	if err != nil || len(want) == len(ds) {
		return s.rebuild(ns, ds)
	}
	ids := make([]uid, len(want))
	for i, k := range want {
		ids[i] = k.id
	}
	jobs, err := s.gather(ns, ids, ds)
	if err != nil {
		return nil, err
	}
	keep := func(st runnel.State) bool { return st == runnel.StateWaiting }
	if kind == runnel.DueDeadline {
		keep = func(st runnel.State) bool { return !st.Final() }
	}
	return s.dropGone(ns, kind, want, jobs, keep)
}

func dueTree(sp *space, kind runnel.DueKind) *timeTree {
	switch kind {
	case runnel.DueLease:
		return sp.leases
	case runnel.DueStart:
		return sp.startBy
	case runnel.DueDeadline:
		return sp.ends
	}
	return nil
}

// A job leaving the cold store while Due reads may leave index entries behind; this drops them.
func (s *Store) dropGone(ns string, kind runnel.DueKind, want []timeKey, jobs []*runnel.Job, keep func(runnel.State) bool) ([]*runnel.Job, error) {
	kept := make(map[uid]bool, len(jobs))
	out := jobs[:0]
	for _, j := range jobs {
		if keep(j.State) {
			id, _ := parseID(j.ID)
			kept[id] = true
			out = append(out, j)
		}
	}
	return out, s.read(ns, func(sp *space) error {
		t := dueTree(sp, kind)
		for _, k := range want {
			if _, ok := sp.get(k.id); !ok && !kept[k.id] {
				t.Delete(k)
			}
		}
		return nil
	})
}

func (s *Store) Events(_ context.Context, ns string, after int64, limit int) ([]runnel.Event, error) {
	var out []runnel.Event
	var head int64
	err := s.read(ns, func(sp *space) error {
		head = sp.eventHead()
		i, _ := slices.BinarySearchFunc(sp.events, after+1, cmpEvent)
		end := min(len(sp.events), i+max(limit, 0))
		out = make([]runnel.Event, 0, end-i)
		for k := i; k < end; k++ {
			out = append(out, sp.event(&sp.events[k]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.olderEvents(ns, after, head, limit, out)
}

func (s *Store) Stats(_ context.Context, ns string) ([]runnel.QueueStats, error) {
	var out []runnel.QueueStats
	err := s.read(ns, func(sp *space) error {
		byName := map[string]*[stCount]int64{}
		add := func(name string, c *[stCount]int64) {
			sum := byName[name]
			if sum == nil {
				sum = &[stCount]int64{}
				byName[name] = sum
			}
			for st, n := range c {
				sum[st] += n
			}
		}
		for q, c := range sp.counts {
			add(sp.syms.name(q), c)
		}
		for q, c := range sp.coldCounts {
			add(q, c)
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

func (s *Store) SwapConfig(_ context.Context, ns string, kind runnel.ConfigKind, name string, old, next []byte) (bool, error) {
	ok := false
	err := s.write(ns, func(t *tx) error {
		cur, found := t.sp.configs[kind][name]
		if found != (old != nil) || !bytes.Equal(cur, old) {
			return nil
		}
		t.putConfig(kind, name, slices.Clone(next))
		ok = true
		return nil
	})
	return ok, err
}

func (s *Store) DeleteConfig(_ context.Context, ns string, kind runnel.ConfigKind, name string) error {
	return s.write(ns, func(t *tx) error {
		if _, ok := t.sp.configs[kind][name]; !ok {
			return runnel.ErrNotFound
		}
		t.delConfig(kind, name)
		return nil
	})
}
