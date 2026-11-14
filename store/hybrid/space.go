package hybrid

import (
	"cmp"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/google/btree"
	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
)

const degree = 32

type timeKey struct {
	at int64
	id uid
}

func lessTime(a, b timeKey) bool {
	if a.at != b.at {
		return a.at < b.at
	}
	return lessID(a.id, b.id)
}

type readyKey struct {
	prio int32
	at   int64
	id   uid
}

func lessReady(a, b readyKey) bool {
	if a.prio != b.prio {
		return a.prio > b.prio
	}
	if a.at != b.at {
		return a.at < b.at
	}
	return lessID(a.id, b.id)
}

func lessID(a, b uid) bool { return cmpID(a, b) < 0 }

func cmpID(a, b uid) int {
	if c := cmp.Compare(binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(b[:8])); c != 0 {
		return c
	}
	return cmp.Compare(binary.BigEndian.Uint64(a[8:]), binary.BigEndian.Uint64(b[8:]))
}

type (
	timeTree  = btree.BTreeG[timeKey]
	readyTree = btree.BTreeG[readyKey]
	idTree    = btree.BTreeG[uid]
)

type unique struct {
	id    uid
	until int64
}

type uniqueKey struct {
	until int64
	key   string
}

func lessUnique(a, b uniqueKey) bool {
	if a.until != b.until {
		return a.until < b.until
	}
	return a.key < b.key
}

type bucket struct {
	tokens float64
	at     int64
}

type countKey struct {
	kind uint8
	a, b sym
}

const (
	cQueue uint8 = iota + 1
	cQueueGroup
	cPool
	cPoolGroup
)

type space struct {
	ns         string
	mu         sync.Mutex
	queue      queue
	syms       syms
	coldCounts map[string]*[stCount]int64
	retired    atomic.Uint64
	coldSeq    int64
	coldQueued map[[2]string]int64
	heads      map[sym]cold.Head
	spilled    atomic.Bool
	hot        atomic.Int64
	total      *atomic.Int64
	// Jobs whose cold copy is still being deleted stay in memory.
	unretiring map[uid]int
	// Ids are time ordered, so a new job's id is above coldMax and needs no cold lookup.
	coldMax atomic.Pointer[uid]

	idx     map[uid]int32
	idxPeak int
	jobs    slabs
	cache   cache

	uniques map[string]unique
	expiry  *btree.BTreeG[uniqueKey]
	kids    map[uid][]uid
	events  []event
	seq     int64
	cursors map[string]int64
	configs map[runnel.ConfigKind]map[string][]byte
	buckets map[string]bucket

	byState [stCount]*idTree
	ready   map[sym]*readyTree
	sched   map[sym]*timeTree
	poll    map[sym]*timeTree
	leases  *timeTree
	startBy *timeTree
	ends    *timeTree
	fin     *timeTree
	counts  map[sym]*[stCount]int64
	active  map[countKey]int
	queued  map[[2]sym]int

	queueCfg map[string]runnel.QueueConfig
	poolCfg  map[string]runnel.PoolConfig
}

func newSpace(ns string) *space {
	sp := &space{
		ns: ns, syms: newSyms(), idx: map[uid]int32{}, cache: cache{bodies: map[uid][]byte{}}, coldCounts: map[string]*[stCount]int64{},
		coldQueued: map[[2]string]int64{}, heads: map[sym]cold.Head{}, unretiring: map[uid]int{},
		uniques: map[string]unique{}, expiry: btree.NewG(degree, lessUnique), kids: map[uid][]uid{},
		cursors: map[string]int64{}, configs: map[runnel.ConfigKind]map[string][]byte{}, buckets: map[string]bucket{},
		ready: map[sym]*readyTree{}, sched: map[sym]*timeTree{}, poll: map[sym]*timeTree{},
		leases: btree.NewG(degree, lessTime), startBy: btree.NewG(degree, lessTime), ends: btree.NewG(degree, lessTime), fin: btree.NewG(degree, lessTime),
		counts: map[sym]*[stCount]int64{}, active: map[countKey]int{}, queued: map[[2]sym]int{},
	}
	for i := range sp.byState {
		sp.byState[i] = btree.NewG(degree, lessID)
	}
	return sp
}

func (sp *space) get(id uid) (job, bool) {
	if i, ok := sp.idx[id]; ok {
		return *sp.jobs.at(i), true
	}
	return job{}, false
}

func (sp *space) bodyRef(id uid) (ref, bool) {
	if i, ok := sp.idx[id]; ok {
		return sp.jobs.at(i).body, true
	}
	return ref{}, false
}

func (sp *space) setJob(h job) {
	if i, ok := sp.idx[h.id]; ok {
		old := sp.jobs.at(i)
		if old.state != h.state {
			sp.byState[old.state].Delete(h.id)
			sp.byState[h.state].ReplaceOrInsert(h.id)
		}
		sp.unindex(old)
		*old = h
		sp.index(&h)
		return
	}
	sp.idx[h.id] = sp.jobs.put(h)
	sp.idxPeak = max(sp.idxPeak, len(sp.idx))
	sp.byState[h.state].ReplaceOrInsert(h.id)
	sp.index(&h)
}

func (sp *space) delJob(id uid) {
	if i, ok := sp.idx[id]; ok {
		h := sp.jobs.at(i)
		sp.byState[h.state].Delete(id)
		sp.unindex(h)
		sp.jobs.clear(i)
		delete(sp.idx, id)
		sp.shrinkIdx()
	}
	delete(sp.kids, id)
	sp.cache.del(id)
}

func (sp *space) draft(h *job) draft {
	d := sp.syms.draft(h)
	d.body = sp.cache.bodies[h.id]
	return d
}

// Go maps, clones too, keep the memory of every entry they ever held.
func (sp *space) shrinkIdx() {
	if sp.idxPeak < shrinkAt || len(sp.idx)*4 > sp.idxPeak {
		return
	}
	m := make(map[uid]int32, len(sp.idx))
	for id, i := range sp.idx {
		m[id] = i
	}
	sp.idx, sp.idxPeak = m, len(m)
}

func (sp *space) body(h *job, fs *files) ([]byte, error) {
	if b, ok := sp.cache.bodies[h.id]; ok {
		return b, nil
	}
	if h.body.n == 0 || fs == nil {
		return nil, errNoBody
	}
	return fs.read(h.body)
}

func (sp *space) count(h *job, d int) {
	key := [2]sym{h.queue, h.group}
	if h.state == stWaiting {
		sp.hot.Add(int64(d))
		sp.total.Add(int64(d))
	}
	switch h.state {
	case stWaiting, stBlocked:
		if h.group != 0 {
			sp.queued[key] += d
		}
	case stRunning, stPolling:
		sp.active[countKey{cQueue, h.queue, 0}] += d
		if h.group != 0 {
			sp.active[countKey{cQueueGroup, h.queue, h.group}] += d
		}
		if h.pool != 0 {
			sp.active[countKey{cPool, h.pool, 0}] += d
			if h.group != 0 {
				sp.active[countKey{cPoolGroup, h.pool, h.group}] += d
			}
		}
	}
	c := sp.counts[h.queue]
	if c == nil {
		c = &[stCount]int64{}
		sp.counts[h.queue] = c
	}
	c[h.state] += int64(d)
}

func treeOf[K any](m map[sym]*btree.BTreeG[K], q sym, less btree.LessFunc[K]) *btree.BTreeG[K] {
	t := m[q]
	if t == nil {
		t = btree.NewG(degree, less)
		m[q] = t
	}
	return t
}

func readyOf(h *job) readyKey { return readyKey{prio: h.prio, at: h.runAt, id: h.id} }

func (sp *space) index(h *job) {
	sp.count(h, 1)
	if h.deadline != 0 && !h.final() {
		sp.ends.ReplaceOrInsert(timeKey{h.deadline, h.id})
	}
	switch h.state {
	case stWaiting:
		treeOf(sp.sched, h.queue, lessTime).ReplaceOrInsert(timeKey{h.runAt, h.id})
		if h.startBy != 0 {
			sp.startBy.ReplaceOrInsert(timeKey{h.startBy, h.id})
		}
	case stRunning:
		sp.leases.ReplaceOrInsert(timeKey{h.lease, h.id})
	case stPolling:
		treeOf(sp.poll, h.queue, lessTime).ReplaceOrInsert(timeKey{h.runAt, h.id})
	case stCompleted, stDead, stCancelled, stExpired:
		sp.fin.ReplaceOrInsert(timeKey{h.finished, h.id})
	}
}

func (sp *space) unindex(h *job) {
	sp.count(h, -1)
	if h.deadline != 0 && !h.final() {
		sp.ends.Delete(timeKey{h.deadline, h.id})
	}
	switch h.state {
	case stWaiting:
		if !sp.unready(h) {
			if t := sp.sched[h.queue]; t != nil {
				t.Delete(timeKey{h.runAt, h.id})
			}
		}
		if h.startBy != 0 {
			sp.startBy.Delete(timeKey{h.startBy, h.id})
		}
	case stRunning:
		sp.leases.Delete(timeKey{h.lease, h.id})
	case stPolling:
		if t := sp.poll[h.queue]; t != nil {
			t.Delete(timeKey{h.runAt, h.id})
		}
	case stCompleted, stDead, stCancelled, stExpired:
		sp.fin.Delete(timeKey{h.finished, h.id})
	}
}

func (sp *space) unready(h *job) bool {
	t := sp.ready[h.queue]
	if t == nil {
		return false
	}
	_, ok := t.Delete(readyOf(h))
	return ok
}

func (sp *space) promote(q sym, now int64) {
	t := sp.sched[q]
	if t == nil {
		return
	}
	for {
		k, ok := t.Min()
		if !ok || k.at > now {
			return
		}
		t.DeleteMin()
		if h, ok := sp.get(k.id); ok {
			treeOf(sp.ready, q, lessReady).ReplaceOrInsert(readyOf(&h))
		}
	}
}
