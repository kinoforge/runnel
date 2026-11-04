package hybrid

import (
	"cmp"
	"encoding/json"
	"sync"

	"github.com/google/btree"
	"github.com/kinoforge/runnel"
)

const degree = 32

// timeKey orders jobs by a timestamp, then id.
type timeKey struct {
	at int64
	id string
}

func lessTime(a, b timeKey) bool {
	if a.at != b.at {
		return a.at < b.at
	}
	return a.id < b.id
}

// readyKey orders waiting jobs the way they are claimed: higher priority, then
// earlier run_at, then id.
type readyKey struct {
	prio int
	at   int64
	id   string
}

func lessReady(a, b readyKey) bool {
	if a.prio != b.prio {
		return a.prio > b.prio
	}
	if a.at != b.at {
		return a.at < b.at
	}
	return a.id < b.id
}

func lessID(a, b string) bool { return a < b }

type (
	timeTree  = btree.BTreeG[timeKey]
	readyTree = btree.BTreeG[readyKey]
	idTree    = btree.BTreeG[string]
)

type unique struct {
	id    string
	until int64
}

// uniqueKey orders the unique keys that expire at a time.
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

// space holds one namespace. Stored jobs are never changed in place: a change
// stores a new copy, so readers and snapshots can hold on to old pointers.
type space struct {
	ns string
	mu sync.Mutex

	jobs    map[string]*runnel.Job
	uniques map[string]unique
	expiry  *btree.BTreeG[uniqueKey]
	kids    map[string][]string
	events  []runnel.Event
	seq     int64
	cursors map[string]int64
	configs map[runnel.ConfigKind]map[string][]byte
	buckets map[string]bucket

	all     *idTree
	byState map[runnel.State]*idTree
	ready   map[string]*readyTree
	sched   map[string]*timeTree
	poll    map[string]*timeTree
	leases  *timeTree
	startBy *timeTree
	fin     *timeTree
	counts  map[string]map[runnel.State]int64
	active  map[string]int
	queued  map[string]int

	queueCfg    map[string]runnel.QueueConfig
	resourceCfg map[string]runnel.ResourceConfig
}

func newSpace(ns string) *space {
	sp := &space{
		ns: ns, jobs: map[string]*runnel.Job{}, uniques: map[string]unique{}, expiry: btree.NewG(degree, lessUnique), kids: map[string][]string{},
		cursors: map[string]int64{}, configs: map[runnel.ConfigKind]map[string][]byte{}, buckets: map[string]bucket{},
		all: btree.NewG(degree, lessID), byState: map[runnel.State]*idTree{},
		ready: map[string]*readyTree{}, sched: map[string]*timeTree{}, poll: map[string]*timeTree{},
		leases: btree.NewG(degree, lessTime), startBy: btree.NewG(degree, lessTime), fin: btree.NewG(degree, lessTime),
		counts: map[string]map[runnel.State]int64{}, active: map[string]int{}, queued: map[string]int{},
	}
	for _, st := range runnel.States {
		sp.byState[st] = btree.NewG(degree, lessID)
	}
	return sp
}

func treeOf[K any](m map[string]*btree.BTreeG[K], q string, less btree.LessFunc[K]) *btree.BTreeG[K] {
	t := m[q]
	if t == nil {
		t = btree.NewG(degree, less)
		m[q] = t
	}
	return t
}

func readyOf(j *runnel.Job) readyKey {
	return readyKey{prio: j.Priority, at: runnel.Millis(j.RunAt), id: j.ID}
}

func (sp *space) countActive(j *runnel.Job, d int) {
	sp.active["q|"+j.Queue] += d
	if j.Tenant != "" {
		sp.active["qt|"+j.Queue+"|"+j.Tenant] += d
	}
	if j.Resource != "" {
		sp.active["r|"+j.Resource] += d
		if j.Tenant != "" {
			sp.active["rt|"+j.Resource+"|"+j.Tenant] += d
		}
	}
}

func (sp *space) countQueued(j *runnel.Job, d int) {
	if j.Tenant != "" {
		sp.queued[j.Queue+"|"+j.Tenant] += d
	}
}

func (sp *space) count(j *runnel.Job, d int64) {
	c := sp.counts[j.Queue]
	if c == nil {
		c = map[runnel.State]int64{}
		sp.counts[j.Queue] = c
	}
	c[j.State] += d
}

// index adds j to every index its state calls for. Waiting jobs due by now go
// to the ready tree, later ones to the schedule.
func (sp *space) index(j *runnel.Job) {
	sp.all.ReplaceOrInsert(j.ID)
	sp.byState[j.State].ReplaceOrInsert(j.ID)
	sp.count(j, 1)
	switch j.State {
	case runnel.StateWaiting:
		treeOf(sp.sched, j.Queue, lessTime).ReplaceOrInsert(timeKey{runnel.Millis(j.RunAt), j.ID})
		if !j.StartBy.IsZero() {
			sp.startBy.ReplaceOrInsert(timeKey{runnel.Millis(j.StartBy), j.ID})
		}
		sp.countQueued(j, 1)
	case runnel.StateBlocked:
		sp.countQueued(j, 1)
	case runnel.StateRunning:
		sp.leases.ReplaceOrInsert(timeKey{runnel.Millis(j.LeaseUntil), j.ID})
		sp.countActive(j, 1)
	case runnel.StatePolling:
		treeOf(sp.poll, j.Queue, lessTime).ReplaceOrInsert(timeKey{runnel.Millis(j.RunAt), j.ID})
		sp.countActive(j, 1)
	default:
		sp.fin.ReplaceOrInsert(timeKey{runnel.Millis(j.FinishedAt), j.ID})
	}
}

func (sp *space) unindex(j *runnel.Job) {
	sp.all.Delete(j.ID)
	sp.byState[j.State].Delete(j.ID)
	sp.count(j, -1)
	switch j.State {
	case runnel.StateWaiting:
		if t := sp.sched[j.Queue]; t != nil {
			t.Delete(timeKey{runnel.Millis(j.RunAt), j.ID})
		}
		if t := sp.ready[j.Queue]; t != nil {
			t.Delete(readyOf(j))
		}
		sp.startBy.Delete(timeKey{runnel.Millis(j.StartBy), j.ID})
		sp.countQueued(j, -1)
	case runnel.StateBlocked:
		sp.countQueued(j, -1)
	case runnel.StateRunning:
		sp.leases.Delete(timeKey{runnel.Millis(j.LeaseUntil), j.ID})
		sp.countActive(j, -1)
	case runnel.StatePolling:
		if t := sp.poll[j.Queue]; t != nil {
			t.Delete(timeKey{runnel.Millis(j.RunAt), j.ID})
		}
		sp.countActive(j, -1)
	default:
		sp.fin.Delete(timeKey{runnel.Millis(j.FinishedAt), j.ID})
	}
}

// promote moves the waiting jobs of queue q that are due at now from the
// schedule to the ready tree.
func (sp *space) promote(q string, now int64) {
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
		if j := sp.jobs[k.id]; j != nil {
			treeOf(sp.ready, q, lessReady).ReplaceOrInsert(readyOf(j))
		}
	}
}

func (sp *space) setJob(j *runnel.Job) {
	if old := sp.jobs[j.ID]; old != nil {
		sp.unindex(old)
	}
	sp.jobs[j.ID] = j
	sp.index(j)
}

func (sp *space) delJob(id string) {
	if old := sp.jobs[id]; old != nil {
		sp.unindex(old)
		delete(sp.jobs, id)
	}
	delete(sp.kids, id)
}

func (sp *space) setUnique(key string, u unique) {
	sp.dropUnique(key)
	sp.uniques[key] = u
	if u.until > 0 {
		sp.expiry.ReplaceOrInsert(uniqueKey{u.until, key})
	}
}

func (sp *space) dropUnique(key string) {
	if old, ok := sp.uniques[key]; ok {
		sp.expiry.Delete(uniqueKey{old.until, key})
		delete(sp.uniques, key)
	}
}

func (sp *space) setConfig(kind runnel.ConfigKind, name string, val []byte) {
	m := sp.configs[kind]
	if m == nil {
		m = map[string][]byte{}
		sp.configs[kind] = m
	}
	m[name] = val
	sp.forget(kind)
}

func (sp *space) delConfig(kind runnel.ConfigKind, name string) {
	delete(sp.configs[kind], name)
	sp.forget(kind)
}

func (sp *space) forget(kind runnel.ConfigKind) {
	switch kind {
	case runnel.ConfigQueue:
		sp.queueCfg = nil
	case runnel.ConfigResource:
		sp.resourceCfg = nil
	}
}

func (sp *space) queueConfig(q string) runnel.QueueConfig {
	if sp.queueCfg == nil {
		sp.queueCfg = decodeAll[runnel.QueueConfig](sp.configs[runnel.ConfigQueue])
	}
	return sp.queueCfg[q]
}

func (sp *space) resourceConfig(r string) runnel.ResourceConfig {
	if sp.resourceCfg == nil {
		sp.resourceCfg = decodeAll[runnel.ResourceConfig](sp.configs[runnel.ConfigResource])
	}
	return sp.resourceCfg[r]
}

// decodeAll skips values that do not decode; the client validates configs
// before it stores them.
func decodeAll[T any](raw map[string][]byte) map[string]T {
	out := make(map[string]T, len(raw))
	for name, b := range raw {
		var v T
		if json.Unmarshal(b, &v) == nil {
			out[name] = v
		}
	}
	return out
}

func (sp *space) addEvent(e runnel.Event) {
	sp.events = append(sp.events, e)
	sp.seq = max(sp.seq, e.Seq)
}

// dropEvents removes the events up to and including seq, which are always the
// oldest.
func (sp *space) dropEvents(seq int64) {
	i := 0
	for i < len(sp.events) && sp.events[i].Seq <= seq {
		i++
	}
	sp.events = sp.events[i:]
}

func cmpEvent(e runnel.Event, seq int64) int { return cmp.Compare(e.Seq, seq) }
