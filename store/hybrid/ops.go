package hybrid

import (
	"fmt"
	"slices"

	"github.com/kinoforge/runnel"
)

// Every change to a namespace is one of these ops. A store call applies its ops
// to memory and logs them as one record; replay and snapshots apply the same
// ops, so recovery runs no job logic.
const (
	opPutJob byte = iota + 1
	opPutHot
	opDelJob
	opPutUnique
	opDelUnique
	opAddKid
	opDelKids
	opPutEvent
	opDropEvents
	opPutCursor
	opPutConfig
	opDelConfig
	opPutBucket
	opSeq
)

// tx records the ops of one store call on one namespace. bodies lists where
// in the record each new body starts, to point its job at the log once the
// record has a place there.
type tx struct {
	sp     *space
	rec    *enc
	head   int
	wake   bool
	ends   []uid
	bodies []placed
}

type placed struct {
	id  uid
	pos int
	n   int
}

// newTx starts a call on sp; with logged unset it records nothing.
func newTx(sp *space, logged bool) *tx {
	t := &tx{sp: sp}
	if logged {
		t.rec = &enc{}
		t.rec.str(sp.ns)
		t.head = len(t.rec.b)
	}
	return t
}

func (t *tx) changed() bool { return t.rec != nil && len(t.rec.b) > t.head }

// put stores h with a new body.
func (t *tx) put(h job, body []byte) {
	h.body = ref{}
	t.sp.setJob(h)
	t.sp.cache.put(h.id, body)
	if t.rec != nil {
		t.rec.byte(opPutJob)
		t.rec.hot(&h, &t.sp.syms)
		pos := t.rec.blob(body)
		t.bodies = append(t.bodies, placed{id: h.id, pos: pos, n: len(body)})
	}
	if h.is(stWaiting) {
		t.wake = true
	}
}

// putHot stores h, keeping the body it has.
func (t *tx) putHot(h job) {
	t.sp.setJob(h)
	t.rec.byte(opPutHot)
	t.rec.hot(&h, &t.sp.syms)
	if h.is(stWaiting) {
		t.wake = true
	}
}

func (t *tx) delJob(id uid) {
	t.sp.delJob(id)
	t.rec.byte(opDelJob)
	t.rec.bytes(id[:])
}

func (t *tx) putUnique(key string, u unique) {
	t.sp.setUnique(key, u)
	t.rec.opUnique(key, u)
}

func (t *tx) delUnique(key string) {
	t.sp.dropUnique(key)
	t.rec.byte(opDelUnique)
	t.rec.str(key)
}

func (t *tx) addKid(parent, kid uid) {
	t.sp.kids[parent] = append(t.sp.kids[parent], kid)
	t.rec.opKid(parent, kid)
}

func (t *tx) delKids(parent uid) {
	delete(t.sp.kids, parent)
	t.rec.byte(opDelKids)
	t.rec.bytes(parent[:])
}

func (t *tx) putEvent(e event) {
	t.sp.addEvent(e)
	t.rec.opEvent(&e, t.sp.syms.names)
	t.ends = append(t.ends, e.id)
}

func (t *tx) dropEvents(seq int64) {
	t.sp.dropEvents(seq)
	t.rec.byte(opDropEvents)
	t.rec.int(seq)
}

func (t *tx) putCursor(name string, v int64) {
	t.sp.cursors[name] = v
	t.rec.opCursor(name, v)
}

func (t *tx) putConfig(kind runnel.ConfigKind, name string, val []byte) {
	t.sp.setConfig(kind, name, val)
	t.rec.opConfig(kind, name, val)
}

func (t *tx) delConfig(kind runnel.ConfigKind, name string) {
	t.sp.delConfig(kind, name)
	t.rec.byte(opDelConfig)
	t.rec.str(string(kind))
	t.rec.str(name)
}

func (t *tx) putBucket(key string, b bucket) {
	t.sp.buckets[key] = b
	t.rec.opBucket(key, b)
}

// blob writes b with its length and returns where b starts in the record.
func (e *enc) blob(b []byte) int {
	if e == nil {
		return 0
	}
	e.bytes(b)
	return len(e.b) - len(b)
}

func (e *enc) opUnique(key string, u unique) {
	e.byte(opPutUnique)
	e.str(key)
	e.bytes(u.id[:])
	e.int(u.until)
}

func (e *enc) opKid(parent, kid uid) {
	e.byte(opAddKid)
	e.bytes(parent[:])
	e.bytes(kid[:])
}

func (e *enc) opEvent(ev *event, names []string) {
	if e == nil {
		return
	}
	e.byte(opPutEvent)
	e.int(ev.seq)
	e.bytes(ev.id[:])
	e.str(names[ev.queue])
	e.str(names[ev.kind])
	e.byte(ev.state)
	e.int(ev.at)
}

func (e *enc) opCursor(name string, v int64) {
	e.byte(opPutCursor)
	e.str(name)
	e.int(v)
}

func (e *enc) opConfig(kind runnel.ConfigKind, name string, val []byte) {
	e.byte(opPutConfig)
	e.str(string(kind))
	e.str(name)
	e.bytes(val)
}

func (e *enc) opBucket(key string, b bucket) {
	e.byte(opPutBucket)
	e.str(key)
	e.float(b.tokens)
	e.int(b.at)
}

func (e *enc) opSeq(seq int64) {
	e.byte(opSeq)
	e.int(seq)
}

// replayer applies logged records. file and base say where the current record
// sits on disk, so bodies are found there later instead of kept in memory.
type replayer struct {
	get  func(ns string) *space
	file uint32
	base int64
}

func (r *replayer) record(rec []byte) error {
	d := newDec(rec)
	sp := r.get(d.str())
	for !d.done() {
		if err := r.op(d, sp); err != nil {
			return err
		}
	}
	return d.err
}

func readID(d *dec) uid {
	var id uid
	copy(id[:], d.bytes())
	return id
}

func (r *replayer) op(d *dec, sp *space) error {
	switch op := d.byte(); op {
	case opPutJob, opPutHot:
		h := decodeHot(d, &sp.syms)
		old, _ := sp.get(h.id)
		h.body = old.body
		if op == opPutJob {
			b := d.bytes()
			h.body = ref{file: r.file, n: uint32(len(b)), off: r.base + int64(d.pos()-len(b))}
		}
		if d.err == nil {
			sp.setJob(h)
		}
	case opDelJob:
		sp.delJob(readID(d))
	case opPutUnique:
		key := d.str()
		sp.setUnique(key, unique{id: readID(d), until: d.int()})
	case opDelUnique:
		sp.dropUnique(d.str())
	case opAddKid:
		p, kid := readID(d), readID(d)
		if !slices.Contains(sp.kids[p], kid) {
			sp.kids[p] = append(sp.kids[p], kid)
		}
	case opDelKids:
		delete(sp.kids, readID(d))
	case opPutEvent:
		e := event{seq: d.int(), id: readID(d)}
		e.queue = sp.syms.id(d.str())
		e.kind = sp.syms.id(d.str())
		e.state, e.at = d.byte(), d.int()
		if n := len(sp.events); n == 0 || e.seq > sp.events[n-1].seq {
			sp.addEvent(e)
		}
	case opDropEvents:
		sp.dropEvents(d.int())
	case opPutCursor:
		name := d.str()
		sp.cursors[name] = d.int()
	case opPutConfig:
		kind, name := runnel.ConfigKind(d.str()), d.str()
		sp.setConfig(kind, name, []byte(d.str()))
	case opDelConfig:
		kind := runnel.ConfigKind(d.str())
		sp.delConfig(kind, d.str())
	case opPutBucket:
		key := d.str()
		sp.buckets[key] = bucket{tokens: d.float(), at: d.int()}
	case opSeq:
		sp.seq = max(sp.seq, d.int())
	default:
		if d.err == nil {
			return fmt.Errorf("hybrid: unknown op %d", op)
		}
	}
	return d.err
}
