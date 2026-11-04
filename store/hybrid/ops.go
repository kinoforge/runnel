package hybrid

import (
	"fmt"

	"github.com/kinoforge/runnel"
)

// Every change to a namespace is one of these ops. A store call applies its ops
// to memory and logs them as one record; replay and snapshots apply the same
// ops, so recovery runs no job logic.
const (
	opPutJob byte = iota + 1
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

// tx records the ops of one store call on one namespace.
type tx struct {
	sp   *space
	rec  enc
	wake bool
	ends []string
	err  error
	head int
}

func newTx(sp *space) *tx {
	t := &tx{sp: sp}
	t.rec.str(sp.ns)
	t.head = len(t.rec.b)
	return t
}

func (t *tx) changed() bool { return len(t.rec.b) > t.head }

func (t *tx) putJob(j *runnel.Job) {
	normalize(j)
	j.Namespace = t.sp.ns
	t.sp.setJob(j)
	if err := t.rec.opJob(j); err != nil && t.err == nil {
		t.err = err
	}
	if j.State == runnel.StateWaiting {
		t.wake = true
	}
}

func (t *tx) delJob(id string) {
	t.sp.delJob(id)
	t.rec.byte(opDelJob)
	t.rec.str(id)
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

func (t *tx) addKid(parent, kid string) {
	t.sp.kids[parent] = append(t.sp.kids[parent], kid)
	t.rec.opKid(parent, kid)
}

func (t *tx) delKids(parent string) {
	delete(t.sp.kids, parent)
	t.rec.byte(opDelKids)
	t.rec.str(parent)
}

func (t *tx) putEvent(e runnel.Event) {
	t.sp.addEvent(e)
	t.rec.opEvent(e)
	t.ends = append(t.ends, e.JobID)
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

func (e *enc) opJob(j *runnel.Job) error {
	e.byte(opPutJob)
	return encodeJob(e, j)
}

func (e *enc) opUnique(key string, u unique) {
	e.byte(opPutUnique)
	e.str(key)
	e.str(u.id)
	e.int(u.until)
}

func (e *enc) opKid(parent, kid string) {
	e.byte(opAddKid)
	e.str(parent)
	e.str(kid)
}

func (e *enc) opEvent(ev runnel.Event) {
	e.byte(opPutEvent)
	e.int(ev.Seq)
	e.str(ev.JobID)
	e.str(ev.Queue)
	e.str(ev.Kind)
	e.str(string(ev.State))
	e.time(ev.At)
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

// replay applies one logged record to the spaces get returns.
func replay(rec []byte, get func(ns string) *space) error {
	d := &dec{b: rec}
	sp := get(d.str())
	for !d.done() {
		if err := replayOp(d, sp); err != nil {
			return err
		}
	}
	return d.err
}

func replayOp(d *dec, sp *space) error {
	switch op := d.byte(); op {
	case opPutJob:
		j, err := decodeJob(d, sp.ns)
		if err != nil {
			return err
		}
		sp.setJob(j)
	case opDelJob:
		sp.delJob(d.str())
	case opPutUnique:
		key := d.str()
		sp.setUnique(key, unique{id: d.str(), until: d.int()})
	case opDelUnique:
		sp.dropUnique(d.str())
	case opAddKid:
		p := d.str()
		sp.kids[p] = append(sp.kids[p], d.str())
	case opDelKids:
		delete(sp.kids, d.str())
	case opPutEvent:
		e := runnel.Event{Seq: d.int(), Namespace: sp.ns, JobID: d.str(), Queue: d.str(), Kind: d.str()}
		e.State, e.At = runnel.State(d.str()), d.time()
		sp.addEvent(e)
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
