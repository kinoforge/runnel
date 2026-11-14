package hybrid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var errShort = errors.New("hybrid: record cut short")

type enc struct{ b []byte }

func (e *enc) byte(v byte) {
	if e != nil {
		e.b = append(e.b, v)
	}
}

func (e *enc) int(v int64) {
	if e != nil {
		e.b = binary.AppendVarint(e.b, v)
	}
}

func (e *enc) float(v float64) {
	if e != nil {
		e.b = binary.LittleEndian.AppendUint64(e.b, math.Float64bits(v))
	}
}

func (e *enc) bytes(v []byte) {
	if e != nil {
		e.b = binary.AppendUvarint(e.b, uint64(len(v)))
		e.b = append(e.b, v...)
	}
}

func (e *enc) str(v string) {
	if e != nil {
		e.b = binary.AppendUvarint(e.b, uint64(len(v)))
		e.b = append(e.b, v...)
	}
}

type dec struct {
	b   []byte
	n   int
	err error
}

func newDec(b []byte) *dec { return &dec{b: b, n: len(b)} }

func (d *dec) pos() int { return d.n - len(d.b) }

func (d *dec) done() bool { return d.err != nil || len(d.b) == 0 }

func (d *dec) byte() byte {
	if d.err != nil || len(d.b) == 0 {
		d.err = errShort
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *dec) int() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errShort
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) float() float64 {
	if d.err != nil || len(d.b) < 8 {
		d.err = errShort
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(d.b))
	d.b = d.b[8:]
	return v
}

func (d *dec) bytes() []byte {
	if d.err != nil {
		return nil
	}
	n, k := binary.Uvarint(d.b)
	if k <= 0 || uint64(len(d.b)-k) < n {
		d.err = errShort
		return nil
	}
	v := d.b[k : k+int(n)]
	d.b = d.b[k+int(n):]
	return v
}

func (d *dec) str() string { return string(d.bytes()) }

func (e *enc) hot(h *job, t *syms) { e.named(h, t.names5(h)) }

func (e *enc) named(h *job, names [5]string) {
	if e == nil {
		return
	}
	e.bytes(h.id[:])
	for _, n := range names {
		e.str(n)
	}
	e.byte(h.state)
	e.byte(h.flags)
	for _, v := range []int64{int64(h.prio), int64(h.attempt), int64(h.maxAttempts), int64(h.pending), h.runAt, h.waitingSince,
		h.startBy, h.lease, h.created, h.updated, h.started, h.finished, h.version, h.window, h.deadline} {
		e.int(v)
	}
}

func decodeHot(d *dec, t *syms) job {
	var h job
	copy(h.id[:], d.bytes())
	for _, p := range []*sym{&h.queue, &h.kind, &h.group, &h.pool, &h.worker} {
		*p = t.id(d.str())
	}
	h.state, h.flags = d.byte(), d.byte()
	for _, p := range []*int32{&h.prio, &h.attempt, &h.maxAttempts, &h.pending} {
		*p = int32(d.int())
	}
	for _, p := range []*int64{&h.runAt, &h.waitingSince, &h.startBy, &h.lease, &h.created, &h.updated, &h.started,
		&h.finished, &h.version, &h.window, &h.deadline} {
		*p = d.int()
	}
	if d.err == nil && (h.state == 0 || h.state >= stCount) {
		d.err = fmt.Errorf("hybrid: bad state %d", h.state)
	}
	return h
}
