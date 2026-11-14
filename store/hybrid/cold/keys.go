package cold

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Namespace names never contain a zero byte, so a zero byte ends the namespace in a key.
const (
	kCount    byte = 'c'
	kStart    byte = 'd'
	kDeadline byte = 'x'
	kFuture   byte = 'f'
	kJob      byte = 'j'
	kGroup    byte = 'q'
	kReady    byte = 'r'
	kState    byte = 's'
	kTime     byte = 't'
)

var errBadKey = errors.New("cold: damaged key or value")

func prefix(kind byte, ns string, more int) []byte {
	b := make([]byte, 0, 2+len(ns)+more)
	b = append(b, kind)
	b = append(b, ns...)
	return append(b, 0)
}

func span(kind byte, ns string) ([]byte, []byte) {
	lo := prefix(kind, ns, 0)
	hi := prefix(kind, ns, 0)
	hi[len(hi)-1] = 1
	return lo, hi
}

func jobKey(ns string, id ID) []byte { return append(prefix(kJob, ns, len(id)), id[:]...) }

func stateKey(ns string, st uint8, id ID) []byte {
	return append(append(prefix(kState, ns, 1+len(id)), st), id[:]...)
}

func timeKey(ns string, at int64, id ID) []byte {
	return append(binary.BigEndian.AppendUint64(prefix(kTime, ns, 8+len(id)), uint64(at)), id[:]...)
}

func (c Count) key() []byte {
	if c.Group != "" {
		return append(appendStr(prefix(kGroup, c.NS, 1+len(c.Queue)+len(c.Group)), c.Queue), c.Group...)
	}
	return append(append(prefix(kCount, c.NS, 1+len(c.Queue)), c.State), c.Queue...)
}

func decodeCountKey(k []byte) (Count, bool) {
	i := bytes.IndexByte(k, 0)
	if i < 1 || len(k) < i+2 {
		return Count{}, false
	}
	c := Count{NS: string(k[1:i])}
	if k[0] == kGroup {
		q, rest, ok := readStr(k[i+1:])
		c.Queue, c.State, c.Group = q, Waiting, string(rest)
		return c, ok && c.Group != ""
	}
	c.State, c.Queue = k[i+1], string(k[i+2:])
	return c, true
}

func idOf(k []byte) ID {
	var id ID
	copy(id[:], k[len(k)-len(id):])
	return id
}

func encodeJob(j *Job) []byte {
	b := make([]byte, 0, 2+5*binary.MaxVarintLen64+len(j.Queue)+len(j.Kind)+len(j.Group)+3+len(j.Data))
	b = append(b, j.State)
	for _, v := range []int64{j.Finished, int64(j.Prio), j.RunAt, j.StartBy, j.Deadline} {
		b = binary.AppendVarint(b, v)
	}
	ready := byte(0)
	if j.Ready {
		ready = 1
	}
	b = append(b, ready)
	b = appendStr(appendStr(appendStr(b, j.Queue), j.Kind), j.Group)
	return append(b, j.Data...)
}

func decodeJob(ns string, id ID, v []byte, data bool) (Job, error) {
	j := Job{NS: ns, ID: id}
	if len(v) < 1 {
		return j, errBadKey
	}
	j.State = v[0]
	v = v[1:]
	var prio int64
	for _, p := range []*int64{&j.Finished, &prio, &j.RunAt, &j.StartBy, &j.Deadline} {
		at, n := binary.Varint(v)
		if n <= 0 {
			return j, errBadKey
		}
		*p, v = at, v[n:]
	}
	if len(v) < 1 {
		return j, errBadKey
	}
	j.Prio, j.Ready = int32(prio), v[0] == 1
	q, k, t, rest, ok := readNames(v[1:])
	if !ok {
		return j, errBadKey
	}
	j.Queue, j.Kind, j.Group = q, k, t
	if data {
		j.Data = bytes.Clone(rest)
	}
	return j, nil
}

func encodeNames(j *Job) []byte {
	return appendStr(appendStr(appendStr(nil, j.Queue), j.Kind), j.Group)
}

func readNames(b []byte) (q, k, t string, rest []byte, ok bool) {
	if q, b, ok = readStr(b); !ok {
		return
	}
	if k, b, ok = readStr(b); !ok {
		return
	}
	t, rest, ok = readStr(b)
	return
}

func encodeWhen(j *Job) []byte { return appendStr([]byte{j.State}, j.Queue) }

func decodeWhen(v []byte) (uint8, string, bool) {
	if len(v) < 1 {
		return 0, "", false
	}
	q, _, ok := readStr(v[1:])
	return v[0], q, ok
}

func appendStr(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

func readStr(b []byte) (string, []byte, bool) {
	n, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < n {
		return "", nil, false
	}
	return string(b[k : k+int(n)]), b[k+int(n):], true
}

func encodeInt(n int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(n)) }

func decodeInt(v []byte) int64 {
	if len(v) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(v))
}
