package cold

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Keys start with a kind byte and the namespace, ended by a zero byte, which
// namespace names never contain.
const (
	kCount byte = 'c'
	kJob   byte = 'j'
	kState byte = 's'
	kTime  byte = 't'
)

var errBadKey = errors.New("cold: damaged key or value")

func prefix(kind byte, ns string, more int) []byte {
	b := make([]byte, 0, 2+len(ns)+more)
	b = append(b, kind)
	b = append(b, ns...)
	return append(b, 0)
}

// span returns the bounds of every key of one kind in ns.
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

func countKey(c Count) []byte {
	return append(append(prefix(kCount, c.NS, 1+len(c.Queue)), c.State), c.Queue...)
}

func decodeCountKey(k []byte) (Count, bool) {
	i := bytes.IndexByte(k, 0)
	if i < 1 || len(k) < i+2 {
		return Count{}, false
	}
	return Count{NS: string(k[1:i]), State: k[i+1], Queue: string(k[i+2:])}, true
}

// idOf reads the id that ends a key.
func idOf(k []byte) ID {
	var id ID
	copy(id[:], k[len(k)-len(id):])
	return id
}

// A record holds the state, finish time and names, then the caller's data.
func encodeJob(j *Job) []byte {
	b := make([]byte, 0, 1+binary.MaxVarintLen64+len(j.Queue)+len(j.Kind)+len(j.Tenant)+3+len(j.Data))
	b = append(b, j.State)
	b = binary.AppendVarint(b, j.Finished)
	b = appendStr(appendStr(appendStr(b, j.Queue), j.Kind), j.Tenant)
	return append(b, j.Data...)
}

func decodeJob(ns string, id ID, v []byte, data bool) (Job, error) {
	j := Job{NS: ns, ID: id}
	if len(v) < 1 {
		return j, errBadKey
	}
	j.State = v[0]
	at, n := binary.Varint(v[1:])
	if n <= 0 {
		return j, errBadKey
	}
	j.Finished = at
	q, k, t, rest, ok := readNames(v[1+n:])
	if !ok {
		return j, errBadKey
	}
	j.Queue, j.Kind, j.Tenant = q, k, t
	if data {
		j.Data = bytes.Clone(rest)
	}
	return j, nil
}

func encodeNames(j *Job) []byte {
	return appendStr(appendStr(appendStr(nil, j.Queue), j.Kind), j.Tenant)
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

// encodeWhen is what the finish-time index needs to choose jobs to prune.
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
