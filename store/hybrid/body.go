package hybrid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kinoforge/runnel"
)

const bodyVersion = 1

// encodeBody packs the fields of j that stay out of memory: its unique key and
// dependencies first, so keyOf can read them without the rest, then the payload,
// result and everything else.
func encodeBody(j *runnel.Job) []byte {
	e := &enc{b: make([]byte, 0, 64+len(j.Payload)+len(j.Result))}
	e.byte(bodyVersion)
	e.str(j.UniqueKey)
	e.int(int64(len(j.DependsOn)))
	for _, d := range j.DependsOn {
		e.str(d)
	}
	e.bytes(j.Payload)
	e.bytes(j.Result)
	e.str(j.Handle)
	e.str(j.CallbackURL)
	for _, d := range []runnel.Duration{j.StartTimeout, j.Timeout, j.UniqueFor, j.Backoff.Base, j.Backoff.Max} {
		e.int(int64(d))
	}
	e.float(j.Backoff.Factor)
	e.float(j.Backoff.Jitter)
	e.int(int64(j.Fallback))
	e.int(int64(len(j.Fallbacks)))
	for _, f := range j.Fallbacks {
		e.str(f.Queue)
		e.str(f.Kind)
		e.str(f.Pool)
	}
	e.int(int64(len(j.Meta)))
	for k, v := range j.Meta {
		e.str(k)
		e.str(v)
	}
	e.int(int64(len(j.Errors)))
	for _, a := range j.Errors {
		e.int(int64(a.Attempt))
		e.str(a.Queue)
		e.str(a.Kind)
		e.int(nanos(a.At))
		e.str(a.Error)
	}
	return e.b
}

func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// keyOf reads only the unique key and dependencies of a body.
func keyOf(b []byte) (string, []string, error) {
	d := &dec{b: b}
	if v := d.byte(); v != bodyVersion && d.err == nil {
		return "", nil, fmt.Errorf("hybrid: body version %d", v)
	}
	key := d.str()
	var deps []string
	for range count(d) {
		deps = append(deps, d.str())
	}
	return key, deps, d.err
}

// count reads a length, refusing more items than bytes left.
func count(d *dec) int {
	n := d.int()
	if n < 0 || n > int64(len(d.b)) {
		if d.err == nil {
			d.err = errShort
		}
		return 0
	}
	return int(n)
}

func raw(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}

func decodeBody(b []byte, j *runnel.Job) error {
	key, deps, err := keyOf(b)
	if err != nil {
		return err
	}
	j.UniqueKey, j.DependsOn = key, deps
	d := &dec{b: b}
	d.byte()
	d.str()
	for range count(d) {
		d.str()
	}
	j.Payload, j.Result = raw(d.bytes()), raw(d.bytes())
	j.Handle, j.CallbackURL = d.str(), d.str()
	for _, p := range []*runnel.Duration{&j.StartTimeout, &j.Timeout, &j.UniqueFor, &j.Backoff.Base, &j.Backoff.Max} {
		*p = runnel.Duration(d.int())
	}
	j.Backoff.Factor, j.Backoff.Jitter = d.float(), d.float()
	j.Fallback = int(d.int())
	for range count(d) {
		j.Fallbacks = append(j.Fallbacks, runnel.Target{Queue: d.str(), Kind: d.str(), Pool: d.str()})
	}
	if n := count(d); n > 0 {
		j.Meta = make(map[string]string, n)
		for range n {
			k := d.str()
			j.Meta[k] = d.str()
		}
	}
	for range count(d) {
		a := runnel.AttemptError{Attempt: int(d.int()), Queue: d.str(), Kind: d.str()}
		a.At, a.Error = fromNanos(d.int()), d.str()
		j.Errors = append(j.Errors, a)
	}
	if d.err != nil {
		return fmt.Errorf("hybrid: job %s body: %w", j.ID, d.err)
	}
	return nil
}
