package hybrid

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/kinoforge/runnel"
)

var errShort = errors.New("hybrid: record cut short")

type enc struct{ b []byte }

func (e *enc) byte(v byte)      { e.b = append(e.b, v) }
func (e *enc) int(v int64)      { e.b = binary.AppendVarint(e.b, v) }
func (e *enc) time(t time.Time) { e.int(runnel.Millis(t)) }
func (e *enc) float(v float64)  { e.b = binary.LittleEndian.AppendUint64(e.b, math.Float64bits(v)) }

func (e *enc) bytes(v []byte) {
	e.b = binary.AppendUvarint(e.b, uint64(len(v)))
	e.b = append(e.b, v...)
}

func (e *enc) str(v string) {
	e.b = binary.AppendUvarint(e.b, uint64(len(v)))
	e.b = append(e.b, v...)
}

// dec reads what enc wrote. The first failure sticks in err and later reads
// return zero values.
type dec struct {
	b   []byte
	err error
}

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

func (d *dec) time() time.Time { return runnel.FromMillis(d.int()) }

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

func encodeJob(e *enc, j *runnel.Job) error {
	data, err := j.EncodeData()
	if err != nil {
		return err
	}
	e.str(j.ID)
	e.str(j.Queue)
	e.str(j.Kind)
	e.str(string(j.State))
	e.int(int64(j.Priority))
	e.str(j.Tenant)
	e.str(j.Resource)
	e.int(int64(j.Attempt))
	e.int(int64(j.MaxAttempts))
	for _, t := range []time.Time{j.RunAt, j.WaitingSince, j.StartBy, j.LeaseUntil, j.CreatedAt, j.UpdatedAt, j.StartedAt, j.FinishedAt} {
		e.time(t)
	}
	e.str(j.Worker)
	e.int(j.Version)
	e.int(int64(j.Pending))
	e.str(j.UniqueKey)
	e.bytes(data)
	return nil
}

func decodeJob(d *dec, ns string) (*runnel.Job, error) {
	j := &runnel.Job{Namespace: ns}
	j.ID, j.Queue, j.Kind, j.State = d.str(), d.str(), d.str(), runnel.State(d.str())
	j.Priority = int(d.int())
	j.Tenant, j.Resource = d.str(), d.str()
	j.Attempt, j.MaxAttempts = int(d.int()), int(d.int())
	for _, t := range []*time.Time{&j.RunAt, &j.WaitingSince, &j.StartBy, &j.LeaseUntil, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt} {
		*t = d.time()
	}
	j.Worker = d.str()
	j.Version = d.int()
	j.Pending = int(d.int())
	j.UniqueKey = d.str()
	data := d.bytes()
	if d.err != nil {
		return nil, d.err
	}
	return j, j.DecodeData(data)
}

// normalize rounds a job's times to milliseconds, the precision every store
// keeps, so a job reads back the same before and after a restart.
func normalize(j *runnel.Job) {
	for _, t := range []*time.Time{&j.RunAt, &j.WaitingSince, &j.StartBy, &j.LeaseUntil, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt} {
		*t = runnel.FromMillis(runnel.Millis(*t))
	}
}
