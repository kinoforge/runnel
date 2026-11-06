// Package cold keeps jobs on disk in Pebble, so they take no memory: finished
// jobs however long they are kept, and waiting jobs past what memory holds.
// Each job is one record keyed by namespace and id, with an index entry by
// state, to list the newest first. Finished jobs are also indexed by finish
// time, to prune the oldest first; waiting jobs in claim order, to bring the
// next ones back. Counts per queue and state are written in the same batch as
// the jobs, so they always match them.
//
// Pebble's own write-ahead log is off: the caller logs every change itself
// and calls Flush before it drops its log, so a crash loses nothing the
// caller cannot replay.
package cold

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
)

// Options sizes the store; fields left at zero take their value from
// DefaultOptions.
type Options struct {
	CacheBytes    int64
	MemTableBytes int64
	BloomBits     int
}

// DefaultOptions holds the value of every option left at zero.
var DefaultOptions = Options{CacheBytes: 16 << 20, MemTableBytes: 8 << 20, BloomBits: 10}

// WithDefaults fills the options left at zero from DefaultOptions.
func (o Options) WithDefaults() Options {
	if o.CacheBytes <= 0 {
		o.CacheBytes = DefaultOptions.CacheBytes
	}
	if o.MemTableBytes <= 0 {
		o.MemTableBytes = DefaultOptions.MemTableBytes
	}
	if o.BloomBits <= 0 {
		o.BloomBits = DefaultOptions.BloomBits
	}
	return o
}

// ID is a job id in its 16 raw bytes.
type ID = [16]byte

// Waiting is the state code of a waiting job; every other state but Blocked,
// which never comes here, is final.
const Waiting uint8 = 2

// Job is a job as the cold store keeps it: the fields its indexes use, and
// the job itself, encoded by the caller. Prio, RunAt, StartBy and Ready only
// matter for a waiting job; Ready puts it in claim order rather than in run
// time order.
type Job struct {
	NS                  string
	ID                  ID
	State               uint8
	Finished            int64
	Prio                int32
	RunAt, StartBy      int64
	Ready               bool
	Queue, Kind, Tenant string
	Data                []byte
}

func (j *Job) waiting() bool { return j.State == Waiting }

// Count is a namespace's number of jobs in one queue and state, or, with
// Tenant set, its number of waiting jobs of one tenant in one queue.
type Count struct {
	NS, Queue string
	State     uint8
	Tenant    string
}

// Store is a cold store. Reads run concurrently; writes take turns, since
// each one reads the records it replaces to keep the counts right.
type Store struct {
	db *pebble.DB
	mu sync.Mutex
}

// Open opens or creates the store in dir.
func Open(dir string, o Options) (*Store, error) {
	o = o.WithDefaults()
	c := pebble.NewCache(o.CacheBytes)
	defer c.Unref()
	opt := &pebble.Options{
		Cache:              c,
		MemTableSize:       uint64(o.MemTableBytes),
		DisableWAL:         true,
		FormatMajorVersion: pebble.FormatNewest,
		Logger:             quiet{},
	}
	// Bloom filters let a lookup of an id that is not stored, as every new
	// job's is, skip the files without reading them.
	for i := range opt.Levels {
		opt.Levels[i].FilterPolicy = bloom.FilterPolicy(o.BloomBits)
	}
	db, err := pebble.Open(dir, opt)
	if err != nil {
		return nil, fmt.Errorf("cold: %w", err)
	}
	return &Store{db: db}, nil
}

// Put writes jobs, replacing any stored under the same ids, and returns how
// the counts changed.
func (s *Store) Put(jobs []Job) (map[Count]int64, error) { return s.put(jobs, true) }

// Add writes jobs the caller knows are not stored, sparing the lookup Put
// makes for each.
func (s *Store) Add(jobs []Job) (map[Count]int64, error) { return s.put(jobs, false) }

func (s *Store) put(jobs []Job, replace bool) (map[Count]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewIndexedBatch()
	defer b.Close()
	delta := map[Count]int64{}
	for i := range jobs {
		j := &jobs[i]
		if replace {
			if err := drop(b, j.NS, j.ID, delta); err != nil {
				return nil, err
			}
		}
		rec := encodeJob(j)
		err := errors.Join(
			b.Set(jobKey(j.NS, j.ID), rec, nil),
			b.Set(stateKey(j.NS, j.State, j.ID), encodeNames(j), nil),
			index(b, j, rec))
		if err != nil {
			return nil, err
		}
		count(j, delta, 1)
	}
	return delta, commit(b, delta)
}

// Delete removes the jobs with ids, if stored, and returns how the counts
// changed.
func (s *Store) Delete(ns string, ids []ID) (map[Count]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewIndexedBatch()
	defer b.Close()
	delta := map[Count]int64{}
	for _, id := range ids {
		if err := drop(b, ns, id, delta); err != nil {
			return nil, err
		}
	}
	return delta, commit(b, delta)
}

// Remove deletes jobs as read from the store, sparing the lookup Delete makes
// for each; the caller ensures they did not change since.
func (s *Store) Remove(jobs []Job) (map[Count]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewIndexedBatch()
	defer b.Close()
	delta := map[Count]int64{}
	for i := range jobs {
		j := &jobs[i]
		count(j, delta, -1)
		err := errors.Join(b.Delete(jobKey(j.NS, j.ID), nil), b.Delete(stateKey(j.NS, j.State, j.ID), nil), unindex(b, j))
		if err != nil {
			return nil, err
		}
	}
	return delta, commit(b, delta)
}

// Get returns the job stored under id.
func (s *Store) Get(ns string, id ID) (Job, bool, error) {
	v, closer, err := s.db.Get(jobKey(ns, id))
	if errors.Is(err, pebble.ErrNotFound) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	defer closer.Close()
	j, err := decodeJob(ns, id, v, true)
	return j, err == nil, err
}

// Meta returns the job stored under id without its data.
func (s *Store) Meta(ns string, id ID) (Job, bool, error) {
	v, closer, err := s.db.Get(jobKey(ns, id))
	if errors.Is(err, pebble.ErrNotFound) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	defer closer.Close()
	j, err := decodeJob(ns, id, v, false)
	return j, err == nil, err
}

// Counts returns every count in the store.
func (s *Store) Counts() (map[Count]int64, error) {
	out := map[Count]int64{}
	for _, kind := range []byte{kCount, kTenant} {
		it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{kind}, UpperBound: []byte{kind + 1}})
		if err != nil {
			return nil, err
		}
		for it.First(); it.Valid(); it.Next() {
			c, ok := decodeCountKey(it.Key())
			if !ok {
				return nil, errors.Join(errBadKey, it.Close())
			}
			out[c] = decodeInt(it.Value())
		}
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Flush makes every write so far durable.
func (s *Store) Flush() error { return s.db.Flush() }

// Close flushes and closes the store; with Pebble's log off, a close without
// a flush would drop the writes still in memory.
func (s *Store) Close() error { return errors.Join(s.db.Flush(), s.db.Close()) }

// drop deletes the record stored under id and its index entries, noting the
// count it leaves.
func drop(b *pebble.Batch, ns string, id ID, delta map[Count]int64) error {
	key := jobKey(ns, id)
	v, closer, err := b.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	old, err := decodeJob(ns, id, v, false)
	_ = closer.Close()
	if err != nil {
		return err
	}
	count(&old, delta, -1)
	return errors.Join(b.Delete(key, nil), b.Delete(stateKey(ns, old.State, id), nil), unindex(b, &old))
}

// count adds d to the counts j is in.
func count(j *Job, delta map[Count]int64, d int64) {
	delta[Count{NS: j.NS, Queue: j.Queue, State: j.State}] += d
	if j.waiting() && j.Tenant != "" {
		delta[Count{NS: j.NS, Queue: j.Queue, State: j.State, Tenant: j.Tenant}] += d
	}
}

// index writes the entries that order j: by finish time when finished, in
// claim or run time order when waiting. A waiting job's entry holds its
// record rec too, so jobs are brought back in order without a lookup each.
func index(b *pebble.Batch, j *Job, rec []byte) error {
	if !j.waiting() {
		return b.Set(timeKey(j.NS, j.Finished, j.ID), encodeWhen(j), nil)
	}
	err := b.Set(orderKey(j), rec, nil)
	if j.StartBy != 0 {
		err = errors.Join(err, b.Set(deadlineKey(j.NS, j.StartBy, j.ID), nil, nil))
	}
	return err
}

func unindex(b *pebble.Batch, j *Job) error {
	if !j.waiting() {
		return b.Delete(timeKey(j.NS, j.Finished, j.ID), nil)
	}
	err := b.Delete(orderKey(j), nil)
	if j.StartBy != 0 {
		err = errors.Join(err, b.Delete(deadlineKey(j.NS, j.StartBy, j.ID), nil))
	}
	return err
}

// commit adds delta to the stored counts and commits b.
func commit(b *pebble.Batch, delta map[Count]int64) error {
	for c, d := range delta {
		if d == 0 {
			continue
		}
		key := c.key()
		n := d
		if v, closer, err := b.Get(key); err == nil {
			n += decodeInt(v)
			_ = closer.Close()
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		var err error
		if n == 0 {
			err = b.Delete(key, nil)
		} else {
			err = b.Set(key, encodeInt(n), nil)
		}
		if err != nil {
			return err
		}
	}
	return b.Commit(pebble.NoSync)
}

// quiet keeps Pebble's routine messages out of the logs.
type quiet struct{}

func (quiet) Infof(string, ...any)              {}
func (quiet) Errorf(format string, args ...any) { slog.Error(fmt.Sprintf("cold: "+format, args...)) }
func (quiet) Fatalf(format string, args ...any) { panic(fmt.Sprintf("cold: "+format, args...)) }
