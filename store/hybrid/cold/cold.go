// Package cold keeps finished jobs on disk in Pebble, so they take no memory
// however long they are kept. Each job is one record keyed by namespace and
// id, with two index entries: by state, to list the newest first, and by finish
// time, to prune the oldest first. Counts per queue and state are written in
// the same batch as the jobs, so they always match them.
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

const (
	cacheBytes    = 16 << 20
	memTableBytes = 4 << 20
	bloomBits     = 10
)

// ID is a job id in its 16 raw bytes.
type ID = [16]byte

// Job is a finished job as the cold store keeps it: the fields its indexes
// use, and the job itself, encoded by the caller.
type Job struct {
	NS                  string
	ID                  ID
	State               uint8
	Finished            int64
	Queue, Kind, Tenant string
	Data                []byte
}

// Count is a namespace's number of jobs in one queue and state.
type Count struct {
	NS, Queue string
	State     uint8
}

// Store is a cold store. Reads run concurrently; writes take turns, since
// each one reads the records it replaces to keep the counts right.
type Store struct {
	db *pebble.DB
	mu sync.Mutex
}

// Open opens or creates the store in dir.
func Open(dir string) (*Store, error) {
	c := pebble.NewCache(cacheBytes)
	defer c.Unref()
	opt := &pebble.Options{
		Cache:              c,
		MemTableSize:       memTableBytes,
		DisableWAL:         true,
		FormatMajorVersion: pebble.FormatNewest,
		Logger:             quiet{},
	}
	// Bloom filters let a lookup of an id that is not stored, as every new
	// job's is, skip the files without reading them.
	for i := range opt.Levels {
		opt.Levels[i].FilterPolicy = bloom.FilterPolicy(bloomBits)
	}
	db, err := pebble.Open(dir, opt)
	if err != nil {
		return nil, fmt.Errorf("cold: %w", err)
	}
	return &Store{db: db}, nil
}

// Put writes jobs, replacing any stored under the same ids, and returns how
// the counts changed.
func (s *Store) Put(jobs []Job) (map[Count]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewIndexedBatch()
	defer b.Close()
	delta := map[Count]int64{}
	for i := range jobs {
		j := &jobs[i]
		if err := drop(b, j.NS, j.ID, delta); err != nil {
			return nil, err
		}
		err := errors.Join(
			b.Set(jobKey(j.NS, j.ID), encodeJob(j), nil),
			b.Set(stateKey(j.NS, j.State, j.ID), encodeNames(j), nil),
			b.Set(timeKey(j.NS, j.Finished, j.ID), encodeWhen(j), nil))
		if err != nil {
			return nil, err
		}
		delta[Count{j.NS, j.Queue, j.State}]++
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
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{kCount}, UpperBound: []byte{kCount + 1}})
	if err != nil {
		return nil, err
	}
	out := map[Count]int64{}
	for it.First(); it.Valid(); it.Next() {
		c, ok := decodeCountKey(it.Key())
		if !ok {
			return nil, errors.Join(errBadKey, it.Close())
		}
		out[c] = decodeInt(it.Value())
	}
	return out, errors.Join(it.Error(), it.Close())
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
	delta[Count{ns, old.Queue, old.State}]--
	return errors.Join(b.Delete(key, nil), b.Delete(stateKey(ns, old.State, id), nil),
		b.Delete(timeKey(ns, old.Finished, id), nil))
}

// commit adds delta to the stored counts and commits b.
func commit(b *pebble.Batch, delta map[Count]int64) error {
	for c, d := range delta {
		if d == 0 {
			continue
		}
		key := countKey(c)
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
