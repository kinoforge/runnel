package hybrid

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/cold"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

// ErrBroken means a write to disk failed. Memory may hold changes the disk does
// not, so the store refuses further calls; reopen it to recover from disk.
var ErrBroken = errors.New("hybrid: store stopped after a disk error")

// Store keeps a small record of every job in memory, indexed for claiming, and
// logs each change before the call returns. Job bodies (payload, result, meta,
// errors) are read back from the log and snapshots once written.
// One process owns the directory; serve it over HTTP to share it.
type Store struct {
	opt   Options
	log   *wal.Log
	files *files
	// snapBytes is the size of the newest snapshot; the log grows at least
	// that much before the next one, so rewriting stays in proportion.
	snapBytes atomic.Int64
	// cold holds finished jobs once they leave memory; coldMu keeps moving
	// them there apart from pruning them.
	cold   *cold.Store
	coldMu sync.Mutex
	// waitMu keeps moving waiting jobs to and from the cold store apart from
	// each other and from deleting the jobs a call brought back.
	waitMu sync.Mutex
	// hotJobs is how many waiting jobs memory holds.
	hotJobs int64
	// hot counts the waiting jobs in memory.
	hot atomic.Int64

	mu     sync.RWMutex
	spaces map[string]*space

	hub    hub
	failed atomic.Bool
	errMu  sync.Mutex
	err    error
	snapMu sync.Mutex
	done   chan struct{}
	wg     sync.WaitGroup
}

// Memory returns a store that keeps nothing on disk.
func Memory() *Store {
	return &Store{spaces: map[string]*space{}, done: make(chan struct{})}
}

// Open loads the store in opt.Dir, replaying its snapshot and log.
func Open(opt Options) (*Store, error) {
	s := Memory()
	if opt.Dir == "" {
		return s, nil
	}
	s.opt = opt.withDefaults()
	s.hotJobs = s.opt.hotBudget()
	opt = s.opt
	s.files = &files{path: s.filePath, open: map[uint32]*os.File{}}
	l, err := wal.Open(opt.Dir, wal.Options{Sync: opt.Sync, Interval: opt.SyncInterval})
	if err != nil {
		return nil, err
	}
	s.log = l
	if err := s.prepareDir(); err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := s.load(); err != nil {
		s.files.close()
		_ = l.Close()
		if s.cold != nil {
			_ = s.cold.Close()
		}
		return nil, err
	}
	s.wg.Add(3)
	go s.checkpointer()
	go s.retirer()
	go s.spiller()
	return s, nil
}

func (s *Store) prepareDir() error {
	if err := checkFormat(s.opt.Dir); err != nil {
		return err
	}
	return removeStaleBackups(s.opt.Dir)
}

// load opens the cold store, replays the snapshot and log into memory, starts
// the log and moves every finished job to the cold store.
func (s *Store) load() error {
	c, err := cold.Open(filepath.Join(s.opt.Dir, coldDir), s.opt.Cold)
	if err != nil {
		return err
	}
	s.cold = c
	counts, err := c.Counts()
	if err != nil {
		return err
	}
	for k, n := range counts {
		sp := s.space(k.NS)
		sp.addCold(map[cold.Count]int64{k: n})
		if sp.coldMax.Load() == nil {
			top, ok, err := c.MaxID(k.NS)
			if err != nil {
				return err
			}
			if ok {
				sp.noteCold(uid(top))
			}
		}
	}
	lasts, err := c.LastEvents()
	if err != nil {
		return err
	}
	for ns, seq := range lasts {
		sp := s.space(ns)
		sp.coldSeq, sp.seq = seq, max(sp.seq, seq)
	}
	if err := s.recover(); err != nil {
		return err
	}
	if err := s.loadWaiting(); err != nil {
		return err
	}
	if err := s.log.Start(); err != nil {
		return err
	}
	return s.retire(math.MaxInt64, true)
}

func (s *Store) space(ns string) *space {
	s.mu.RLock()
	sp := s.spaces[ns]
	s.mu.RUnlock()
	if sp != nil {
		return sp
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sp = s.spaces[ns]
	if sp == nil {
		sp = newSpace(ns)
		sp.total = &s.hot
		s.spaces[ns] = sp
	}
	return sp
}

func (s *Store) broken() error {
	if !s.failed.Load() {
		return nil
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *Store) fail(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = errors.Join(ErrBroken, err)
		s.failed.Store(true)
	}
	s.errMu.Unlock()
}

// write runs fn on namespace ns under its lock and logs the ops fn made. It
// returns once the log holds them, after releasing the lock so other calls can
// share the same fsync.
func (s *Store) write(ns string, fn func(*tx) error) error { return s.commit(ns, true, fn) }

// lease is write for claims and lease extensions, which return without waiting
// for the disk. The log is ordered, so the finish that follows a claim makes the
// claim durable too; a crash before then only lets the job run again, as
// at-least-once delivery allows.
func (s *Store) lease(ns string, fn func(*tx) error) error { return s.commit(ns, false, fn) }

func (s *Store) commit(ns string, wait bool, fn func(*tx) error) error {
	if err := s.broken(); err != nil {
		return err
	}
	sp := s.space(ns)
	c := &call{fn: fn, turn: make(chan bool, 1)}
	s.run(sp, c)
	if c.logErr == nil && c.pos.LSN > 0 && wait {
		c.logErr = s.log.Wait(c.pos.LSN)
	}
	if c.logErr == nil && len(c.t.unretired) > 0 {
		s.coldMu.Lock()
		s.waitMu.Lock()
		c.logErr = s.unretire(sp, c.t.unretired)
		s.waitMu.Unlock()
		s.coldMu.Unlock()
	}
	if c.logErr != nil {
		s.fail(c.logErr)
		return s.broken()
	}
	s.hub.notify(ns, c.t.wake, c.t.ends)
	return c.err
}

// place points the jobs given new bodies by t at where the record puts them.
// Each body leaves the cache once the record is written; after that it is
// read from disk, which the OS page cache keeps in memory while it can.
func (s *Store) place(sp *space, t *tx, pos wal.Pos) {
	sp.cache.evict(s.log.Written(), sp.bodyRef)
	for _, b := range t.bodies {
		if i, ok := sp.idx[b.id]; ok {
			h := sp.jobs.at(i)
			h.body = ref{file: uint32(pos.Seg), n: uint32(b.n), off: pos.Off + int64(b.pos)}
			sp.cache.queue = append(sp.cache.queue, leaving{id: b.id, at: h.body, lsn: pos.LSN})
		}
	}
}

func (s *Store) filePath(id uint32) string {
	if id&snapBit != 0 {
		return s.snapPath(uint64(id &^ snapBit))
	}
	return s.log.Path(uint64(id))
}

// read runs fn on namespace ns under its lock.
func (s *Store) read(ns string, fn func(*space) error) error {
	if err := s.broken(); err != nil {
		return err
	}
	sp := s.space(ns)
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return fn(sp)
}

func (s *Store) Migrate(context.Context) error { return s.broken() }

// Close stops background work and flushes the log.
func (s *Store) Close() error {
	select {
	case <-s.done:
		return nil
	default:
	}
	close(s.done)
	s.wg.Wait()
	if s.log == nil {
		return nil
	}
	err := s.log.Close()
	s.files.close()
	return errors.Join(err, s.cold.Close())
}

// Subscribe signals when jobs in ns may be ready to claim.
func (s *Store) Subscribe(ctx context.Context, ns string) (<-chan struct{}, error) {
	return s.hub.subscribe(ctx, ns), nil
}

// Ended sends the id of each job in ns that reaches a final state.
func (s *Store) Ended(ctx context.Context, ns string) (<-chan string, error) {
	return s.hub.ended(ctx, ns), nil
}

var (
	_ runnel.Store      = (*Store)(nil)
	_ runnel.Waker      = (*Store)(nil)
	_ runnel.EndWatcher = (*Store)(nil)
)
