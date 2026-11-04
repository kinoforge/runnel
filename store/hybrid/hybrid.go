package hybrid

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const (
	defaultSnapshotBytes = 64 << 20
	checkEvery           = time.Second
)

// ErrBroken means a write to disk failed. Memory may hold changes the disk does
// not, so the store refuses further calls; reopen it to recover from disk.
var ErrBroken = errors.New("hybrid: store stopped after a disk error")

// Options configures a store kept on disk. An empty Dir keeps it in memory only.
type Options struct {
	Dir  string
	Sync wal.SyncMode
	// SyncInterval is how often SyncPeriodic fsyncs; default 100ms.
	SyncInterval time.Duration
	// SnapshotBytes is the log size that triggers a snapshot; default 64 MiB.
	SnapshotBytes int64
}

// Store keeps every job in memory, indexed for claiming, and logs each change
// before the call returns. One process owns the directory; serve it over HTTP
// to share it.
type Store struct {
	opt Options
	log *wal.Log

	gate   sync.RWMutex
	mu     sync.Mutex
	spaces map[string]*space

	hub    hub
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
	if opt.SnapshotBytes <= 0 {
		opt.SnapshotBytes = defaultSnapshotBytes
	}
	s.opt = opt
	l, err := wal.Open(opt.Dir, wal.Options{Sync: opt.Sync, Interval: opt.SyncInterval})
	if err != nil {
		return nil, err
	}
	s.log = l
	if err := s.recover(); err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := l.Start(); err != nil {
		_ = l.Close()
		return nil, err
	}
	s.wg.Add(1)
	go s.checkpointer()
	return s, nil
}

func (s *Store) space(ns string) *space {
	s.mu.Lock()
	defer s.mu.Unlock()
	sp := s.spaces[ns]
	if sp == nil {
		sp = newSpace(ns)
		s.spaces[ns] = sp
	}
	return sp
}

func (s *Store) broken() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *Store) fail(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = errors.Join(ErrBroken, err)
	}
	s.errMu.Unlock()
}

// write runs fn on namespace ns under its lock and logs the ops fn made. It
// returns once the log holds them, after releasing the lock so other calls can
// share the same fsync.
func (s *Store) write(ns string, fn func(*tx) error) error {
	if err := s.broken(); err != nil {
		return err
	}
	s.gate.RLock()
	sp := s.space(ns)
	sp.mu.Lock()
	t := newTx(sp)
	err := fn(t)
	var lsn uint64
	logErr := t.err
	if logErr == nil && s.log != nil && t.changed() {
		lsn, logErr = s.log.Append(t.rec.b)
	}
	sp.mu.Unlock()
	s.gate.RUnlock()
	if logErr == nil && lsn > 0 {
		logErr = s.log.Wait(lsn)
	}
	if logErr != nil {
		s.fail(logErr)
		return s.broken()
	}
	s.hub.notify(ns, t.wake, t.ends)
	return err
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
	return s.log.Close()
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
