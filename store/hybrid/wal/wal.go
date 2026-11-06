// Package wal is an append-only, checksummed log split into numbered segment
// files. Commits from many goroutines share one write and one fsync (group
// commit). It knows nothing about what the records mean.
package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	segPrefix       = "wal-"
	segSuffix       = ".log"
	lockName        = "LOCK"
	defaultInterval = 100 * time.Millisecond
	filePerm        = 0o600
	dirPerm         = 0o750
)

// SyncMode says when written records reach the disk.
type SyncMode int

const (
	// SyncEach makes every commit wait for an fsync. Concurrent commits share it.
	SyncEach SyncMode = iota
	// SyncPeriodic fsyncs every Interval. A crash of the machine, not of the
	// process, can lose the last Interval of commits.
	SyncPeriodic
	// SyncOff answers before writing: records are written every Interval and
	// flushed by the operating system. Any crash can lose the last Interval.
	SyncOff
)

var syncModes = map[string]SyncMode{"": SyncEach, "each": SyncEach, "periodic": SyncPeriodic, "off": SyncOff}

// ParseSync reads a sync mode by name: each, periodic or off; empty is each.
func ParseSync(name string) (SyncMode, error) {
	m, ok := syncModes[name]
	if !ok {
		return 0, fmt.Errorf("wal: sync %q must be each, periodic or off", name)
	}
	return m, nil
}

var (
	// ErrLocked means another process has the directory open.
	ErrLocked = errors.New("wal: directory is in use by another process")
	// ErrClosed means the log was closed.
	ErrClosed = errors.New("wal: closed")
)

type Options struct {
	Sync     SyncMode
	Interval time.Duration
}

// Log is a write-ahead log in one directory.
type Log struct {
	dir  string
	opt  Options
	lock *os.File

	// io guards writes to f and swapping f; syncMu keeps f open while a
	// background fsync runs, so that fsync never blocks writers.
	io     sync.Mutex
	syncMu sync.Mutex
	f      *os.File
	seg    uint64
	size   atomic.Int64

	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	spare    []byte
	end      int64
	appended uint64
	written  uint64
	synced   uint64
	writing  bool
	err      error

	done chan struct{}
	wg   sync.WaitGroup
}

// Open locks dir, creating it if needed. Call Replay, then Start, before Append.
func Open(dir string, opt Options) (*Log, error) {
	if opt.Interval <= 0 {
		opt.Interval = defaultInterval
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}
	if err := lockFile(lock); err != nil {
		_ = lock.Close()
		return nil, err
	}
	l := &Log{dir: dir, opt: opt, lock: lock, done: make(chan struct{})}
	l.cond = sync.NewCond(&l.mu)
	return l, nil
}

func (l *Log) Dir() string { return l.dir }

// Segments lists the segment numbers in the directory, oldest first.
func (l *Log) Segments() ([]uint64, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, segPrefix), segSuffix), 10, 64)
		if err == nil {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out, nil
}

// Path is the file of segment seg.
func (l *Log) Path(seg uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%s%016d%s", segPrefix, seg, segSuffix))
}

// Replay calls fn with every record in segments from onward, in order, with
// the segment and offset where the record starts. A torn frame at the end of the
// newest segment is cut off; anywhere else it is an error.
func (l *Log) Replay(from uint64, fn func(seg uint64, off int64, rec []byte) error) error {
	segs, err := l.Segments()
	if err != nil {
		return err
	}
	for i, seg := range segs {
		if seg < from {
			continue
		}
		if err := l.replayOne(seg, i == len(segs)-1, fn); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) replayOne(seg uint64, last bool, fn func(uint64, int64, []byte) error) error {
	f, err := os.OpenFile(l.Path(seg), os.O_RDWR, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	good, err := ReadFrames(f, func(off int64, rec []byte) error { return fn(seg, off, rec) })
	if errors.Is(err, ErrTorn) && last {
		if err := f.Truncate(good); err != nil {
			return err
		}
		return f.Sync()
	}
	if err != nil {
		return fmt.Errorf("wal: segment %d: %w", seg, err)
	}
	return nil
}

// Start opens a new segment after the newest one and begins accepting appends.
func (l *Log) Start() error {
	segs, err := l.Segments()
	if err != nil {
		return err
	}
	next := uint64(1)
	if len(segs) > 0 {
		next = segs[len(segs)-1] + 1
	}
	if err := l.openSegment(next); err != nil {
		return err
	}
	l.wg.Add(1)
	go l.background()
	return nil
}

func (l *Log) openSegment(seg uint64) error {
	f, err := os.OpenFile(l.Path(seg), os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.f, l.seg, l.end = f, seg, 0
	l.mu.Unlock()
	l.size.Store(0)
	return SyncDir(l.dir)
}

// Rotate starts the next segment and returns its number. Records appended
// before the switch go to the older segments, which are written and synced
// before it returns; appends go on meanwhile, into the new segment.
func (l *Log) Rotate() (uint64, error) {
	l.io.Lock()
	defer l.io.Unlock()
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	next, err := os.OpenFile(l.Path(l.seg+1), os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	old, data, upto := l.f, l.buf, l.appended
	l.buf, l.spare = l.spare[:0], nil
	l.f, l.seg, l.end = next, l.seg+1, 0
	seg := l.seg
	l.mu.Unlock()
	l.size.Store(0)
	if err := finish(old, data); err != nil {
		l.fail(err)
		return 0, err
	}
	l.mu.Lock()
	l.written, l.synced = max(l.written, upto), max(l.synced, upto)
	l.spare = data
	l.cond.Broadcast()
	l.mu.Unlock()
	return seg, SyncDir(l.dir)
}

// finish writes the last records of a segment, syncs and closes it.
func finish(f *os.File, data []byte) error {
	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			return err
		}
	}
	return errors.Join(f.Sync(), f.Close())
}

// Sync writes and syncs every record appended so far. Writers carry on
// during the fsync.
func (l *Log) Sync() error {
	l.io.Lock()
	err := l.flushLocked(false)
	l.io.Unlock()
	if err == nil {
		err = l.syncWritten()
	}
	return err
}

// Remove deletes the segments numbered below before.
func (l *Log) Remove(before uint64) error {
	segs, err := l.Segments()
	if err != nil {
		return err
	}
	for _, seg := range segs {
		if seg < before {
			if err := os.Remove(l.Path(seg)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close flushes and syncs what is queued and releases the directory.
func (l *Log) Close() error {
	var err error
	if l.f != nil {
		close(l.done)
		l.wg.Wait()
		l.io.Lock()
		err = l.flushLocked(true)
		err = errors.Join(err, l.f.Close())
		l.io.Unlock()
	}
	l.fail(ErrClosed)
	return errors.Join(err, l.lock.Close())
}

// SyncDir makes renames and removals in dir durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}
