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
	// SyncOff leaves flushing to the operating system.
	SyncOff
)

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

	io   sync.Mutex
	f    *os.File
	seg  uint64
	size int64

	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	spare    []byte
	appended uint64
	written  uint64
	synced   uint64
	err      error

	kick chan struct{}
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
	l := &Log{dir: dir, opt: opt, lock: lock, kick: make(chan struct{}, 1), done: make(chan struct{})}
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

func (l *Log) path(seg uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%s%016d%s", segPrefix, seg, segSuffix))
}

// Replay calls fn with every record in segments from onward, in order. A torn
// frame at the end of the newest segment is cut off; anywhere else it is an error.
func (l *Log) Replay(from uint64, fn func(rec []byte) error) error {
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

func (l *Log) replayOne(seg uint64, last bool, fn func([]byte) error) error {
	f, err := os.OpenFile(l.path(seg), os.O_RDWR, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	good, err := ReadFrames(f, fn)
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
	go l.flusher()
	return nil
}

func (l *Log) openSegment(seg uint64) error {
	f, err := os.OpenFile(l.path(seg), os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return err
	}
	l.f, l.seg, l.size = f, seg, 0
	return SyncDir(l.dir)
}

// Append queues rec and returns its sequence number for Wait.
func (l *Log) Append(rec []byte) (uint64, error) {
	l.mu.Lock()
	if l.err != nil {
		l.mu.Unlock()
		return 0, l.err
	}
	l.buf = AppendFrame(l.buf, rec)
	l.appended++
	lsn := l.appended
	l.mu.Unlock()
	select {
	case l.kick <- struct{}{}:
	default:
	}
	return lsn, nil
}

// Wait blocks until record lsn is written, and synced under SyncEach.
func (l *Log) Wait(lsn uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.err == nil && (l.written < lsn || (l.opt.Sync == SyncEach && l.synced < lsn)) {
		l.cond.Wait()
	}
	if l.written >= lsn && (l.opt.Sync != SyncEach || l.synced >= lsn) {
		return nil
	}
	return l.err
}

func (l *Log) flusher() {
	defer l.wg.Done()
	var tick <-chan time.Time
	if l.opt.Sync == SyncPeriodic {
		t := time.NewTicker(l.opt.Interval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-l.kick:
			l.io.Lock()
			l.fail(l.flushLocked(l.opt.Sync == SyncEach))
			l.io.Unlock()
		case <-tick:
			l.io.Lock()
			l.fail(l.flushLocked(true))
			l.io.Unlock()
		case <-l.done:
			return
		}
	}
}

// flushLocked writes queued frames and fsyncs when sync is set. The caller
// holds l.io.
func (l *Log) flushLocked(sync bool) error {
	l.mu.Lock()
	data, upto, synced := l.buf, l.appended, l.synced
	l.buf, l.spare = l.spare[:0], nil
	l.mu.Unlock()
	if len(data) > 0 {
		if _, err := l.f.Write(data); err != nil {
			return err
		}
		l.size += int64(len(data))
	}
	if sync && (len(data) > 0 || synced < upto) {
		if err := l.f.Sync(); err != nil {
			return err
		}
	}
	l.mu.Lock()
	l.written = upto
	if sync {
		l.synced = upto
	}
	l.spare = data
	l.cond.Broadcast()
	l.mu.Unlock()
	return nil
}

func (l *Log) fail(err error) {
	if err == nil {
		return
	}
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.cond.Broadcast()
	l.mu.Unlock()
}

// Size is how many bytes the current segment holds.
func (l *Log) Size() int64 {
	l.io.Lock()
	defer l.io.Unlock()
	return l.size
}

// Rotate syncs and closes the current segment and opens the next one, whose
// number it returns. Records appended before the call are in older segments.
func (l *Log) Rotate() (uint64, error) {
	l.io.Lock()
	defer l.io.Unlock()
	if err := l.flushLocked(true); err != nil {
		l.fail(err)
		return 0, err
	}
	if err := l.f.Close(); err != nil {
		return 0, err
	}
	if err := l.openSegment(l.seg + 1); err != nil {
		l.fail(err)
		return 0, err
	}
	return l.seg, nil
}

// Remove deletes the segments numbered below before.
func (l *Log) Remove(before uint64) error {
	segs, err := l.Segments()
	if err != nil {
		return err
	}
	for _, seg := range segs {
		if seg < before {
			if err := os.Remove(l.path(seg)); err != nil {
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
