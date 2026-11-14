package wal

import "time"

const offBacklog = 8 << 20

// Pos says where an appended record will be: its sequence number for Wait, and
// the segment and offset its bytes start at once written.
type Pos struct {
	LSN uint64
	Seg uint64
	Off int64
}

// Append queues rec and returns its position. It never touches the disk, so
// callers may hold their own locks around it.
func (l *Log) Append(rec []byte) (Pos, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return Pos{}, l.err
	}
	l.buf = AppendFrame(l.buf, rec)
	l.appended++
	p := Pos{LSN: l.appended, Seg: l.seg, Off: l.end + headerLen}
	l.end += int64(headerLen + len(rec))
	return p, nil
}

// Written is the sequence number of the last record handed to the file, which
// readers of the segment can now see.
func (l *Log) Written() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.written
}

func (l *Log) reached(lsn uint64) bool {
	return l.written >= lsn && (l.opt.Sync != SyncEach || l.synced >= lsn)
}

// Wait blocks until record lsn is written, and synced under SyncEach.
// Concurrent callers share one write and one fsync (group commit).
func (l *Log) Wait(lsn uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.opt.Sync == SyncOff && len(l.buf) < offBacklog && l.err == nil {
		return nil
	}
	for !l.reached(lsn) {
		if l.err != nil {
			return l.err
		}
		if l.writing {
			l.cond.Wait()
			continue
		}
		l.writing = true
		l.mu.Unlock()
		l.io.Lock()
		err := l.flushLocked(l.opt.Sync == SyncEach)
		l.io.Unlock()
		l.mu.Lock()
		l.writing = false
		if err != nil && l.err == nil {
			l.err = err
		}
		l.cond.Broadcast()
	}
	return nil
}

// The caller holds l.io.
func (l *Log) flushLocked(sync bool) error {
	l.mu.Lock()
	data, upto := l.buf, l.appended
	l.buf, l.spare = l.spare[:0], nil
	l.mu.Unlock()
	if len(data) > 0 {
		if _, err := l.f.Write(data); err != nil {
			return err
		}
		l.size.Add(int64(len(data)))
	}
	if sync {
		if err := l.f.Sync(); err != nil {
			return err
		}
	}
	l.mu.Lock()
	l.written = max(l.written, upto)
	if sync {
		l.synced = max(l.synced, upto)
	}
	l.spare = data
	l.cond.Broadcast()
	l.mu.Unlock()
	return nil
}

func (l *Log) background() {
	defer l.wg.Done()
	t := time.NewTicker(l.opt.Interval)
	defer t.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-t.C:
		}
		l.io.Lock()
		err := l.flushLocked(false)
		l.io.Unlock()
		if err == nil && l.opt.Sync == SyncPeriodic {
			err = l.syncWritten()
		}
		l.fail(err)
	}
}

func (l *Log) syncWritten() error {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	l.mu.Lock()
	upto := l.written
	l.mu.Unlock()
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.mu.Lock()
	l.synced = max(l.synced, upto)
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
func (l *Log) Size() int64 { return l.size.Load() }
