package hybrid

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// snapBit marks a file id as a snapshot rather than a log segment.
const snapBit = 1 << 31

var errNoBody = errors.New("hybrid: job body missing")

// files reads job bodies from log segments and snapshots, keeping each file
// open once. Readers hold the read lock, so a file is never closed under them.
type files struct {
	path func(id uint32) string
	mu   sync.RWMutex
	open map[uint32]*os.File
}

func (fs *files) read(r ref) ([]byte, error) {
	b := make([]byte, r.n)
	if _, err := fs.readAt(r.file, r.off, b); err != nil {
		return nil, err
	}
	return b, nil
}

// readAt fills b from file id at off and returns how much it read; past the
// end of the file it reads less.
func (fs *files) readAt(id uint32, off int64, b []byte) (int, error) {
	fs.mu.RLock()
	f := fs.open[id]
	if f == nil {
		fs.mu.RUnlock()
		if err := fs.load(id); err != nil {
			return 0, err
		}
		fs.mu.RLock()
		f = fs.open[id]
	}
	defer fs.mu.RUnlock()
	if f == nil {
		return 0, fmt.Errorf("hybrid: body file %d closed", id)
	}
	n, err := f.ReadAt(b, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}
	return n, nil
}

// window reads bodies in file order through one buffer, so a snapshot reads
// each file once instead of once per body.
type window struct {
	fs   *files
	file uint32
	off  int64
	buf  []byte
}

func (w *window) read(at ref) ([]byte, error) {
	end := at.off + int64(at.n)
	if at.file != w.file || at.off < w.off || end > w.off+int64(len(w.buf)) {
		buf := make([]byte, max(int(at.n), chunkBytes))
		n, err := w.fs.readAt(at.file, at.off, buf)
		if err != nil {
			return nil, err
		}
		if n < int(at.n) {
			return nil, errNoBody
		}
		w.file, w.off, w.buf = at.file, at.off, buf[:n]
	}
	return w.buf[at.off-w.off : end-w.off], nil
}

func (fs *files) load(id uint32) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open[id] != nil {
		return nil
	}
	f, err := os.Open(fs.path(id))
	if err != nil {
		return err
	}
	fs.open[id] = f
	return nil
}

// drop closes the files gone reports true for.
func (fs *files) drop(gone func(id uint32) bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for id, f := range fs.open {
		if gone(id) {
			_ = f.Close()
			delete(fs.open, id)
		}
	}
}

func (fs *files) close() {
	fs.drop(func(uint32) bool { return true })
}

// cache holds the job bodies whose records are not written yet; without a log
// it holds them all.
type cache struct {
	bodies map[uid][]byte
	queue  []leaving
	peak   int
}

// shrinkAt is the size past which an emptied cache map is replaced, since Go
// maps keep their memory.
const shrinkAt = 1 << 12

// leaving is a body that may leave the cache once record lsn is written, if
// the job still has the body at ref.
type leaving struct {
	id  uid
	at  ref
	lsn uint64
}

func (c *cache) put(id uid, b []byte) {
	c.bodies[id] = b
	c.peak = max(c.peak, len(c.bodies))
}

func (c *cache) del(id uid) { delete(c.bodies, id) }

// evict drops the queued bodies whose records are written by now.
func (c *cache) evict(written uint64, bodyOf func(uid) (ref, bool)) {
	i := 0
	for ; i < len(c.queue) && c.queue[i].lsn <= written; i++ {
		l := c.queue[i]
		if at, ok := bodyOf(l.id); ok && at == l.at {
			c.del(l.id)
		}
	}
	if i > 0 {
		c.queue = append(c.queue[:0], c.queue[i:]...)
	}
	if len(c.bodies) == 0 && c.peak > shrinkAt {
		c.bodies, c.peak = map[uid][]byte{}, 0
	}
}
