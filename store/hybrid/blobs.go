package hybrid

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
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
	fs.mu.RLock()
	f := fs.open[r.file]
	if f == nil {
		fs.mu.RUnlock()
		if err := fs.load(r.file); err != nil {
			return nil, err
		}
		fs.mu.RLock()
		f = fs.open[r.file]
	}
	defer fs.mu.RUnlock()
	if f == nil {
		return nil, fmt.Errorf("hybrid: body file %d closed", r.file)
	}
	b := make([]byte, r.n)
	if _, err := f.ReadAt(b, r.off); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return b, nil
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

// cache holds job bodies in memory. Every body starts here; once its record is
// written, it leaves when its job is final or the cache is over budget, and is
// read from disk after that. Without a log nothing leaves.
type cache struct {
	bodies map[uid][]byte
	queue  []leaving
	size   *atomic.Int64
}

// leaving is a body that may leave the cache once record lsn is written, if
// the job still has the body at ref.
type leaving struct {
	id  uid
	at  ref
	lsn uint64
}

func (c *cache) put(id uid, b []byte) {
	if old, ok := c.bodies[id]; ok {
		c.size.Add(-int64(len(old)))
	}
	c.bodies[id] = b
	c.size.Add(int64(len(b)))
}

func (c *cache) del(id uid) {
	if old, ok := c.bodies[id]; ok {
		c.size.Add(-int64(len(old)))
		delete(c.bodies, id)
	}
}

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
}
