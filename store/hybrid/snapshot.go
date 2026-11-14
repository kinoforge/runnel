package hybrid

import (
	"cmp"
	"errors"
	"maps"
	"os"
	"slices"

	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const chunkBytes = 1 << 20

// Snapshots run while calls go on. The log was rotated first, so the segments after it hold whatever a snapshot misses, and replaying them over it gives the same state.

func (s *Store) writeSnapshot(seg uint64) (map[*space][]moved, error) {
	path := s.snapPath(seg)
	f, err := os.OpenFile(path+tmpSuffix, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return nil, err
	}
	w := &snapWriter{f: f, seg: seg, bodies: window{fs: s.files}, moved: map[*space][]moved{}}
	for _, sp := range s.allSpaces() {
		if err = w.space(sp); err != nil {
			break
		}
	}
	if err == nil {
		// Records of changes made after the rotation must be on disk before the snapshot replaces the segments before them.
		err = s.log.Sync()
	}
	if err == nil {
		err = s.cold.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = os.Rename(path+tmpSuffix, path)
	}
	if err != nil {
		_ = os.Remove(path + tmpSuffix)
		return nil, err
	}
	s.snapBytes.Store(w.off)
	return w.moved, wal.SyncDir(s.opt.Dir)
}

func (s *Store) allSpaces() []*space {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Values(s.spaces))
}

type snapWriter struct {
	f      *os.File
	seg    uint64
	off    int64
	bodies window
	sp     *space
	e      enc
	buf    []byte
	page   []job
	spots  []spot
	moved  map[*space][]moved
}

type spot struct {
	id       uid
	from     ref
	pos, len int
}

func (w *snapWriter) space(sp *space) error {
	w.sp = sp
	w.start()
	sp.mu.Lock()
	w.e.opSeq(sp.seq)
	for name, val := range sp.cursors {
		w.e.opCursor(name, val)
	}
	for kind, m := range sp.configs {
		for name, val := range m {
			w.e.opConfig(kind, name, val)
		}
	}
	for key, b := range sp.buckets {
		w.e.opBucket(key, b)
	}
	sp.mu.Unlock()
	for b := 0; ; b++ {
		names, ok := w.block(b)
		if !ok {
			break
		}
		if err := w.jobs(names); err != nil {
			return err
		}
	}
	sp.mu.Lock()
	names := sp.syms.names[:len(sp.syms.names):len(sp.syms.names)]
	uniques, kids, events := maps.Clone(sp.uniques), maps.Clone(sp.kids), sp.events[:len(sp.events):len(sp.events)]
	sp.mu.Unlock()
	for key, u := range uniques {
		w.e.opUnique(key, u)
	}
	for parent, list := range kids {
		for _, k := range list {
			w.e.opKid(parent, k)
		}
	}
	for i := range events {
		w.e.opEvent(&events[i], names)
		if err := w.maybeFlush(); err != nil {
			return err
		}
	}
	return w.flush()
}

func (w *snapWriter) block(b int) ([]string, bool) {
	sp := w.sp
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if b >= len(sp.jobs.blocks) {
		return nil, false
	}
	w.page = append(w.page[:0], sp.jobs.blocks[b]...)
	return sp.syms.names[:len(sp.syms.names):len(sp.syms.names)], true
}

func (w *snapWriter) jobs(names []string) error {
	sy := syms{names: names}
	live := slices.DeleteFunc(w.page, func(h job) bool { return h.state == 0 })
	slices.SortFunc(live, func(a, b job) int { return cmpRef(a.body, b.body) })
	for i := range live {
		if err := w.job(&live[i], &sy); err != nil {
			return err
		}
	}
	return nil
}

// A body written after the rotation stays where it is: the log after the snapshot sets it again.
func (w *snapWriter) job(h *job, sy *syms) error {
	if h.body.n == 0 || (h.body.file&snapBit == 0 && uint64(h.body.file) >= w.seg) {
		w.e.byte(opPutHot)
		w.e.hot(h, sy)
		return w.maybeFlush()
	}
	body, err := w.bodies.read(h.body)
	if err != nil {
		return err
	}
	w.e.byte(opPutJob)
	w.e.hot(h, sy)
	pos := w.e.blob(body)
	w.spots = append(w.spots, spot{id: h.id, from: h.body, pos: pos, len: len(body)})
	return w.maybeFlush()
}

func (w *snapWriter) start() {
	w.e.b = w.e.b[:0]
	w.e.str(w.sp.ns)
}

func (w *snapWriter) maybeFlush() error {
	if len(w.e.b) < chunkBytes {
		return nil
	}
	return w.flush()
}

func (w *snapWriter) flush() error {
	base := w.off + wal.HeaderLen
	for _, sp := range w.spots {
		w.moved[w.sp] = append(w.moved[w.sp], moved{id: sp.id, from: sp.from, to: base + int64(sp.pos)})
	}
	w.spots = w.spots[:0]
	w.buf = wal.AppendFrame(w.buf[:0], w.e.b)
	_, err := w.f.Write(w.buf)
	w.off += int64(len(w.buf))
	w.start()
	return err
}

func cmpRef(a, b ref) int {
	if a.file != b.file {
		return cmp.Compare(a.file, b.file)
	}
	return cmp.Compare(a.off, b.off)
}
