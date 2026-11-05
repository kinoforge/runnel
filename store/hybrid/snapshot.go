package hybrid

import (
	"errors"
	"maps"
	"os"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const chunkBytes = 1 << 20

// view is a point-in-time copy of a space. Job records are copied; bodies are
// read from disk while the snapshot is written.
type view struct {
	ns      string
	names   []string
	jobs    []job
	uniques map[string]unique
	kids    map[uid][]uid
	events  []runnel.Event
	seq     int64
	cursors map[string]int64
	configs map[runnel.ConfigKind]map[string][]byte
	buckets map[string]bucket
}

// copyAll copies every space. The caller holds the gate exclusively, and the
// log was just rotated, so every body is written and has its place on disk.
func (s *Store) copyAll() []view {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]view, 0, len(s.spaces))
	for _, sp := range s.spaces {
		v := view{ns: sp.ns, names: sp.syms.names[:len(sp.syms.names):len(sp.syms.names)], jobs: make([]job, 0, len(sp.idx)),
			uniques: maps.Clone(sp.uniques), kids: maps.Clone(sp.kids), events: sp.events[:len(sp.events):len(sp.events)],
			seq: sp.seq, cursors: maps.Clone(sp.cursors), configs: map[runnel.ConfigKind]map[string][]byte{}, buckets: maps.Clone(sp.buckets)}
		for _, h := range sp.slots {
			if h.state != 0 {
				v.jobs = append(v.jobs, h)
			}
		}
		for k, m := range sp.configs {
			v.configs[k] = maps.Clone(m)
		}
		out = append(out, v)
	}
	return out
}

// writeSnapshot writes views to the snapshot for seg and returns where each
// body went, by namespace.
func (s *Store) writeSnapshot(seg uint64, views []view) (map[string][]moved, error) {
	path := s.snapPath(seg)
	f, err := os.OpenFile(path+tmpSuffix, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return nil, err
	}
	w := &snapWriter{f: f, file: snapBit | uint32(seg), read: s.files.read, moved: map[string][]moved{}}
	for _, v := range views {
		if err = w.view(v); err != nil {
			break
		}
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
	return w.moved, wal.SyncDir(s.opt.Dir)
}

// snapWriter writes records of about chunkBytes in the format replay reads, and
// notes where in the file each body lands.
type snapWriter struct {
	f     *os.File
	file  uint32
	off   int64
	read  func(ref) ([]byte, error)
	ns    string
	e     enc
	buf   []byte
	spots []spot
	moved map[string][]moved
}

type spot struct {
	id       uid
	from     ref
	pos, len int
}

func (w *snapWriter) view(v view) error {
	w.ns = v.ns
	w.start()
	w.e.opSeq(v.seq)
	for name, val := range v.cursors {
		w.e.opCursor(name, val)
	}
	for kind, m := range v.configs {
		for name, val := range m {
			w.e.opConfig(kind, name, val)
		}
	}
	for key, b := range v.buckets {
		w.e.opBucket(key, b)
	}
	sy := syms{names: v.names}
	for i := range v.jobs {
		if err := w.job(&v.jobs[i], &sy); err != nil {
			return err
		}
	}
	for key, u := range v.uniques {
		w.e.opUnique(key, u)
	}
	for parent, kids := range v.kids {
		for _, k := range kids {
			w.e.opKid(parent, k)
		}
	}
	for _, e := range v.events {
		w.e.opEvent(e)
		if err := w.maybeFlush(); err != nil {
			return err
		}
	}
	return w.flush()
}

func (w *snapWriter) job(h *job, sy *syms) error {
	body, err := w.read(h.body)
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
	w.e.str(w.ns)
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
		w.moved[w.ns] = append(w.moved[w.ns], moved{id: sp.id, from: sp.from,
			to: ref{file: w.file, n: uint32(sp.len), off: base + int64(sp.pos)}})
	}
	w.spots = w.spots[:0]
	w.buf = wal.AppendFrame(w.buf[:0], w.e.b)
	_, err := w.f.Write(w.buf)
	w.off += int64(len(w.buf))
	w.start()
	return err
}
