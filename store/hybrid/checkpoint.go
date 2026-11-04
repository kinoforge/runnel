package hybrid

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const (
	snapPrefix = "snap-"
	snapSuffix = ".bin"
	tmpSuffix  = ".tmp"
	chunkBytes = 1 << 20
)

// recover loads the newest snapshot, then replays the log segments written
// after it.
func (s *Store) recover() error {
	snaps, err := s.snapshots()
	if err != nil {
		return err
	}
	from := uint64(0)
	if len(snaps) > 0 {
		from = snaps[len(snaps)-1]
		if err := s.loadSnapshot(from); err != nil {
			return err
		}
	}
	return s.log.Replay(from, func(rec []byte) error { return replay(rec, s.space) })
}

func (s *Store) snapPath(seg uint64) string {
	return filepath.Join(s.opt.Dir, fmt.Sprintf("%s%016d%s", snapPrefix, seg, snapSuffix))
}

func (s *Store) snapshots() ([]uint64, error) {
	entries, err := os.ReadDir(s.opt.Dir)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, snapPrefix) || !strings.HasSuffix(name, snapSuffix) {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, snapPrefix), snapSuffix), 10, 64); err == nil {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out, nil
}

func (s *Store) loadSnapshot(seg uint64) error {
	f, err := os.Open(s.snapPath(seg))
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := wal.ReadFrames(f, func(rec []byte) error { return replay(rec, s.space) }); err != nil {
		return fmt.Errorf("hybrid: snapshot %d: %w", seg, err)
	}
	return nil
}

func (s *Store) checkpointer() {
	defer s.wg.Done()
	t := time.NewTicker(checkEvery)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			if s.broken() == nil && s.log.Size() >= s.opt.SnapshotBytes {
				if err := s.Checkpoint(); err != nil {
					s.fail(err)
				}
			}
		}
	}
}

// Checkpoint writes a snapshot of every namespace and deletes the log segments
// it covers. Calls pause only while the snapshot copies its pointers.
func (s *Store) Checkpoint() error {
	if s.log == nil {
		return nil
	}
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	s.gate.Lock()
	seg, err := s.log.Rotate()
	if err != nil {
		s.gate.Unlock()
		return err
	}
	views := s.copyAll()
	s.gate.Unlock()
	if err := s.writeSnapshot(seg, views); err != nil {
		return err
	}
	old, err := s.snapshots()
	if err != nil {
		return err
	}
	for _, n := range old {
		if n < seg {
			if err := os.Remove(s.snapPath(n)); err != nil {
				return err
			}
		}
	}
	return s.log.Remove(seg)
}

// view is a point-in-time copy of a space. Jobs and event slices are shared,
// which is safe because the store never changes them in place.
type view struct {
	ns      string
	jobs    []*runnel.Job
	uniques map[string]unique
	kids    map[string][]string
	events  []runnel.Event
	seq     int64
	cursors map[string]int64
	configs map[runnel.ConfigKind]map[string][]byte
	buckets map[string]bucket
}

// copyAll copies every space. The caller holds the gate exclusively.
func (s *Store) copyAll() []view {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]view, 0, len(s.spaces))
	for _, sp := range s.spaces {
		v := view{ns: sp.ns, jobs: slices.Collect(maps.Values(sp.jobs)), uniques: maps.Clone(sp.uniques),
			kids: maps.Clone(sp.kids), events: sp.events[:len(sp.events):len(sp.events)], seq: sp.seq,
			cursors: maps.Clone(sp.cursors), configs: map[runnel.ConfigKind]map[string][]byte{}, buckets: maps.Clone(sp.buckets)}
		for k, m := range sp.configs {
			v.configs[k] = maps.Clone(m)
		}
		out = append(out, v)
	}
	return out
}

func (s *Store) writeSnapshot(seg uint64, views []view) error {
	path := s.snapPath(seg)
	f, err := os.OpenFile(path+tmpSuffix, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	err = writeViews(f, views)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = os.Rename(path+tmpSuffix, path)
	}
	if err != nil {
		_ = os.Remove(path + tmpSuffix)
		return err
	}
	return wal.SyncDir(s.opt.Dir)
}

// writeViews writes each view as records of about chunkBytes, each starting
// with the namespace, in the format replay reads.
func writeViews(f *os.File, views []view) error {
	for _, v := range views {
		if err := writeView(f, v); err != nil {
			return err
		}
	}
	return nil
}

func writeView(f *os.File, v view) error {
	{
		w := chunker{f: f, ns: v.ns}
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
		for _, j := range v.jobs {
			if err := w.e.opJob(j); err != nil {
				return err
			}
			if err := w.maybeFlush(); err != nil {
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
		if err := w.flush(); err != nil {
			return err
		}
	}
	return nil
}

type chunker struct {
	f   *os.File
	ns  string
	e   enc
	buf []byte
}

func (c *chunker) start() {
	c.e.b = c.e.b[:0]
	c.e.str(c.ns)
}

func (c *chunker) maybeFlush() error {
	if len(c.e.b) < chunkBytes {
		return nil
	}
	return c.flush()
}

func (c *chunker) flush() error {
	c.buf = wal.AppendFrame(c.buf[:0], c.e.b)
	_, err := c.f.Write(c.buf)
	c.start()
	return err
}
