package hybrid

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel/store/hybrid/wal"
)

const (
	snapPrefix = "snap-"
	snapSuffix = ".bin"
	tmpSuffix  = ".tmp"
)

// recover loads the newest snapshot, then replays the log segments written
// after it. Bodies stay on disk; jobs only learn where they are.
func (s *Store) recover() error {
	snaps, err := s.snapshots()
	if err != nil {
		return err
	}
	r := &replayer{get: s.space, cold: s.cold}
	from := uint64(0)
	if len(snaps) > 0 {
		from = snaps[len(snaps)-1]
		if err := s.loadSnapshot(from, r); err != nil {
			return err
		}
	}
	return s.log.Replay(from, func(seg uint64, off int64, rec []byte) error {
		r.file, r.base = uint32(seg), off
		return r.record(rec)
	})
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

func (s *Store) loadSnapshot(seg uint64, r *replayer) error {
	f, err := os.Open(s.snapPath(seg))
	if err != nil {
		return err
	}
	defer f.Close()
	r.file = snapBit | uint32(seg)
	n, err := wal.ReadFrames(f, func(off int64, rec []byte) error {
		r.base = off
		return r.record(rec)
	})
	if err != nil {
		return fmt.Errorf("hybrid: snapshot %d: %w", seg, err)
	}
	s.snapBytes.Store(n)
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
			if s.broken() == nil && s.log.Size() >= max(s.opt.SnapshotBytes, s.snapBytes.Load()) {
				if err := s.Checkpoint(); err != nil {
					s.fail(err)
				}
			}
		}
	}
}

// Checkpoint writes a snapshot of every namespace, bodies included, points the
// jobs at their bodies in it, and deletes the log segments and snapshots it
// replaces. Calls go on while it runs.
func (s *Store) Checkpoint() error {
	if s.log == nil {
		return nil
	}
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	seg, err := s.log.Rotate()
	if err != nil {
		return err
	}
	moved, err := s.writeSnapshot(seg)
	if err != nil {
		return err
	}
	s.repoint(snapBit|uint32(seg), moved)
	s.files.drop(func(id uint32) bool { return uint64(id&^snapBit) < seg })
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

// moved is a body copied into a snapshot, to offset to.
type moved struct {
	id   uid
	from ref
	to   int64
}

// repoint moves jobs whose body is still the one copied to the copy in
// snapshot file, so the older files can go. It takes the lock a slab at a
// time. Bodies cached from before the rotation are all written by now and
// leave first, so no cached body refers to an old file.
func (s *Store) repoint(file uint32, bySpace map[*space][]moved) {
	written := s.log.Written()
	for sp, list := range bySpace {
		for len(list) > 0 {
			n := min(len(list), slabSize)
			sp.mu.Lock()
			sp.cache.evict(written, sp.bodyRef)
			for _, m := range list[:n] {
				if i, ok := sp.idx[m.id]; ok {
					if h := sp.jobs.at(i); h.body == m.from {
						h.body = ref{file: file, n: m.from.n, off: m.to}
					}
				}
			}
			sp.mu.Unlock()
			list = list[n:]
		}
	}
}
