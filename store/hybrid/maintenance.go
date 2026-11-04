package hybrid

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/kinoforge/runnel"
)

const (
	archiveName = "archive.jsonl"
	filePerm    = 0o600
)

func (s *Store) Prune(_ context.Context, ns string, req runnel.PruneRequest) (int, error) {
	if len(req.States) == 0 || req.Limit <= 0 {
		return 0, nil
	}
	for _, st := range req.States {
		if !st.Final() {
			return 0, fmt.Errorf("%w: only finished jobs can be pruned", runnel.ErrInvalid)
		}
	}
	before := runnel.Millis(req.Before)
	var gone []*runnel.Job
	err := s.write(ns, func(t *tx) error {
		sp := t.sp
		t.sp.fin.Ascend(func(k timeKey) bool {
			if k.at >= before || len(gone) >= req.Limit {
				return false
			}
			j := sp.jobs[k.id]
			if k.at > 0 && slices.Contains(req.States, j.State) && (req.Queue == "" || j.Queue == req.Queue) {
				gone = append(gone, j)
			}
			return true
		})
		if req.Archive {
			if err := s.archive(gone); err != nil {
				gone = nil
				return err
			}
		}
		for _, j := range gone {
			t.delJob(j.ID)
		}
		for {
			k, ok := sp.expiry.Min()
			if !ok || k.until >= before {
				break
			}
			t.delUnique(k.key)
		}
		return nil
	})
	return len(gone), err
}

// archive appends pruned jobs to the archive file in the store's directory. A
// store in memory only has nowhere to keep them.
func (s *Store) archive(jobs []*runnel.Job) error {
	if s.opt.Dir == "" || len(jobs) == 0 {
		return nil
	}
	var b []byte
	for _, j := range jobs {
		line, err := json.Marshal(j)
		if err != nil {
			return err
		}
		b = append(append(b, line...), '\n')
	}
	f, err := os.OpenFile(filepath.Join(s.opt.Dir, archiveName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (s *Store) PruneEvents(_ context.Context, ns string, req runnel.EventPruneRequest) (int, error) {
	if req.Limit <= 0 || req.Before.IsZero() {
		return 0, nil
	}
	before := runnel.Millis(req.Before)
	n := 0
	err := s.write(ns, func(t *tx) error {
		var last int64
		for _, e := range t.sp.events {
			if n >= req.Limit || runnel.Millis(e.At) >= before || (req.UpTo > 0 && e.Seq > req.UpTo) {
				break
			}
			last = e.Seq
			n++
		}
		if n > 0 {
			t.dropEvents(last)
		}
		return nil
	})
	return n, err
}

func (s *Store) SwapCursor(_ context.Context, ns, name string, old, next int64) (bool, error) {
	if old == next {
		return true, nil
	}
	ok := false
	err := s.write(ns, func(t *tx) error {
		if t.sp.cursors[name] == old {
			t.putCursor(name, next)
			ok = true
		}
		return nil
	})
	return ok, err
}
