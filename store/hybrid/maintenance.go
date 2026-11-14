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
	dirPerm     = 0o750
)

func (s *Store) Prune(_ context.Context, ns string, req runnel.PruneRequest) (int, error) {
	if len(req.States) == 0 || req.Limit <= 0 {
		return 0, nil
	}
	var states []uint8
	for _, st := range req.States {
		if !st.Final() {
			return 0, fmt.Errorf("%w: only finished jobs can be pruned", runnel.ErrInvalid)
		}
		states = append(states, stateCode(st))
	}
	before := ms(req.Before)
	s.coldMu.Lock()
	defer s.coldMu.Unlock()
	old, err := s.coldPrunable(ns, before, states, req)
	if err != nil {
		return 0, err
	}
	n := 0
	var dropped []uid
	err = s.write(ns, func(t *tx) error {
		sp := t.sp
		for _, id := range old.ids {
			if _, ok := sp.get(id); !ok {
				dropped = append(dropped, id)
			}
		}
		gone := sp.prunable(before, states, req.Queue, req.Limit-len(dropped))
		if req.Archive {
			if err := s.archivePruned(sp, gone, dropped, old.jobs); err != nil {
				return err
			}
		}
		for _, h := range gone {
			t.delJob(h.id)
		}
		for _, id := range dropped {
			t.delJob(id)
		}
		n = len(gone) + len(dropped)
		expire(t, before)
		return nil
	})
	if err == nil && len(dropped) > 0 {
		err = s.dropCold(s.space(ns), dropped)
	}
	return n, err
}

// archivePruned archives the pruned jobs: those in memory, then those only
// the cold store held, read beforehand into cold.
func (s *Store) archivePruned(sp *space, gone []job, dropped []uid, cold map[uid]*runnel.Job) error {
	list := make([]*runnel.Job, 0, len(gone)+len(dropped))
	for i := range gone {
		j, err := s.full(sp, &gone[i])
		if err != nil {
			return err
		}
		list = append(list, j)
	}
	for _, id := range dropped {
		if j := cold[id]; j != nil {
			list = append(list, j)
		}
	}
	return s.archive(list)
}

// expire frees the unique keys held for a time that ended before before.
func expire(t *tx, before int64) {
	for {
		k, ok := t.sp.expiry.Min()
		if !ok || k.until >= before {
			return
		}
		t.delUnique(k.key)
	}
}

// prunable returns up to limit jobs in memory that finished before before in
// one of states, and in queue when it is named.
func (sp *space) prunable(before int64, states []uint8, queue string, limit int) []job {
	q, known := sp.syms.find(queue)
	var gone []job
	sp.fin.Ascend(func(k timeKey) bool {
		if !known || k.at >= before || len(gone) >= limit {
			return false
		}
		h, _ := sp.get(k.id)
		if k.at > 0 && slices.Contains(states, h.state) && (q == 0 || h.queue == q) {
			gone = append(gone, h)
		}
		return true
	})
	return gone
}

// coldOld are jobs in the cold store to prune, read whole when archived.
type coldOld struct {
	ids  []uid
	jobs map[uid]*runnel.Job
}

func (s *Store) coldPrunable(ns string, before int64, states []uint8, req runnel.PruneRequest) (coldOld, error) {
	var old coldOld
	if s.cold == nil {
		return old, nil
	}
	ids, err := s.cold.Oldest(ns, before, req.Limit, func(st uint8, q string) bool {
		return slices.Contains(states, st) && (req.Queue == "" || q == req.Queue)
	})
	if err != nil {
		return old, err
	}
	old.jobs = map[uid]*runnel.Job{}
	for _, c := range ids {
		id := uid(c)
		old.ids = append(old.ids, id)
		if req.Archive {
			if old.jobs[id], err = s.coldGet(ns, id); err != nil {
				return old, err
			}
		}
	}
	return old, nil
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
	before := ms(req.Before)
	s.coldMu.Lock()
	defer s.coldMu.Unlock()
	n, all, err := s.pruneColdEvents(ns, before, req.UpTo, req.Limit)
	if err != nil || !all {
		return n, err
	}
	err = s.write(ns, func(t *tx) error {
		var last int64
		for _, e := range t.sp.events {
			if n >= req.Limit || e.at >= before || (req.UpTo > 0 && e.seq > req.UpTo) {
				break
			}
			last = e.seq
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
