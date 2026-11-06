package cold_test

import (
	"testing"

	"github.com/kinoforge/runnel/store/hybrid/cold"
)

func waiting(n byte, prio int32, at int64, ready bool) cold.Job {
	return cold.Job{NS: "acme", ID: id(n), State: cold.Waiting, Prio: prio, RunAt: at, StartBy: int64(n) * 10,
		Ready: ready, Queue: "q", Kind: "k", Tenant: "t", Data: []byte{n}}
}

// TestWaitingOrder checks ready jobs come back in claim order, negative
// priorities and run times included, and future ones join them once woken.
func TestWaitingOrder(t *testing.T) {
	s, err := cold.Open(t.TempDir(), cold.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	delta, err := s.Put([]cold.Job{waiting(1, -5, 10, true), waiting(2, 7, -3, true), waiting(3, 7, 5, true),
		waiting(4, 0, 50, false), waiting(5, 9, 40, false)})
	if err != nil {
		t.Fatal(err)
	}
	if delta[cold.Count{NS: "acme", Queue: "q", State: cold.Waiting, Tenant: "t"}] != 5 {
		t.Fatalf("tenant count %v", delta)
	}
	h, err := s.Front("acme", "q")
	if err != nil || !h.HasReady || h.Ready.ID != id(2) || !h.HasDue || h.Due != 40 {
		t.Fatalf("front %+v %v", h, err)
	}
	if n, err := s.Wake("acme", "q", 45, 100); err != nil || n != 1 {
		t.Fatalf("woke %d %v", n, err)
	}
	jobs, h, err := s.Take("acme", "q", 3, 1<<20)
	if err != nil || len(jobs) != 3 || jobs[0].ID != id(5) || jobs[1].ID != id(2) || jobs[2].ID != id(3) {
		t.Fatalf("take %+v %v", jobs, err)
	}
	if !h.HasReady || h.Ready.ID != id(1) || h.Due != 50 {
		t.Fatalf("head after take %+v", h)
	}
	var deadlines int
	if err := s.Deadlines(func(ns string, at int64, _ cold.ID) {
		if ns == "acme" && at > 0 {
			deadlines++
		}
	}); err != nil || deadlines != 5 {
		t.Fatalf("deadlines %d %v", deadlines, err)
	}
	if _, err := s.Delete("acme", []cold.ID{id(1), id(2), id(3), id(4), id(5)}); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.Front("acme", "q"); !h.Empty() {
		t.Fatalf("front after delete %+v", h)
	}
	counts, _ := s.Counts()
	if len(counts) != 0 {
		t.Fatalf("counts after delete %v", counts)
	}
}
