package cold_test

import (
	"slices"
	"testing"

	"github.com/kinoforge/runnel/store/hybrid/cold"
)

func id(n byte) cold.ID { return cold.ID{15: n} }

func job(n byte, state uint8, at int64, queue string) cold.Job {
	return cold.Job{NS: "acme", ID: id(n), State: state, Finished: at, Queue: queue, Kind: "k", Data: []byte{n}}
}

func TestPutGetListPrune(t *testing.T) {
	dir := t.TempDir()
	s, err := cold.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put([]cold.Job{job(1, 5, 100, "a"), job(2, 5, 300, "b"), job(3, 6, 200, "a")}); err != nil {
		t.Fatal(err)
	}
	delta, err := s.Put([]cold.Job{job(1, 7, 400, "a")})
	if err != nil {
		t.Fatal(err)
	}
	if delta[cold.Count{NS: "acme", Queue: "a", State: 5}] != -1 || delta[cold.Count{NS: "acme", Queue: "a", State: 7}] != 1 {
		t.Fatalf("replace delta %v", delta)
	}
	if j, ok, err := s.Get("acme", id(1)); err != nil || !ok || j.State != 7 || j.Finished != 400 || j.Data[0] != 1 {
		t.Fatalf("get %+v %v %v", j, ok, err)
	}
	if _, ok, _ := s.Get("other", id(1)); ok {
		t.Fatal("namespaces share jobs")
	}
	got, _ := s.List("acme", 5, cold.Filter{}, nil, 0)
	if !slices.Equal(got, []cold.ID{id(2)}) {
		t.Fatalf("completed %v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = cold.Open(dir); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	counts, _ := s.Counts()
	want := map[cold.Count]int64{{NS: "acme", Queue: "b", State: 5}: 1, {NS: "acme", Queue: "a", State: 6}: 1, {NS: "acme", Queue: "a", State: 7}: 1}
	if len(counts) != len(want) {
		t.Fatalf("counts after reopen %v", counts)
	}
	for c, n := range want {
		if counts[c] != n {
			t.Fatalf("counts after reopen %v", counts)
		}
	}
	old, _ := s.Oldest("acme", 350, 10, func(_ uint8, q string) bool { return q == "a" })
	if !slices.Equal(old, []cold.ID{id(3)}) {
		t.Fatalf("oldest %v", old)
	}
	if _, err := s.Delete("acme", old); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get("acme", id(3)); ok {
		t.Fatal("deleted job still there")
	}
}

func TestListNewestFirstWithCursor(t *testing.T) {
	s, err := cold.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var jobs []cold.Job
	for n := byte(1); n <= 9; n++ {
		q := "a"
		if n%3 == 0 {
			q = "b"
		}
		jobs = append(jobs, job(n, 5, int64(n), q))
	}
	if _, err := s.Put(jobs); err != nil {
		t.Fatal(err)
	}
	page, _ := s.List("acme", 5, cold.Filter{Queue: "a"}, nil, 3)
	if !slices.Equal(page, []cold.ID{id(8), id(7), id(5)}) {
		t.Fatalf("first page %v", page)
	}
	cur := page[2]
	page, _ = s.List("acme", 5, cold.Filter{Queue: "a"}, &cur, 3)
	if !slices.Equal(page, []cold.ID{id(4), id(2), id(1)}) {
		t.Fatalf("second page %v", page)
	}
}
