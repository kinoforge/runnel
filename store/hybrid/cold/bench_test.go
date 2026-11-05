package cold_test

import (
	"crypto/rand"
	"testing"

	"github.com/kinoforge/runnel/store/hybrid/cold"
)

func BenchmarkMetaMiss(b *testing.B) {
	s, err := cold.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	for range 100 {
		jobs := make([]cold.Job, 1000)
		for i := range jobs {
			_, _ = rand.Read(jobs[i].ID[:])
			jobs[i].NS, jobs[i].State, jobs[i].Queue, jobs[i].Data = "acme", 5, "q", make([]byte, 100)
		}
		if _, err := s.Put(jobs); err != nil {
			b.Fatal(err)
		}
	}
	var id cold.ID
	b.ResetTimer()
	for b.Loop() {
		_, _ = rand.Read(id[:])
		if _, ok, _ := s.Meta("acme", id); ok {
			b.Fatal("found")
		}
	}
}
