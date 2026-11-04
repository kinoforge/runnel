package hybrid_test

import (
	"sync"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/storetest"
)

func TestConformanceMemory(t *testing.T) {
	s := hybrid.Memory()
	t.Cleanup(func() { _ = s.Close() })
	storetest.Run(t, func(*testing.T) runnel.Store { return s })
}

func TestConformanceDisk(t *testing.T) {
	dir := t.TempDir()
	var once sync.Once
	var s *hybrid.Store
	storetest.Run(t, func(t *testing.T) runnel.Store {
		once.Do(func() {
			var err error
			s, err = hybrid.Open(hybrid.Options{Dir: dir, SnapshotBytes: 1 << 16})
			if err != nil {
				t.Fatal(err)
			}
		})
		return s
	})
	_ = s.Close()
}
