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
	conformOnDisk(t, hybrid.Options{SnapshotBytes: 1 << 16})
}

// TestConformanceDiskNoCache keeps no body in memory once written, so every
// read goes to the log or a snapshot.
func TestConformanceDiskNoCache(t *testing.T) {
	conformOnDisk(t, hybrid.Options{SnapshotBytes: 1 << 14, CacheBytes: 1})
}

func conformOnDisk(t *testing.T, opt hybrid.Options) {
	opt.Dir = t.TempDir()
	var once sync.Once
	var s *hybrid.Store
	storetest.Run(t, func(t *testing.T) runnel.Store {
		once.Do(func() {
			var err error
			if s, err = hybrid.Open(opt); err != nil {
				t.Fatal(err)
			}
		})
		return s
	})
	if s != nil {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
