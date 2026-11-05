package hybrid_test

import (
	"sync"
	"testing"
	"time"

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

// TestConformanceDiskRetiring moves finished jobs to the cold store within
// milliseconds, so every rule also runs against jobs read back from it.
func TestConformanceDiskRetiring(t *testing.T) {
	conformOnDisk(t, hybrid.Options{SnapshotBytes: 1 << 14}, time.Nanosecond, 5*time.Millisecond)
}

func conformOnDisk(t *testing.T, opt hybrid.Options, retire ...time.Duration) {
	opt.Dir = t.TempDir()
	var once sync.Once
	var s *hybrid.Store
	storetest.Run(t, func(t *testing.T) runnel.Store {
		once.Do(func() {
			var err error
			if len(retire) == 2 {
				s, err = hybrid.OpenRetiring(opt, retire[0], retire[1])
			} else {
				s, err = hybrid.Open(opt)
			}
			if err != nil {
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
