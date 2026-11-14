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

func TestConformanceDiskRetiring(t *testing.T) {
	conformOnDisk(t, hybrid.Options{SnapshotBytes: 1 << 14}, time.Nanosecond, 5*time.Millisecond)
}

func TestConformanceDiskSpilling(t *testing.T) {
	opt := hybrid.Options{Dir: t.TempDir(), SnapshotBytes: 1 << 14}
	conformWith(t, func() (*hybrid.Store, error) { return hybrid.OpenSpilling(opt, 1, 5*time.Millisecond) })
}

func conformOnDisk(t *testing.T, opt hybrid.Options, retire ...time.Duration) {
	opt.Dir = t.TempDir()
	conformWith(t, func() (*hybrid.Store, error) {
		if len(retire) == 2 {
			return hybrid.OpenRetiring(opt, retire[0], retire[1])
		}
		return hybrid.Open(opt)
	})
}

func conformWith(t *testing.T, open func() (*hybrid.Store, error)) {
	var once sync.Once
	var s *hybrid.Store
	storetest.Run(t, func(t *testing.T) runnel.Store {
		once.Do(func() {
			var err error
			if s, err = open(); err != nil {
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
