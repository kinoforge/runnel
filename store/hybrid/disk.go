package hybrid

import (
	"fmt"
	"time"

	"github.com/kinoforge/runnel"
)

func (s *Store) checkDisk() error {
	if s.opt.Dir == "" {
		return nil
	}
	now := time.Now().UnixNano()
	next := s.diskNext.Load()
	if now >= next && s.diskNext.CompareAndSwap(next, now+int64(s.opt.DiskCheckEvery)) {
		free, ok := freeBytes(s.opt.Dir)
		s.diskLow.Store(ok && free < uint64(s.opt.MinFreeBytes))
	}
	if s.diskLow.Load() {
		return fmt.Errorf("%w: less than %d bytes free under %s", runnel.ErrNoSpace, s.opt.MinFreeBytes, s.opt.Dir)
	}
	return nil
}
