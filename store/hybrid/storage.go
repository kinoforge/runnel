package hybrid

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"

	"github.com/kinoforge/runnel"
)

func (s *Store) Storage(context.Context) (runnel.StorageInfo, error) {
	info := runnel.StorageInfo{Kind: "hybrid", Details: map[string]int64{"hot_jobs": s.hot.Load(), "hot_jobs_limit": s.hotJobs}}
	if s.log == nil {
		return info, s.broken()
	}
	info.Details["log_bytes"] = s.log.Size()
	info.Details["snapshot_bytes"] = s.snapBytes.Load()
	n, err := dirBytes(s.opt.Dir)
	info.Bytes = n
	return info, err
}

func dirBytes(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		n += fi.Size()
		return nil
	})
	return n, err
}
