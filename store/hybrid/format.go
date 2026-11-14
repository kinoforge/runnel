package hybrid

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/kinoforge/runnel/store/hybrid/wal"
)

// Raise it for any change an older runnel would misread.
const formatVersion = 1

const (
	formatFile  = "FORMAT"
	formatName  = "runnel-hybrid"
	backupGlob  = "backup-*.tmp"
	backupFiles = "backup-%d.tmp"
)

// ErrFormat means the data directory was written in a format this build does
// not read.
var ErrFormat = errors.New("hybrid: unsupported data format")

func checkFormat(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, formatFile))
	if errors.Is(err, fs.ErrNotExist) {
		return newFormat(dir)
	}
	if err != nil {
		return err
	}
	var name string
	var v int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(b)), "%s %d", &name, &v); err != nil || name != formatName {
		return fmt.Errorf("%w: %s is not a %s file", ErrFormat, formatFile, formatName)
	}
	if v != formatVersion {
		return fmt.Errorf("%w: %s holds format %d and this runnel reads format %d", ErrFormat, dir, v, formatVersion)
	}
	return nil
}

func newFormat(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if n := e.Name(); strings.HasPrefix(n, snapPrefix) || n == coldDir || strings.HasSuffix(n, ".log") {
			return fmt.Errorf("%w: %s holds data but no %s file", ErrFormat, dir, formatFile)
		}
	}
	return writeFile(dir, formatFile, formatLine())
}

func formatLine() []byte { return fmt.Appendf(nil, "%s %d\n", formatName, formatVersion) }

func writeFile(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+tmpSuffix)
	if err := os.WriteFile(tmp, data, filePerm); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	err = errors.Join(f.Sync(), f.Close())
	if err == nil {
		err = os.Rename(tmp, filepath.Join(dir, name))
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return wal.SyncDir(dir)
}

func removeStaleBackups(dir string) error {
	stale, err := filepath.Glob(filepath.Join(dir, backupGlob))
	if err != nil {
		return err
	}
	for _, p := range stale {
		if err := os.RemoveAll(p); err != nil {
			return err
		}
	}
	return nil
}
