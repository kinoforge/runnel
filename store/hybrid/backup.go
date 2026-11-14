package hybrid

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Backup writes a copy of the store to w as a tar file while calls go on.
// The copy holds what a crash at the end of the backup would leave: the
// newest snapshot, then a checkpoint of the cold store, then the log from
// the snapshot on. Each part is taken after the one before it, so the log
// covers everything the cold store holds and opening the copy replays it.
// Unpack it with Restore.
func (s *Store) Backup(ctx context.Context, w io.Writer) error {
	if s.log == nil {
		return errors.New("hybrid: a memory store has nothing to back up")
	}
	if err := s.broken(); err != nil {
		return err
	}
	// Holding snapMu keeps checkpoints from deleting the files being copied.
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	tw := tar.NewWriter(w)
	b := &backup{tw: tw, now: time.Now()}
	if err := b.bytes(formatFile, formatLine()); err != nil {
		return err
	}
	snaps, err := s.snapshots()
	if err != nil {
		return err
	}
	from := uint64(0)
	if len(snaps) > 0 {
		from = snaps[len(snaps)-1]
		if err := b.file(ctx, s.snapPath(from), filepath.Base(s.snapPath(from))); err != nil {
			return err
		}
	}
	tmp := filepath.Join(s.opt.Dir, fmt.Sprintf(backupFiles, time.Now().UnixNano()))
	defer os.RemoveAll(tmp)
	if err := s.cold.Checkpoint(tmp); err != nil {
		return err
	}
	if err := b.tree(ctx, tmp, coldDir); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	segs, err := s.log.Segments()
	if err != nil {
		return err
	}
	for _, seg := range segs {
		if seg >= from {
			if err := b.file(ctx, s.log.Path(seg), filepath.Base(s.log.Path(seg))); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

type backup struct {
	tw  *tar.Writer
	now time.Time
}

func (b *backup) bytes(name string, data []byte) error {
	if err := b.tw.WriteHeader(&tar.Header{Name: name, Mode: filePerm, Size: int64(len(data)), ModTime: b.now, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := b.tw.Write(data)
	return err
}

// file copies the bytes src holds now. The newest log segment grows while it
// is read; a record cut off at its end is dropped when the copy is opened.
func (b *backup) file(ctx context.Context, src, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if err := b.tw.WriteHeader(&tar.Header{Name: name, Mode: filePerm, Size: st.Size(), ModTime: st.ModTime(), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err = io.CopyN(b.tw, f, st.Size())
	return err
}

func (b *backup) tree(ctx context.Context, root, prefix string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		return b.file(ctx, p, path.Join(prefix, filepath.ToSlash(rel)))
	})
}

// Restore unpacks a backup written by Backup into dir, which must be empty
// or missing. Open the store on dir afterwards to replay it.
func Restore(r io.Reader, dir string) error {
	if err := emptyDir(dir); err != nil {
		return err
	}
	tr := tar.NewReader(r)
	seen := false
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if h.Typeflag != tar.TypeReg {
			return fmt.Errorf("hybrid: backup entry %q is not a file", h.Name)
		}
		dst, err := entryPath(dir, h.Name)
		if err != nil {
			return err
		}
		if err := writeEntry(dst, tr); err != nil {
			return err
		}
		seen = seen || h.Name == formatFile
	}
	if !seen {
		return fmt.Errorf("hybrid: not a backup: it has no %s file", formatFile)
	}
	return syncTree(dir)
}

func emptyDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return os.MkdirAll(dir, dirPerm)
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("hybrid: restore needs an empty directory, %s is not", dir)
	}
	return nil
}

// entryPath keeps every entry inside dir.
func entryPath(dir, name string) (string, error) {
	clean := path.Clean(name)
	if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(name, `\`) {
		return "", fmt.Errorf("hybrid: backup entry %q leaves the directory", name)
	}
	return filepath.Join(dir, filepath.FromSlash(clean)), nil
}

func writeEntry(dst string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(dst), dirPerm); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, filePerm)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

func syncTree(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		return errors.Join(f.Sync(), f.Close())
	})
}
