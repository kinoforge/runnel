package wal_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

func TestAnUnfinishedRotationDoesNotBlockReplay(t *testing.T) {
	spec.Rule(t, "X04")
	dir := t.TempDir()
	l := start(t, dir, wal.SyncEach)
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	for _, r := range []string{"a", "b"} {
		p, err := l.Append([]byte(r))
		if err == nil {
			err = l.Wait(p.LSN)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	seg, tmp := l.Path(1), l.Path(2)+".tmp"
	_ = l.Close()
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	frame := wal.AppendFrame(nil, []byte("torn record"))
	_, _ = f.Write(frame[:len(frame)-3])
	_ = f.Close()
	if err := os.WriteFile(tmp, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := records(t, dir, 0); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("replay after a crash mid-rotation: %v", got)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the unfinished segment was left behind: %v", err)
	}
}

func TestRotateKeepsRecordsInOrder(t *testing.T) {
	spec.Rule(t, "X04")
	dir := t.TempDir()
	l := start(t, dir, wal.SyncEach)
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	for i, r := range []string{"a", "b", "c"} {
		p, err := l.Append([]byte(r))
		if err == nil {
			err = l.Wait(p.LSN)
		}
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if _, err := l.Rotate(); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = l.Close()
	if left, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(left) != 0 {
		t.Fatalf("rotation left %v", left)
	}
	if got := records(t, dir, 0); len(got) != 3 || got[2] != "c" {
		t.Fatalf("records after rotation %v", got)
	}
}

type failingReader struct {
	r   *bytes.Reader
	err error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.r.Len() == 0 {
		return 0, f.err
	}
	return f.r.Read(p)
}

func TestAReadErrorIsNotATornFrame(t *testing.T) {
	spec.Rule(t, "X04")
	data := wal.AppendFrame(nil, []byte("kept"))
	data = append(data, wal.AppendFrame(nil, []byte("unread"))[:4]...)
	var got []string
	good, err := wal.ReadFrames(&failingReader{r: bytes.NewReader(data), err: syscall.EIO}, func(_ int64, rec []byte) error {
		got = append(got, string(rec))
		return nil
	})
	if !errors.Is(err, syscall.EIO) || errors.Is(err, wal.ErrTorn) {
		t.Fatalf("a disk error read as %v", err)
	}
	if len(got) != 1 || good != int64(wal.HeaderLen+4) {
		t.Fatalf("records %v, good %d", got, good)
	}
	if _, err := wal.ReadFrames(bytes.NewReader(data), func(int64, []byte) error { return nil }); !errors.Is(err, wal.ErrTorn) {
		t.Fatalf("a short frame at the end is %v, want ErrTorn", err)
	}
}
