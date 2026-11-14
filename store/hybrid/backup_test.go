package hybrid_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/hybrid"
)

func restored(t *testing.T, backup []byte) *hybrid.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "restored")
	if err := hybrid.Restore(bytes.NewReader(backup), dir); err != nil {
		t.Fatal(err)
	}
	return open(t, dir)
}

func TestBackupRestore(t *testing.T) {
	spec.Rule(t, "X07")
	s, err := hybrid.OpenRetiring(hybrid.Options{Dir: t.TempDir()}, time.Nanosecond, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	fill(t, s)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	c := runnel.NewClient(s, ns)
	if _, err := c.Enqueue(context.Background(), "q", "after", nil); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); s.Cold(ns) == 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no job reached the cold store")
		}
	}
	want := dump(t, s)
	var buf bytes.Buffer
	if err := s.Backup(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	r := restored(t, buf.Bytes())
	defer r.Close()
	if got := dump(t, r); got != want {
		t.Fatalf("restored\n got %s\nwant %s", got, want)
	}
}

func TestBackupWhileWriting(t *testing.T) {
	spec.Rule(t, "X07")
	s := open(t, t.TempDir())
	defer s.Close()
	c := runnel.NewClient(s, ns)
	ctx := context.Background()
	var mu sync.Mutex
	var acked []string
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			res, err := c.Enqueue(ctx, "q", "k", map[string]string{"pad": "0123456789abcdef"})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			acked = append(acked, res.ID)
			mu.Unlock()
		}
	})
	time.Sleep(20 * time.Millisecond)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	before := append([]string(nil), acked...)
	mu.Unlock()
	var buf bytes.Buffer
	err := s.Backup(ctx, &buf)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	r := restored(t, buf.Bytes())
	defer r.Close()
	got, err := r.Get(ctx, ns, before)
	if err != nil || len(got) != len(before) {
		t.Fatalf("restored %d of %d jobs acknowledged before the backup: %v", len(got), len(before), err)
	}
}

func TestRestoreRefuses(t *testing.T) {
	spec.Rule(t, "X07")
	full := t.TempDir()
	if err := os.WriteFile(filepath.Join(full, "x"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := hybrid.Restore(bytes.NewReader(nil), full); err == nil {
		t.Fatal("restored into a directory with files")
	}
	for name, entries := range map[string][]string{"escape": {"FORMAT", "../evil"}, "no format": {"wal-1.log"}} {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		for _, e := range entries {
			_ = tw.WriteHeader(&tar.Header{Name: e, Mode: 0o600, Size: 1, Typeflag: tar.TypeReg})
			_, _ = tw.Write([]byte{'x'})
		}
		_ = tw.Close()
		if err := hybrid.Restore(&buf, filepath.Join(t.TempDir(), "d")); err == nil {
			t.Errorf("%s: restored", name)
		}
	}
}

func TestDataFormat(t *testing.T) {
	spec.Rule(t, "X06")
	dir := t.TempDir()
	s := open(t, dir)
	_ = s.Close()
	if b, err := os.ReadFile(filepath.Join(dir, "FORMAT")); err != nil || string(b) != "runnel-hybrid 1\n" {
		t.Fatalf("FORMAT %q %v", b, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "FORMAT"), []byte("runnel-hybrid 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hybrid.Open(hybrid.Options{Dir: dir}); !errors.Is(err, hybrid.ErrFormat) {
		t.Fatalf("newer format: %v", err)
	}
	old := t.TempDir()
	if err := os.WriteFile(filepath.Join(old, "wal-0000000000000001.log"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hybrid.Open(hybrid.Options{Dir: old}); !errors.Is(err, hybrid.ErrFormat) {
		t.Fatalf("data without FORMAT: %v", err)
	}
}
