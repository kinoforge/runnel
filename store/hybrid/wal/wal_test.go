package wal_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/kinoforge/runnel/store/hybrid/wal"
)

func start(t *testing.T, dir string, mode wal.SyncMode) *wal.Log {
	t.Helper()
	l, err := wal.Open(dir, wal.Options{Sync: mode})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func records(t *testing.T, dir string, from uint64) []string {
	t.Helper()
	l := start(t, dir, wal.SyncEach)
	defer l.Close()
	var out []string
	if err := l.Replay(from, func(_ uint64, _ int64, rec []byte) error {
		out = append(out, string(rec))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGroupCommitKeepsEveryRecord(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncEach, wal.SyncPeriodic, wal.SyncOff} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			dir := t.TempDir()
			l := start(t, dir, mode)
			if err := l.Start(); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for g := range 8 {
				wg.Go(func() {
					for i := range 100 {
						p, err := l.Append(fmt.Appendf(nil, "%d-%d", g, i))
						if err == nil {
							err = l.Wait(p.LSN)
						}
						if err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if got := records(t, dir, 0); len(got) != 800 {
				t.Fatalf("replayed %d records, want 800", len(got))
			}
		})
	}
}

func TestRotateAndRemove(t *testing.T) {
	dir := t.TempDir()
	l := start(t, dir, wal.SyncEach)
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	commit := func(s string) {
		p, err := l.Append([]byte(s))
		if err == nil {
			err = l.Wait(p.LSN)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	commit("a")
	seg, err := l.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	commit("b")
	if err := l.Remove(seg); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	if got := records(t, dir, 0); len(got) != 1 || got[0] != "b" {
		t.Fatalf("after remove %v", got)
	}
}

func TestFramesDetectDamage(t *testing.T) {
	b := wal.AppendFrame(nil, []byte("hello"))
	b = wal.AppendFrame(b, []byte("world"))
	b[len(b)-1] ^= 0xff
	var got []string
	good, err := wal.ReadFrames(bytes.NewReader(b), func(_ int64, rec []byte) error {
		got = append(got, string(rec))
		return nil
	})
	if !errors.Is(err, wal.ErrTorn) || len(got) != 1 || good != int64(len(b)/2) {
		t.Fatalf("got %v good %d err %v", got, good, err)
	}
}

// TestRotateWhileAppending checks that every record lands where Append said,
// even when a rotation happens between its Append and its write.
func TestRotateWhileAppending(t *testing.T) {
	dir := t.TempDir()
	l := start(t, dir, wal.SyncPeriodic)
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	at := map[string]wal.Pos{}
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 500 {
				rec := fmt.Sprintf("%d-%d", g, i)
				p, err := l.Append([]byte(rec))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				at[rec] = p
				mu.Unlock()
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			if _, err := l.Rotate(); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = start(t, dir, wal.SyncEach)
	defer l.Close()
	n := 0
	if err := l.Replay(0, func(seg uint64, off int64, rec []byte) error {
		n++
		if p := at[string(rec)]; p.Seg != seg || p.Off != off {
			return fmt.Errorf("record %s at %d/%d, Append said %d/%d", rec, seg, off, p.Seg, p.Off)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != len(at) {
		t.Fatalf("replayed %d records, want %d", n, len(at))
	}
}
