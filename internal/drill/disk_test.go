//go:build drill

package drill_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

const ballastBytes = 16 << 20

type smallDisk struct {
	ballast string
	store   string
}

func newSmallDisk(t *testing.T) smallDisk {
	t.Helper()
	root := os.Getenv("RUNNEL_DRILL_DISK")
	if root == "" {
		t.Fatal("RUNNEL_DRILL_DISK must name a small filesystem: run make drill")
	}
	base, err := os.MkdirTemp(root, "disk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	d := smallDisk{ballast: filepath.Join(base, "ballast"), store: "hybrid:" + filepath.Join(base, "store")}
	if err := os.WriteFile(d.ballast, make([]byte, ballastBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	return d
}

func (d smallDisk) free(t *testing.T) {
	t.Helper()
	if err := os.Remove(d.ballast); err != nil {
		t.Fatal(err)
	}
}

func fillUntilRefused(t *testing.T, url string) ([]string, error) {
	t.Helper()
	c := client(url)
	big := strings.Repeat("x", 256<<10)
	var acked []string
	var last error
	failures := 0
	for failures < 20 && len(acked) < 2000 {
		res, err := c.Enqueue(context.Background(), "work", "k", big)
		if err == nil {
			acked = append(acked, res.ID)
			failures = 0
			continue
		}
		failures++
		last = err
		if errors.Is(err, runnel.ErrInvalid) {
			t.Fatalf("a full disk answered as bad input: %v", err)
		}
	}
	if failures < 20 {
		t.Fatalf("disk never filled after %d jobs", len(acked))
	}
	t.Logf("enqueues refused after %d jobs of 256 KiB: %v", len(acked), last)
	return acked, last
}

func TestDiskLow(t *testing.T) {
	spec.Rule(t, "D03")
	d := newSmallDisk(t)
	srv := serve(t, d.store)
	setupNamespace(t, srv.url())
	acked, last := fillUntilRefused(t, srv.url())
	if !errors.Is(last, runnel.ErrNoSpace) {
		t.Fatalf("enqueue on a low disk refused with %v, want ErrNoSpace", last)
	}
	if code := health(t, srv.url()); code != http.StatusOK {
		t.Fatalf("healthz while new jobs are refused for space: %d, want 200", code)
	}
	work(t, srv.url(), 1)
	allDone(t, srv.url(), acked, time.Minute)
	d.free(t)
	c := client(srv.url())
	eventually(t, "an enqueue taken once space came back", 10*time.Second, func() bool {
		_, err := c.Enqueue(context.Background(), "work", "k", "small")
		return err == nil
	})
	if !srv.running() {
		t.Fatal("server exited on a low disk")
	}
	srv.restart()
	if lost := missing(acked, listAll(t, srv.url())); len(lost) != 0 {
		t.Fatalf("%d of %d acknowledged jobs lost after a low disk and a restart: %v", len(lost), len(acked), lost[:min(5, len(lost))])
	}
}

func TestDiskFull(t *testing.T) {
	spec.Rule(t, "D06", "X04")
	d := newSmallDisk(t)
	srv := serve(t, d.store, "RUNNEL_HYBRID_MIN_FREE_BYTES=1")
	setupNamespace(t, srv.url())
	acked, _ := fillUntilRefused(t, srv.url())
	if !srv.running() {
		t.Fatal("server exited when the disk filled")
	}
	if code := health(t, srv.url()); code != http.StatusServiceUnavailable {
		t.Fatalf("healthz on a store stopped by a full disk: %d, want 503", code)
	}
	c := client(srv.url())
	if _, err := c.Enqueue(context.Background(), "work", "k", "small"); err == nil {
		t.Fatal("a store stopped by a full disk acknowledged a write")
	}
	d.free(t)
	srv.restart()
	if lost := missing(acked, listAll(t, srv.url())); len(lost) != 0 {
		t.Fatalf("%d of %d acknowledged jobs lost after a full disk and a restart: %v", len(lost), len(acked), lost[:min(5, len(lost))])
	}
	if _, err := c.Enqueue(context.Background(), "work", "k", "small"); err != nil {
		t.Fatalf("write after space came back and a restart: %v", err)
	}
	if code := health(t, srv.url()); code != http.StatusOK {
		t.Fatalf("healthz after the restart: %d", code)
	}
}

func health(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}
