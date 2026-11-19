//go:build drill

package drill_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func snapshots(t *testing.T, dir string) []string {
	t.Helper()
	got, err := filepath.Glob(filepath.Join(dir, "snap-*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func checkpointed(t *testing.T, dir string, after []string) {
	t.Helper()
	eventually(t, "a new snapshot", time.Minute, func() bool {
		now := snapshots(t, dir)
		return len(now) > 0 && (len(after) == 0 || now[len(now)-1] != after[len(after)-1])
	})
}

func TestKillAfterColdChanges(t *testing.T) {
	spec.Rule(t, "D07", "X04")
	dir := t.TempDir()
	srv := serve(t, "hybrid:"+dir, "RUNNEL_HYBRID_HOT_JOBS=16", "RUNNEL_HYBRID_SPILL_EVERY=50ms",
		"RUNNEL_HYBRID_RETIRE_AFTER=100ms", "RUNNEL_HYBRID_RETIRE_EVERY=100ms",
		"RUNNEL_HYBRID_SNAPSHOT_BYTES=65536", "RUNNEL_HYBRID_CHECK_EVERY=200ms")
	setupNamespace(t, srv.url())
	c := client(srv.url())
	ctx := context.Background()
	expired, err := c.Enqueue(ctx, "late", "k", nil, runnel.TTL(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the late job to expire", 30*time.Second, func() bool {
		j, err := c.Get(ctx, expired.ID)
		return err == nil && j.State == runnel.StateExpired
	})
	payload := map[string]string{"pad": strings.Repeat("x", 512)}
	var ids []string
	batch := func() {
		for range 400 {
			res, err := c.Enqueue(ctx, "work", "k", payload)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, res.ID)
		}
	}
	batch()
	checkpointed(t, dir, nil)
	first := snapshots(t, dir)
	batch()
	checkpointed(t, dir, first)
	cancelled := map[string]bool{}
	for _, id := range ids[:20] {
		if _, err := c.Cancel(ctx, id); err != nil {
			t.Fatal(err)
		}
		cancelled[id] = true
	}
	if _, err := c.Retry(ctx, expired.ID); err != nil {
		t.Fatal(err)
	}
	srv.restart()
	jobs := listAll(t, srv.url())
	for id := range cancelled {
		if j := jobs[id]; j == nil || j.State != runnel.StateCancelled {
			t.Fatalf("cancelled job %s came back as %+v", id, j)
		}
	}
	if j := jobs[expired.ID]; j == nil || j.State != runnel.StateWaiting {
		t.Fatalf("retried job came back as %+v", j)
	}
	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range stats {
		if st.Queue == "work" && st.Counts[runnel.StateWaiting] != int64(len(ids)-len(cancelled)) {
			t.Fatalf("work queue counts %v after the kill, want %d waiting", st.Counts, len(ids)-len(cancelled))
		}
	}
	w := work(t, srv.url(), 2)
	eventually(t, "every job to finish", 2*time.Minute, func() bool {
		stats, err := c.Stats(ctx)
		if err != nil {
			return false
		}
		for _, st := range stats {
			if st.Queue == "work" && st.Counts[runnel.StateCompleted] == int64(len(ids)-len(cancelled)) {
				return true
			}
		}
		return false
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range ids {
		if want := map[bool]int{true: 0, false: 1}[cancelled[id]]; w.runs[id] != want {
			t.Errorf("job %s ran %d times, want %d", id, w.runs[id], want)
		}
	}
}
