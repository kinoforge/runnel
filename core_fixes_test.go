package runnel

import (
	"context"
	"errors"
	"math"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestLastTickWhenFarBehind(t *testing.T) {
	spec.Rule(t, "T02")
	now := time.Date(2026, 11, 20, 10, 32, 17, 0, time.UTC)
	cases := []struct {
		spec string
		from time.Time
		want time.Time
	}{
		{"*/5 * * * *", now.Add(-30 * 24 * time.Hour), time.Date(2026, 11, 20, 10, 30, 0, 0, time.UTC)},
		{"0 9 * * *", now.Add(-5 * 365 * 24 * time.Hour), time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)},
		{"* 9 * * 1", now.Add(-60 * 24 * time.Hour), time.Date(2026, 11, 16, 9, 59, 0, 0, time.UTC)},
		{"*/5 * * * *", now.Add(-12 * time.Minute), time.Date(2026, 11, 20, 10, 30, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		sp, err := parseSpec(c.spec, "")
		if err != nil {
			t.Fatal(err)
		}
		first := sp.Next(c.from)
		if got := lastTick(sp, first, now); !got.Equal(c.want) {
			t.Errorf("%s from %s: %s, want %s", c.spec, c.from, got, c.want)
		}
	}
}

func TestScheduleLeavesRoomForItsMeta(t *testing.T) {
	spec.Rule(t, "T02", "J02")
	s := Schedule{Name: "s", Spec: "@hourly", Queue: "q", Kind: "k", Meta: map[string]string{}}
	for i := range DefaultLimits.MaxMeta - 2 {
		s.Meta[string(rune('a'+i))] = "v"
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("a schedule with room for its meta: %v", err)
	}
	s.Meta["zz"] = "v"
	if err := s.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a schedule whose firings would carry too much meta: %v", err)
	}
	if err := (Schedule{Name: "s", Spec: "@hourly", Queue: "q", Kind: "k", MaxAttempts: -1}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative max attempts: %v", err)
	}
}

func TestNumbersThatAreNotFiniteAreRefused(t *testing.T) {
	spec.Rule(t, "X03", "J02")
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := (QueueConfig{Name: "q", Rate: v}).Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("queue rate %v: %v", v, err)
		}
		if err := (QueueConfig{Name: "q", GroupRate: v}).Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("queue group rate %v: %v", v, err)
		}
		if err := (PoolConfig{Name: "p", Rate: v}).Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("pool rate %v: %v", v, err)
		}
		if err := (QueueConfig{Name: "q", Backoff: Backoff{Factor: v}}).Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("queue backoff factor %v: %v", v, err)
		}
		j, _ := NewJob("q", "k", nil, WithBackoff(Backoff{Jitter: v}))
		if err := j.Prepare(time.Now(), DefaultLimits); !errors.Is(err, ErrInvalid) {
			t.Errorf("job backoff jitter %v: %v", v, err)
		}
	}
	j, _ := NewJob("q", "k", nil, In(-time.Second))
	if err := j.Prepare(time.Now(), DefaultLimits); !errors.Is(err, ErrInvalid) {
		t.Errorf("negative delay: %v", err)
	}
}

func TestReservedAddressesAreNotPublic(t *testing.T) {
	spec.Rule(t, "H04")
	for _, a := range []string{"192.0.2.10", "198.51.100.1", "203.0.113.9", "192.88.99.1", "64:ff9b:1::1", "100::1", "2001::1", "2002:c000:0204::1", "::ffff:0:a00:1", "fec0::1"} {
		if PublicAddress(netip.MustParseAddr(a)) {
			t.Errorf("%s counted as public", a)
		}
	}
	for _, a := range []string{"93.184.215.14", "2606:4700::6810:84e5"} {
		if !PublicAddress(netip.MustParseAddr(a)) {
			t.Errorf("%s counted as private", a)
		}
	}
}

func TestARunTrackedAgainEndsTheOldOne(t *testing.T) {
	spec.Rule(t, "L05")
	w := NewWorker(nil, "ns", WorkerConfig{Queues: []string{"q"}})
	j := &Job{ID: NewID()}
	old, oldCtx := w.track(context.Background(), j)
	_, newCtx := w.track(context.Background(), j)
	if !old.lost.Load() || !errors.Is(context.Cause(oldCtx), ErrLost) {
		t.Fatalf("the replaced run goes on: lost %v, cause %v", old.lost.Load(), context.Cause(oldCtx))
	}
	if newCtx.Err() != nil {
		t.Fatal("the new run was cancelled")
	}
}

type flakyEnds struct {
	mu    sync.Mutex
	calls int
	chans []chan string
}

func (f *flakyEnds) Ended(ctx context.Context, ns string) (<-chan string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	ch := make(chan string, 1)
	f.chans = append(f.chans, ch)
	return ch, nil
}

func TestWaitHubReconnectsAfterItsFeedEnds(t *testing.T) {
	spec.Rule(t, "W01")
	src := &flakyEnds{}
	h := &endHub{src: src, ns: "ns", subs: map[string]map[chan struct{}]struct{}{}}
	ch, live, done := h.watch("a")
	if !live {
		t.Fatal("first watch is not live")
	}
	src.mu.Lock()
	close(src.chans[0])
	src.mu.Unlock()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("a watcher was not woken when the feed ended")
	}
	done()
	ch2, live, done2 := h.watch("b")
	defer done2()
	if !live {
		t.Fatal("watch after the feed ended is not live")
	}
	src.mu.Lock()
	calls := src.calls
	src.chans[1] <- "b"
	src.mu.Unlock()
	if calls != 2 {
		t.Fatalf("the feed was opened %d times, want 2", calls)
	}
	select {
	case <-ch2:
	case <-time.After(2 * time.Second):
		t.Fatal("no wakeup from the new feed")
	}
}
