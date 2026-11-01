package runnel

import (
	"context"
	"reflect"
	"sync"
	"time"
)

const (
	clockResync       = 30 * time.Second
	clockReadTimeout  = 2 * time.Second
	clockRetryBackoff = 5 * time.Second
)

// Clock tells the time that leases, schedules and retries are measured in.
type Clock interface {
	Now() time.Time
}

// TimeSource is implemented by stores that can report their own time, so that
// every process sharing a store measures time the same way.
type TimeSource interface {
	Time(ctx context.Context) (time.Time, error)
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// SystemClock is the local wall clock.
var SystemClock Clock = systemClock{}

// StoreClock follows the clock of a TimeSource. Every 30 seconds it samples the
// source three times, keeps the offset measured over the fastest round trip and
// applies it on every read, so reading the time costs no round trip.
type StoreClock struct {
	src     TimeSource
	resync  time.Duration
	timeout time.Duration

	mu     sync.Mutex
	offset time.Duration
	next   time.Time
	busy   bool
}

// NewStoreClock returns a clock that follows src.
func NewStoreClock(src TimeSource) *StoreClock {
	return &StoreClock{src: src, resync: clockResync, timeout: clockReadTimeout}
}

// Now returns the source's current time. The first read waits for one sample;
// later reads refresh the offset in the background.
func (c *StoreClock) Now() time.Time {
	c.mu.Lock()
	first := c.next.IsZero()
	start := !c.busy && !time.Now().Before(c.next)
	if start {
		c.busy = true
	}
	c.mu.Unlock()
	if start {
		if first {
			c.sync()
		} else {
			go c.sync()
		}
	}
	c.mu.Lock()
	off := c.offset
	c.mu.Unlock()
	return time.Now().Add(off).UTC()
}

// Offset is how far the source's clock is ahead of the local one.
func (c *StoreClock) Offset() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

func (c *StoreClock) sync() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	best, rtt, ok := time.Duration(0), time.Duration(-1), false
	for range 3 {
		before := time.Now()
		at, err := c.src.Time(ctx)
		after := time.Now()
		if err != nil {
			break
		}
		if d := after.Sub(before); rtt < 0 || d < rtt {
			best, rtt, ok = at.Sub(before.Add(d/2)), d, true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.busy = false
	if !ok {
		c.next = time.Now().Add(clockRetryBackoff)
		return
	}
	c.offset, c.next = best, time.Now().Add(c.resync)
}

var clocks sync.Map

// ClockOf returns the shared clock for a store: the store's own clock when it
// has one, a StoreClock when it is a TimeSource, the system clock otherwise.
func ClockOf(s Store) Clock {
	if owner, ok := s.(interface{ Clock() Clock }); ok {
		return owner.Clock()
	}
	src, ok := s.(TimeSource)
	if !ok {
		return SystemClock
	}
	if !reflect.TypeOf(s).Comparable() {
		return NewStoreClock(src)
	}
	if c, ok := clocks.Load(s); ok {
		return c.(Clock)
	}
	c, _ := clocks.LoadOrStore(s, NewStoreClock(src))
	return c.(Clock)
}
