package runnel

import (
	"context"
	"sync"
	"time"
)

const (
	hubIdleTimeout = time.Minute
	waitPollMin    = 50 * time.Millisecond
	waitPollMax    = time.Second
)

type endHub struct {
	src EndWatcher
	ns  string

	mu    sync.Mutex
	subs  map[string]map[chan struct{}]struct{}
	n     int
	stop  context.CancelFunc
	idle  *time.Timer
	ready bool
}

func (h *endHub) watch(id string) (<-chan struct{}, bool, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.idle != nil {
		h.idle.Stop()
		h.idle = nil
	}
	if h.stop == nil {
		ctx, cancel := context.WithCancel(context.Background())
		ends, err := h.src.Ended(ctx, h.ns)
		if err != nil {
			cancel()
		} else {
			h.stop, h.ready = cancel, true
			go h.forward(ctx, ends)
		}
	}
	if h.subs[id] == nil {
		h.subs[id] = map[chan struct{}]struct{}{}
	}
	h.subs[id][ch] = struct{}{}
	h.n++
	live := h.ready
	return ch, live, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[id], ch)
		if len(h.subs[id]) == 0 {
			delete(h.subs, id)
		}
		if h.n--; h.n == 0 && h.stop != nil {
			h.idle = time.AfterFunc(hubIdleTimeout, h.shutdown)
		}
	}
}

func (h *endHub) shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n == 0 && h.stop != nil {
		h.stop()
		h.stop, h.ready, h.idle = nil, false, nil
	}
}

func (h *endHub) forward(ctx context.Context, ends <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-ends:
			h.mu.Lock()
			for ch := range h.subs[id] {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
			h.mu.Unlock()
		}
	}
}

func (c *Client) ends() *endHub {
	src, ok := c.store.(EndWatcher)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hub == nil {
		c.hub = &endHub{src: src, ns: c.ns, subs: map[string]map[chan struct{}]struct{}{}}
	}
	return c.hub
}

// Wait blocks until the job is completed, dead or cancelled and returns it.
// When ctx ends first it returns the job as last read together with the
// context's error. Stores that announce finished jobs wake the wait at once;
// others are read with a short backoff.
func (c *Client) Wait(ctx context.Context, id string) (*Job, error) {
	var signal <-chan struct{}
	live := false
	if h := c.ends(); h != nil {
		var done func()
		signal, live, done = h.watch(id)
		defer done()
	}
	pause := waitPollMin
	if live {
		pause = time.Second
	}
	var last *Job
	for {
		j, err := c.Get(ctx, id)
		if err != nil {
			if last != nil && ctx.Err() != nil {
				return last, ctx.Err()
			}
			return nil, err
		}
		if j.State.Final() {
			return j, nil
		}
		last = j
		t := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			t.Stop()
			return last, ctx.Err()
		case <-signal:
		case <-t.C:
		}
		t.Stop()
		if !live {
			pause = min(pause*2, waitPollMax)
		}
	}
}

// EnqueueAndWait enqueues a job and waits for it to finish, as Wait does. A
// duplicate of a unique job waits for the job already holding the key.
func (c *Client) EnqueueAndWait(ctx context.Context, queue, kind string, payload any, opts ...Option) (*Job, error) {
	res, err := c.Enqueue(ctx, queue, kind, payload, opts...)
	if err != nil {
		return nil, err
	}
	return c.Wait(ctx, res.ID)
}
