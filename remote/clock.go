package remote

import (
	"sync"
	"time"
)

type serverClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *serverClock) observe(server, local time.Time) {
	if server.IsZero() {
		return
	}
	c.mu.Lock()
	c.offset = server.Sub(local)
	c.mu.Unlock()
}

func (c *serverClock) Now() time.Time {
	c.mu.Lock()
	off := c.offset
	c.mu.Unlock()
	return time.Now().Add(off).UTC()
}
