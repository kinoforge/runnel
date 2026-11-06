package main

import (
	"os"
	"runtime/debug"

	"github.com/kinoforge/runnel/internal/cgroup"
)

// fitContainer tells the garbage collector to keep the heap under percent of
// the container's memory limit, leaving the rest to the page cache, which
// holds the store's file data and counts against the same limit. GOMEMLIMIT,
// when set, wins. Go does not read the container limit on its own.
func fitContainer(percent int64) {
	if os.Getenv("GOMEMLIMIT") != "" || percent <= 0 {
		return
	}
	if n := cgroup.MemoryLimit(); n > 0 {
		debug.SetMemoryLimit(n * percent / 100)
	}
}
