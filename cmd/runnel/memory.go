package main

import (
	"os"
	"runtime/debug"

	"github.com/kinoforge/runnel/internal/cgroup"
)

// memoryShare leaves room in the container for the page cache, which holds
// the store's unwritten file data and counts against the same limit.
const memoryShare = 0.7

// fitContainer tells the garbage collector to keep the heap under 70% of the
// container's memory limit, unless GOMEMLIMIT already says otherwise. Go does
// not read the container limit on its own.
func fitContainer() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	if n := cgroup.MemoryLimit(); n > 0 {
		debug.SetMemoryLimit(int64(float64(n) * memoryShare))
	}
}
