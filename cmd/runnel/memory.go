package main

import (
	"os"
	"runtime/debug"

	"github.com/kinoforge/runnel/internal/cgroup"
)

const memoryShare = 0.9

// fitContainer tells the garbage collector to keep the heap under 90% of the
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
