package main

import (
	"os"
	"runtime/debug"

	"github.com/kinoforge/runnel/internal/cgroup"
)

func fitContainer(percent int64) {
	if os.Getenv("GOMEMLIMIT") != "" || percent <= 0 {
		return
	}
	if n := cgroup.MemoryLimit(); n > 0 {
		debug.SetMemoryLimit(n * percent / 100)
	}
}
