// Package cgroup reads the memory limit a container runs under, so Runnel sizes
// itself from Docker or Kubernetes settings instead of its own.
package cgroup

import (
	"os"
	"strconv"
	"strings"
)

// noLimit is above any real limit; cgroup v1 reports "unlimited" as a value near
// the largest int64.
const noLimit = 1 << 60

var limitFiles = []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}

// MemoryLimit returns the container's memory limit in bytes, or 0 when there is
// none.
func MemoryLimit() int64 { return readLimit(limitFiles) }

func readLimit(paths []string) int64 {
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && n > 0 && n < noLimit {
			return n
		}
	}
	return 0
}
