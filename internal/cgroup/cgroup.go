package cgroup

import (
	"os"
	"strconv"
	"strings"
)

// cgroup v1 reports no limit as a number near the largest int64.
const noLimit = 1 << 60

var limitFiles = []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}

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
