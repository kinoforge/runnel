package hybrid

import "github.com/kinoforge/runnel/internal/cgroup"

const (
	cacheShare   = 4
	defaultBytes = 512 << 20
)

// defaultCache is a quarter of the container's memory limit, so Docker or
// Kubernetes limits size the cache without a setting of its own.
func defaultCache() int64 {
	if n := cgroup.MemoryLimit(); n > 0 {
		return n / cacheShare
	}
	return defaultBytes
}
