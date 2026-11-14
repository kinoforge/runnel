package server

import (
	"context"
	"math"
	"net/http"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/cgroup"
)

type systemView struct {
	Version      string              `json:"version"`
	GoVersion    string              `json:"go_version"`
	StartedAt    time.Time           `json:"started_at"`
	Goroutines   int                 `json:"goroutines"`
	CPUs         int                 `json:"cpus"`
	Memory       memoryView          `json:"memory"`
	Storage      *runnel.StorageInfo `json:"storage"`
	StorageError string              `json:"storage_error,omitempty"`
}

type memoryView struct {
	Total     uint64 `json:"total_bytes"`
	Heap      uint64 `json:"heap_bytes"`
	NextGC    uint64 `json:"next_gc_bytes"`
	Limit     uint64 `json:"limit_bytes"`
	Container int64  `json:"container_limit_bytes"`
	GCCycles  uint64 `json:"gc_cycles"`
}

var memorySamples = []string{"/memory/classes/total:bytes", "/memory/classes/heap/objects:bytes", "/gc/heap/goal:bytes", "/gc/gomemlimit:bytes", "/gc/cycles/total:gc-cycles"}

func readMemory() memoryView {
	samples := make([]metrics.Sample, len(memorySamples))
	for i, name := range memorySamples {
		samples[i].Name = name
	}
	metrics.Read(samples)
	v := make([]uint64, len(samples))
	for i, s := range samples {
		if s.Value.Kind() == metrics.KindUint64 {
			v[i] = s.Value.Uint64()
		}
	}
	if v[3] == math.MaxInt64 {
		v[3] = 0
	}
	return memoryView{Total: v[0], Heap: v[1], NextGC: v[2], Limit: v[3], GCCycles: v[4], Container: cgroup.MemoryLimit()}
}

func (s *Server) system(w http.ResponseWriter, r *http.Request, _ principal) error {
	v := systemView{Version: s.cfg.Version, GoVersion: runtime.Version(), StartedAt: s.started, Goroutines: runtime.NumGoroutine(),
		CPUs: runtime.GOMAXPROCS(0), Memory: readMemory()}
	if sr, ok := s.store.(runnel.StorageReporter); ok {
		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.HealthTimeout)
		defer cancel()
		info, err := sr.Storage(ctx)
		if err != nil {
			s.log.Error("runnel: storage report failed", "err", err)
			v.StorageError = "storage report failed"
		} else {
			v.Storage = &info
		}
	}
	writeJSON(w, http.StatusOK, v)
	return nil
}
