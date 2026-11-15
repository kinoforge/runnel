package hybrid

import (
	"math"
	"runtime/debug"
	"time"

	"github.com/kinoforge/runnel/store/hybrid/cold"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

// Options configures a store kept on disk. An empty Dir keeps it in memory
// only. Fields left at zero take their value from DefaultOptions.
type Options struct {
	Dir  string
	Sync wal.SyncMode
	// SyncInterval is how often SyncPeriodic fsyncs.
	SyncInterval time.Duration
	// SnapshotBytes is the log size that triggers a snapshot.
	SnapshotBytes int64
	// CheckEvery is how often the log size is checked for a snapshot.
	CheckEvery time.Duration

	// HotPercent is the share of the memory limit waiting jobs may take,
	// counted at JobBytes each. HotJobs, when set, is the count itself; with
	// no memory limit, it is NoLimitHotJobs.
	HotPercent     int64
	JobBytes       int64
	HotJobs        int64
	MinHotJobs     int64
	NoLimitHotJobs int64

	// RetireAfter is how long a finished job stays in memory before it moves
	// to the cold store, checked every RetireEvery, RetireBatch at a time.
	RetireAfter time.Duration
	RetireEvery time.Duration
	RetireBatch int

	// Waiting jobs past the memory budget move to disk every SpillEvery and
	// come back in batches as workers reach them.
	SpillEvery  time.Duration
	SpillBatch  int64
	RefillBatch int64
	RefillAhead int64
	BatchBytes  int
	MaxRefills  int
	WakeBatch   int

	MinFreeBytes   int64
	DiskCheckEvery time.Duration

	// Cold sizes the cold store's caches.
	Cold cold.Options
}

// DefaultOptions holds the value of every option left at zero.
var DefaultOptions = Options{
	SyncInterval:   100 * time.Millisecond,
	SnapshotBytes:  64 << 20,
	CheckEvery:     time.Second,
	HotPercent:     25,
	JobBytes:       300,
	MinHotJobs:     1 << 14,
	NoLimitHotJobs: 1 << 20,
	RetireAfter:    time.Second,
	RetireEvery:    time.Second,
	RetireBatch:    slabSize,
	SpillEvery:     100 * time.Millisecond,
	SpillBatch:     slabSize,
	RefillBatch:    2 * slabSize,
	RefillAhead:    8 * slabSize,
	BatchBytes:     4 << 20,
	MaxRefills:     8,
	WakeBatch:      16 * slabSize,
	MinFreeBytes:   64 << 20,
	DiskCheckEvery: 100 * time.Millisecond,
	Cold:           cold.DefaultOptions,
}

func (o Options) withDefaults() Options {
	d := DefaultOptions
	pick(&o.SyncInterval, d.SyncInterval)
	pick(&o.SnapshotBytes, d.SnapshotBytes)
	pick(&o.CheckEvery, d.CheckEvery)
	pick(&o.HotPercent, d.HotPercent)
	pick(&o.JobBytes, d.JobBytes)
	pick(&o.MinHotJobs, d.MinHotJobs)
	pick(&o.NoLimitHotJobs, d.NoLimitHotJobs)
	pick(&o.RetireAfter, d.RetireAfter)
	pick(&o.RetireEvery, d.RetireEvery)
	pick(&o.RetireBatch, d.RetireBatch)
	pick(&o.SpillEvery, d.SpillEvery)
	pick(&o.SpillBatch, d.SpillBatch)
	pick(&o.RefillBatch, d.RefillBatch)
	pick(&o.RefillAhead, d.RefillAhead)
	pick(&o.BatchBytes, d.BatchBytes)
	pick(&o.MaxRefills, d.MaxRefills)
	pick(&o.WakeBatch, d.WakeBatch)
	pick(&o.MinFreeBytes, d.MinFreeBytes)
	pick(&o.DiskCheckEvery, d.DiskCheckEvery)
	o.Cold = o.Cold.WithDefaults()
	return o
}

func pick[T int | int64 | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

func (o Options) hotBudget() int64 {
	if o.HotJobs > 0 {
		return o.HotJobs
	}
	limit := debug.SetMemoryLimit(-1)
	if limit == math.MaxInt64 {
		return o.NoLimitHotJobs
	}
	return max(o.MinHotJobs, limit*o.HotPercent/100/o.JobBytes)
}
