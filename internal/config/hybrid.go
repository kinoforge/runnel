package config

import (
	"time"

	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/cold"
	"github.com/kinoforge/runnel/store/hybrid/wal"
)

type Memory struct {
	HeapPercent int64 `yaml:"heap_percent"`
}

type Hybrid struct {
	Sync          string        `yaml:"sync"`
	SyncInterval  time.Duration `yaml:"sync_interval"`
	SnapshotBytes int64         `yaml:"snapshot_bytes"`
	CheckEvery    time.Duration `yaml:"check_every"`

	HotPercent     int64 `yaml:"hot_percent"`
	JobBytes       int64 `yaml:"job_bytes"`
	HotJobs        int64 `yaml:"hot_jobs"`
	MinHotJobs     int64 `yaml:"min_hot_jobs"`
	NoLimitHotJobs int64 `yaml:"no_limit_hot_jobs"`

	RetireAfter time.Duration `yaml:"retire_after"`
	RetireEvery time.Duration `yaml:"retire_every"`
	RetireBatch int           `yaml:"retire_batch"`

	SpillEvery  time.Duration `yaml:"spill_every"`
	SpillBatch  int64         `yaml:"spill_batch"`
	RefillBatch int64         `yaml:"refill_batch"`
	RefillAhead int64         `yaml:"refill_ahead"`
	BatchBytes  int           `yaml:"batch_bytes"`
	MaxRefills  int           `yaml:"max_refills"`
	WakeBatch   int           `yaml:"wake_batch"`

	ColdCacheBytes    int64 `yaml:"cold_cache_bytes"`
	ColdMemTableBytes int64 `yaml:"cold_memtable_bytes"`
	ColdBloomBits     int   `yaml:"cold_bloom_bits"`
}

func defaultHybrid() Hybrid {
	d := hybrid.DefaultOptions
	return Hybrid{
		Sync: "each", SyncInterval: d.SyncInterval, SnapshotBytes: d.SnapshotBytes, CheckEvery: d.CheckEvery,
		HotPercent: d.HotPercent, JobBytes: d.JobBytes, HotJobs: d.HotJobs, MinHotJobs: d.MinHotJobs, NoLimitHotJobs: d.NoLimitHotJobs,
		RetireAfter: d.RetireAfter, RetireEvery: d.RetireEvery, RetireBatch: d.RetireBatch,
		SpillEvery: d.SpillEvery, SpillBatch: d.SpillBatch, RefillBatch: d.RefillBatch, RefillAhead: d.RefillAhead,
		BatchBytes: d.BatchBytes, MaxRefills: d.MaxRefills, WakeBatch: d.WakeBatch,
		ColdCacheBytes: d.Cold.CacheBytes, ColdMemTableBytes: d.Cold.MemTableBytes, ColdBloomBits: d.Cold.BloomBits,
	}
}

func (h Hybrid) Options() (hybrid.Options, error) {
	sync, err := wal.ParseSync(h.Sync)
	return hybrid.Options{
		Sync: sync, SyncInterval: h.SyncInterval, SnapshotBytes: h.SnapshotBytes, CheckEvery: h.CheckEvery,
		HotPercent: h.HotPercent, JobBytes: h.JobBytes, HotJobs: h.HotJobs, MinHotJobs: h.MinHotJobs, NoLimitHotJobs: h.NoLimitHotJobs,
		RetireAfter: h.RetireAfter, RetireEvery: h.RetireEvery, RetireBatch: h.RetireBatch,
		SpillEvery: h.SpillEvery, SpillBatch: h.SpillBatch, RefillBatch: h.RefillBatch, RefillAhead: h.RefillAhead,
		BatchBytes: h.BatchBytes, MaxRefills: h.MaxRefills, WakeBatch: h.WakeBatch,
		Cold: cold.Options{CacheBytes: h.ColdCacheBytes, MemTableBytes: h.ColdMemTableBytes, BloomBits: h.ColdBloomBits},
	}, err
}
