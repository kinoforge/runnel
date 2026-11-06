package hybrid

import "time"

// OpenRetiring opens a store whose finished jobs move to the cold store after
// age, checked every tick.
func OpenRetiring(opt Options, age, tick time.Duration) (*Store, error) {
	opt.RetireAfter, opt.RetireEvery, opt.SpillEvery = age, tick, tick
	return Open(opt)
}

// OpenSpilling opens a store that holds at most hot waiting jobs in memory,
// checked every tick, and moves finished jobs to the cold store at once.
func OpenSpilling(opt Options, hot int64, tick time.Duration) (*Store, error) {
	opt.HotJobs, opt.RetireAfter, opt.RetireEvery, opt.SpillEvery = hot, time.Nanosecond, tick, tick
	return Open(opt)
}

// Cold returns how many jobs of ns the cold store holds.
func (s *Store) Cold(ns string) int64 {
	sp := s.space(ns)
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var n int64
	for _, c := range sp.coldCounts {
		for _, v := range c {
			n += v
		}
	}
	return n
}
