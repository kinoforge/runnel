package hybrid

import "time"

func OpenRetiring(opt Options, age, tick time.Duration) (*Store, error) {
	opt.RetireAfter, opt.RetireEvery, opt.SpillEvery = age, tick, tick
	return Open(opt)
}

func OpenSpilling(opt Options, hot int64, tick time.Duration) (*Store, error) {
	opt.HotJobs, opt.RetireAfter, opt.RetireEvery, opt.SpillEvery = hot, time.Nanosecond, tick, tick
	return Open(opt)
}

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
