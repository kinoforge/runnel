package hybrid

import "time"

// OpenRetiring opens a store whose finished jobs move to the cold store after
// age, checked every tick.
func OpenRetiring(opt Options, age, tick time.Duration) (*Store, error) {
	return open(opt, age, tick)
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
