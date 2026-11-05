package hybrid

import "slices"

const (
	slabBits  = 12
	slabSize  = 1 << slabBits
	slabMask  = slabSize - 1
	slabStart = 64
)

// slabs hold job records in blocks of slabSize. Growing one slice would copy
// every record and briefly need twice their memory; blocks never move. New
// records fill the tail block, which grows until full, so small namespaces
// stay small. A block whose records are all gone is released, and its place
// is reused for a later tail, so memory follows the jobs held, not the most
// ever held.
type slabs struct {
	blocks [][]job
	live   []int32
	free   []int32
	empty  []int32
	tail   int32
}

func (s *slabs) at(i int32) *job { return &s.blocks[i>>slabBits][i&slabMask] }

// put stores h in a free slot and returns its index.
func (s *slabs) put(h job) int32 {
	if n := len(s.free); n > 0 {
		i := s.free[n-1]
		s.free = s.free[:n-1]
		*s.at(i) = h
		s.live[i>>slabBits]++
		return i
	}
	if len(s.blocks) == 0 || len(s.blocks[s.tail]) == slabSize {
		s.newTail()
	}
	b := s.blocks[s.tail]
	if len(b) == cap(b) {
		grown := make([]job, len(b), min(2*cap(b), slabSize))
		copy(grown, b)
		b = grown
	}
	s.blocks[s.tail] = append(b, h)
	s.live[s.tail]++
	return s.tail<<slabBits | int32(len(b))
}

// newTail starts a tail block in a released place, or after the last block.
func (s *slabs) newTail() {
	if n := len(s.empty); n > 0 {
		s.tail = s.empty[n-1]
		s.empty = s.empty[:n-1]
	} else {
		s.tail = int32(len(s.blocks))
		s.blocks = append(s.blocks, nil)
		s.live = append(s.live, 0)
	}
	s.blocks[s.tail] = make([]job, 0, slabStart)
}

// clear frees slot i, releasing its block once the block is empty.
func (s *slabs) clear(i int32) {
	b := i >> slabBits
	*s.at(i) = job{}
	s.live[b]--
	if s.live[b] > 0 || b == s.tail {
		s.free = append(s.free, i)
		return
	}
	s.blocks[b] = nil
	s.empty = append(s.empty, b)
	s.free = slices.DeleteFunc(s.free, func(f int32) bool { return f>>slabBits == b })
}
