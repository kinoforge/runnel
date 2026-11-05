package hybrid

const (
	slabBits  = 12
	slabSize  = 1 << slabBits
	slabMask  = slabSize - 1
	slabStart = 64
)

// slabs hold job records in blocks of slabSize. Growing one slice would copy
// every record and briefly need twice their memory; blocks never move once
// full. The last block grows until full, so small namespaces stay small.
type slabs struct {
	blocks [][]job
	n      int32
}

func (s *slabs) at(i int32) *job { return &s.blocks[i>>slabBits][i&slabMask] }

// add appends h and returns its index.
func (s *slabs) add(h job) int32 {
	last := len(s.blocks) - 1
	if last < 0 || len(s.blocks[last]) == slabSize {
		s.blocks = append(s.blocks, make([]job, 0, slabStart))
		last++
	}
	b := s.blocks[last]
	if len(b) == cap(b) {
		grown := make([]job, len(b), min(2*cap(b), slabSize))
		copy(grown, b)
		b = grown
	}
	s.blocks[last] = append(b, h)
	s.n++
	return s.n - 1
}
