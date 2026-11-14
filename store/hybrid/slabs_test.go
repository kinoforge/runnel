package hybrid

import "testing"

func TestSlabsReleaseEmptyBlocks(t *testing.T) {
	var s slabs
	var ids []int32
	for n := range 3 * slabSize {
		ids = append(ids, s.put(job{version: int64(n + 1)}))
	}
	for _, i := range ids[:slabSize] {
		s.clear(i)
	}
	if s.blocks[0] != nil || len(s.free) != 0 {
		t.Fatalf("empty block kept: %v blocks, %d free", len(s.blocks), len(s.free))
	}
	s.clear(ids[slabSize])
	used := map[int32]bool{}
	for _, i := range ids[slabSize+1:] {
		used[i] = true
	}
	for range slabSize + 1 {
		i := s.put(job{version: -1})
		if used[i] {
			t.Fatalf("slot %d handed out twice", i)
		}
		used[i] = true
	}
	if len(s.blocks) != 3 || s.blocks[0] == nil {
		t.Fatalf("%d blocks, want the released place reused", len(s.blocks))
	}
	for _, i := range ids[slabSize+1:] {
		if s.at(i).version <= 0 {
			t.Fatalf("slot %d lost its job", i)
		}
	}
}
