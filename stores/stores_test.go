package stores_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/stores"
)

func TestOpenHybridAndMemory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, dsn := range []string{"memory:", "hybrid:" + filepath.Join(dir, "a"), "hybrid://" + filepath.Join(dir, "b") + "?sync=periodic"} {
		s, err := stores.Open(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: %v", dsn, err)
		}
		if _, ok := s.(*hybrid.Store); !ok {
			t.Fatalf("%s opened %T", dsn, s)
		}
		_ = s.Close()
	}
	for _, dsn := range []string{"hybrid:", "hybrid:" + dir + "?sync=sometimes", "nope:x", "plain"} {
		if s, err := stores.Open(ctx, dsn); err == nil {
			_ = s.Close()
			t.Fatalf("%s opened", dsn)
		}
	}
}
