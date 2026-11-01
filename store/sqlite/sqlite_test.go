package sqlite_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/sqlite"
	"github.com/kinoforge/runnel/store/storetest"
)

func TestConformance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runnel.db")
	var once sync.Once
	var shared runnel.Store
	storetest.Run(t, func(t *testing.T) runnel.Store {
		once.Do(func() {
			s, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			shared = s
		})
		return shared
	})
}
