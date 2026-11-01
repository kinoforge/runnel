package mysql_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/mysql"
	"github.com/kinoforge/runnel/store/storetest"
)

func TestConformance(t *testing.T) {
	dsn := os.Getenv("RUNNEL_TEST_MYSQL")
	if dsn == "" {
		t.Skip("RUNNEL_TEST_MYSQL not set")
	}
	var once sync.Once
	var shared runnel.Store
	storetest.Run(t, func(t *testing.T) runnel.Store {
		once.Do(func() {
			s, err := mysql.Open(dsn)
			if err != nil {
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
