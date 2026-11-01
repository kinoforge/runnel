package redisstore_test

import (
	"os"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/redisstore"
	"github.com/kinoforge/runnel/store/storetest"
)

func TestConformance(t *testing.T) {
	ran := false
	for _, name := range []string{"REDIS", "VALKEY", "DRAGONFLY"} {
		url := os.Getenv("RUNNEL_TEST_" + name)
		if url == "" {
			continue
		}
		ran = true
		t.Run(name, func(t *testing.T) {
			s, err := redisstore.Open(url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			storetest.Run(t, func(t *testing.T) runnel.Store { return s })
		})
	}
	if !ran {
		t.Skip("set RUNNEL_TEST_REDIS, RUNNEL_TEST_VALKEY or RUNNEL_TEST_DRAGONFLY")
	}
}
