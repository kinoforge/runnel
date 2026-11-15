package redisstore_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/store/redisstore"
	"github.com/redis/go-redis/v9"
)

func TestMigrateRescoresWaitingJobs(t *testing.T) {
	spec.Rule(t, "C02", "X02")
	ran := false
	for _, name := range []string{"REDIS", "VALKEY", "DRAGONFLY"} {
		url := os.Getenv("RUNNEL_TEST_" + name)
		if url == "" {
			continue
		}
		ran = true
		t.Run(name, func(t *testing.T) { rescoreOldScores(t, url) })
	}
	if !ran {
		t.Skip("set RUNNEL_TEST_REDIS, RUNNEL_TEST_VALKEY or RUNNEL_TEST_DRAGONFLY")
	}
}

func rescoreOldScores(t *testing.T, url string) {
	ctx := context.Background()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	prefix := fmt.Sprintf("mig%d", rand.Int64N(1<<40))
	t.Cleanup(func() {
		keys, _ := rdb.Keys(ctx, prefix+":*").Result()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
		_ = rdb.Close()
	})
	s := redisstore.New(rdb, prefix)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	c := runnel.NewClient(s, "app")
	late, _ := c.Enqueue(ctx, "q", "k", nil)
	early, _ := c.Enqueue(ctx, "q", "k", nil)
	second := time.Now().Add(-time.Minute).Truncate(time.Second).UnixMilli()
	ready := prefix + ":{app}:ready:q"
	for id, runAt := range map[string]int64{late.ID: second + 900, early.ID: second + 100} {
		rdb.HSet(ctx, prefix+":{app}:j:"+id, "run_at", strconv.FormatInt(runAt, 10))
		rdb.ZAdd(ctx, ready, redis.Z{Score: float64(1000*(int64(1)<<40) + second/1000), Member: id})
	}
	rdb.Del(ctx, prefix+":schema")
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := rdb.ZScore(ctx, ready, early.ID).Val(); got != float64(1000*(int64(1)<<42)+second+100) {
		t.Fatalf("score after migrate %v", got)
	}
	if v, _ := rdb.Get(ctx, prefix+":schema").Int(); v != 2 {
		t.Fatalf("schema %d after migrate", v)
	}
	jobs, err := s.Claim(ctx, "app", runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 2, Lease: time.Minute, Now: time.Now()})
	if err != nil || len(jobs) != 2 || jobs[0].ID != early.ID || jobs[1].ID != late.ID {
		t.Fatalf("claim after migrate %v %v", jobs, err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate again: %v", err)
	}
}
