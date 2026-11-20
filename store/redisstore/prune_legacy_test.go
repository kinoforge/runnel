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

func TestPruneReachesEntriesFromOlderVersions(t *testing.T) {
	spec.Rule(t, "T03", "J07")
	ran := false
	for _, name := range []string{"REDIS", "VALKEY", "DRAGONFLY"} {
		url := os.Getenv("RUNNEL_TEST_" + name)
		if url == "" {
			continue
		}
		ran = true
		t.Run(name, func(t *testing.T) { pruneLegacy(t, url) })
	}
	if !ran {
		t.Skip("set RUNNEL_TEST_REDIS, RUNNEL_TEST_VALKEY or RUNNEL_TEST_DRAGONFLY")
	}
}

func pruneLegacy(t *testing.T, url string) {
	ctx := context.Background()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	prefix := fmt.Sprintf("old%d", rand.Int64N(1<<40))
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
	res, err := c.Enqueue(ctx, "q", "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.Claim(ctx, "app", runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 1, Lease: time.Minute, Now: time.Now()})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim %v %v", jobs, err)
	}
	done := runnel.Decide(jobs[0], runnel.Outcome{Kind: runnel.OutcomeDone}, time.Now())
	if ok, err := s.Apply(ctx, "app", []runnel.Change{{Job: done}}, time.Now()); err != nil || !ok[0] {
		t.Fatalf("apply %v %v", ok, err)
	}
	ns := prefix + ":{app}:"
	score := rdb.ZScore(ctx, ns+"fin:completed", res.ID).Val()
	rdb.ZRem(ctx, ns+"fin:completed", res.ID)
	rdb.ZAdd(ctx, ns+"fin", redis.Z{Score: score, Member: res.ID})
	past := strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10)
	rdb.HSet(ctx, ns+"uniq", "legacy", "01a1fa4d-0000-7000-8000-000000000000|"+past)
	for range 3 {
		if _, err := s.Prune(ctx, "app", runnel.PruneRequest{States: []runnel.State{runnel.StateCompleted}, Before: time.Now().Add(time.Minute), Limit: 10}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Get(ctx, res.ID); err == nil {
		t.Fatal("a job indexed by an older version was never pruned")
	}
	if rdb.Exists(ctx, ns+"fin").Val() != 0 {
		t.Fatal("the old shared index was not drained")
	}
	if rdb.HExists(ctx, ns+"uniq", "legacy").Val() {
		t.Fatal("an expired unique key from an older version was never freed")
	}
}
