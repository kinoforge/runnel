package redisstore

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	schemaVersion = 2
	readyStep     = int64(1) << 42
	rescoreBatch  = 500
)

func readyScore(priority, runAt int64) float64 { return float64((1000-priority)*readyStep + runAt) }

func (s *Store) Migrate(ctx context.Context) error {
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return err
	}
	key := s.prefix + ":schema"
	v, err := s.rdb.Get(ctx, key).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if v >= schemaVersion {
		return nil
	}
	if err := s.eachNode(ctx, s.rescore); err != nil {
		return err
	}
	return s.rdb.Set(ctx, key, schemaVersion, 0).Err()
}

func (s *Store) eachNode(ctx context.Context, fn func(context.Context, redis.Cmdable) error) error {
	if c, ok := s.rdb.(*redis.ClusterClient); ok {
		return c.ForEachMaster(ctx, func(ctx context.Context, n *redis.Client) error { return fn(ctx, n) })
	}
	return fn(ctx, s.rdb)
}

func (s *Store) rescore(ctx context.Context, node redis.Cmdable) error {
	iter := node.ScanType(ctx, 0, s.prefix+":{*}:ready:*", rescoreBatch, "zset").Iterator()
	for iter.Next(ctx) {
		if err := s.rescoreSet(ctx, iter.Val()); err != nil {
			return err
		}
	}
	return iter.Err()
}

func (s *Store) rescoreSet(ctx context.Context, set string) error {
	end := strings.Index(set, "}:")
	if end < 0 {
		return nil
	}
	jobs := set[:end+2] + "j:"
	var cursor uint64
	for {
		pairs, next, err := s.rdb.ZScan(ctx, set, cursor, "", rescoreBatch).Result()
		if err != nil {
			return err
		}
		if err := s.rescoreIDs(ctx, set, jobs, pairs); err != nil {
			return err
		}
		if cursor = next; cursor == 0 {
			return nil
		}
	}
}

func (s *Store) rescoreIDs(ctx context.Context, set, jobs string, pairs []string) error {
	ids := make([]string, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		ids = append(ids, pairs[i])
	}
	reads := make([]*redis.SliceCmd, len(ids))
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, id := range ids {
			reads[i] = p.HMGet(ctx, jobs+id, "priority", "run_at")
		}
		return nil
	})
	if err != nil {
		return err
	}
	_, err = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, id := range ids {
			v := reads[i].Val()
			if len(v) != 2 || v[0] == nil || v[1] == nil {
				continue
			}
			prio, perr := strconv.ParseInt(v[0].(string), 10, 64)
			runAt, rerr := strconv.ParseInt(v[1].(string), 10, 64)
			if perr != nil || rerr != nil {
				continue
			}
			p.ZAddXX(ctx, set, redis.Z{Score: readyScore(prio, runAt), Member: id})
		}
		return nil
	})
	return err
}
