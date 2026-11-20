package redisstore

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/kinoforge/runnel"
)

func (s *Store) PutConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, s.p(ns)+"cfg:"+string(kind), name, value)
	if kind == runnel.ConfigPool {
		if ms := pauseMillis(value); ms != "" {
			pipe.HSet(ctx, s.p(ns)+"cfg:pool_pause", name, ms)
		} else {
			pipe.HDel(ctx, s.p(ns)+"cfg:pool_pause", name)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) SwapConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, old, next []byte) (bool, error) {
	had := "0"
	if old != nil {
		had = "1"
	}
	pm := ""
	if kind == runnel.ConfigPool {
		pm = pauseMillis(next)
	}
	n, err := s.swapConfig.Run(ctx, s.rdb, []string{s.p(ns)}, string(kind), name, had, old, next, pm).Int()
	return n == 1, err
}

func pauseMillis(value []byte) string {
	var p runnel.PoolConfig
	if json.Unmarshal(value, &p) != nil || p.PausedUntil.IsZero() {
		return ""
	}
	return strconv.FormatInt(p.PausedUntil.UnixMilli(), 10)
}

func (s *Store) Configs(ctx context.Context, ns string, kind runnel.ConfigKind) (map[string][]byte, error) {
	m, err := s.rdb.HGetAll(ctx, s.p(ns)+"cfg:"+string(kind)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		out[k] = []byte(v)
	}
	return out, nil
}

func (s *Store) DeleteConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string) error {
	n, err := s.rdb.HDel(ctx, s.p(ns)+"cfg:"+string(kind), name).Result()
	if err == nil && n == 0 {
		return runnel.ErrNotFound
	}
	if err == nil && kind == runnel.ConfigPool {
		_ = s.rdb.HDel(ctx, s.p(ns)+"cfg:pool_pause", name).Err()
	}
	return err
}
