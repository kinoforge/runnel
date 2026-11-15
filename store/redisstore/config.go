package redisstore

import (
	"context"
	"encoding/json"

	"github.com/kinoforge/runnel"
)

func (s *Store) PutConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	value, err := stored(kind, value)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, s.p(ns)+"cfg:"+string(kind), name, value).Err()
}

func (s *Store) SwapConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, old, next []byte) (bool, error) {
	next, err := stored(kind, next)
	if err != nil {
		return false, err
	}
	had := "0"
	if old != nil {
		had = "1"
	}
	n, err := s.swapConfig.Run(ctx, s.rdb, []string{s.p(ns)}, string(kind), name, had, old, next).Int()
	return n == 1, err
}

func stored(kind runnel.ConfigKind, value []byte) ([]byte, error) {
	if kind != runnel.ConfigPool {
		return value, nil
	}
	return withPauseMillis(value)
}

// The claim script cannot parse RFC 3339 times.
func withPauseMillis(value []byte) ([]byte, error) {
	var p runnel.PoolConfig
	if json.Unmarshal(value, &p) != nil || p.PausedUntil.IsZero() {
		return value, nil
	}
	var m map[string]any
	if err := json.Unmarshal(value, &m); err != nil {
		return nil, err
	}
	m["paused_until_ms"] = p.PausedUntil.UnixMilli()
	return json.Marshal(m)
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
	return err
}
