package redisstore

import (
	"context"
	"encoding/json"

	"github.com/kinoforge/runnel"
)

func (s *Store) PutConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	if kind == runnel.ConfigPool {
		v, err := withPauseMillis(value)
		if err != nil {
			return err
		}
		value = v
	}
	return s.rdb.HSet(ctx, s.p(ns)+"cfg:"+string(kind), name, value).Err()
}

// withPauseMillis adds paused_until_ms to a pool, since the claim script cannot
// read RFC 3339 times. Readers ignore the extra field.
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
	return s.rdb.HDel(ctx, s.p(ns)+"cfg:"+string(kind), name).Err()
}
