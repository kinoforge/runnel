package redisstore

import (
	"context"

	"github.com/kinoforge/runnel"
)

func (s *Store) PutConfig(ctx context.Context, ns string, kind runnel.ConfigKind, name string, value []byte) error {
	return s.rdb.HSet(ctx, s.p(ns)+"cfg:"+string(kind), name, value).Err()
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
