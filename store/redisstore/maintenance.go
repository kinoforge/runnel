package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/kinoforge/runnel"
	"github.com/redis/go-redis/v9"
)

func (s *Store) Prune(ctx context.Context, ns string, req runnel.PruneRequest) (int, error) {
	if len(req.States) == 0 || req.Limit <= 0 {
		return 0, nil
	}
	for _, st := range req.States {
		if !st.Final() {
			return 0, fmt.Errorf("%w: only finished jobs can be pruned", runnel.ErrInvalid)
		}
	}
	states, err := jsonArg(req.States)
	if err != nil {
		return 0, err
	}
	archive := "0"
	if req.Archive {
		archive = "1"
	}
	n, err := s.prune.Run(ctx, s.rdb, []string{s.p(ns)}, ms(req.Before), req.Limit, states, req.Queue, archive, max(req.Limit*5, 500)).Int()
	return n, err
}

func (s *Store) PruneEvents(ctx context.Context, ns string, req runnel.EventPruneRequest) (int, error) {
	if req.Limit <= 0 || req.Before.IsZero() {
		return 0, nil
	}
	return s.pruneEvents.Run(ctx, s.rdb, []string{s.p(ns)}, ms(req.Before), strconv.FormatInt(req.UpTo, 10), req.Limit).Int()
}

func (s *Store) Cursor(ctx context.Context, ns, name string) (int64, error) {
	v, err := s.rdb.HGet(ctx, s.p(ns)+"cursors", name).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}

func (s *Store) SwapCursor(ctx context.Context, ns, name string, old, next int64) (bool, error) {
	if old == next {
		return true, nil
	}
	n, err := s.swap.Run(ctx, s.rdb, []string{s.p(ns)}, name, strconv.FormatInt(old, 10), strconv.FormatInt(next, 10)).Int()
	return n == 1, err
}
