package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/redis/go-redis/v9"
)

const (
	listChunkFactor = 2
	minListChunk    = 50
	maxListRounds   = 50
)

func (s *Store) Get(ctx context.Context, ns string, ids []string) ([]*runnel.Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, s.p(ns)+"j:"+id, fields...)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]*runnel.Job, 0, len(ids))
	seen := map[string]bool{}
	for i, c := range cmds {
		if seen[ids[i]] {
			continue
		}
		seen[ids[i]] = true
		j, err := decode(ns, c.Val())
		if err != nil {
			return nil, err
		}
		if j != nil {
			out = append(out, j)
		}
	}
	return out, nil
}

func (s *Store) List(ctx context.Context, ns string, q runnel.Query) ([]*runnel.Job, string, error) {
	key := s.p(ns) + "all"
	if q.State != "" {
		key = s.p(ns) + "st:" + string(q.State)
	}
	maxLex := "+"
	if q.Cursor != "" {
		maxLex = "(" + q.Cursor
	}
	var out []*runnel.Job
	chunk := int64(max(q.Limit*listChunkFactor, minListChunk))
	lastID := ""
	more := false
	for range maxListRounds {
		ids, err := s.rdb.ZRevRangeByLex(ctx, key, &redis.ZRangeBy{Max: maxLex, Min: "-", Count: chunk}).Result()
		if err != nil {
			return nil, "", err
		}
		if len(ids) == 0 {
			more = false
			break
		}
		jobs, err := s.Get(ctx, ns, ids)
		if err != nil {
			return nil, "", err
		}
		for _, j := range jobs {
			if (q.Queue == "" || j.Queue == q.Queue) && (q.Kind == "" || j.Kind == q.Kind) && (q.Group == "" || j.Group == q.Group) &&
				(q.State == "" || j.State == q.State) {
				out = append(out, j)
				if len(out) == q.Limit {
					return out, j.ID, nil
				}
			}
		}
		lastID = ids[len(ids)-1]
		maxLex = "(" + lastID
		if int64(len(ids)) < chunk {
			more = false
			break
		}
		more = true
	}
	if more {
		return out, lastID, nil
	}
	return out, "", nil
}

func is(want runnel.State) func(runnel.State) bool {
	return func(st runnel.State) bool { return st == want }
}

func (s *Store) Due(ctx context.Context, ns string, kind runnel.DueKind, now time.Time, limit int) ([]*runnel.Job, error) {
	var key, maxScore string
	var want func(runnel.State) bool
	switch kind {
	case runnel.DueLease:
		key, maxScore, want = "leases", "("+ms(now), is(runnel.StateRunning)
	case runnel.DueStart:
		key, maxScore, want = "startby", ms(now), is(runnel.StateWaiting)
	case runnel.DueDeadline:
		key, maxScore, want = "deadlines", ms(now), func(st runnel.State) bool { return !st.Final() }
	default:
		return nil, fmt.Errorf("runnel: unknown due kind %d", kind)
	}
	ids, err := s.rdb.ZRangeByScore(ctx, s.p(ns)+key, &redis.ZRangeBy{Min: "-inf", Max: maxScore, Count: int64(limit)}).Result()
	if err != nil {
		return nil, err
	}
	jobs, err := s.Get(ctx, ns, ids)
	if err != nil {
		return nil, err
	}
	out := jobs[:0]
	for _, j := range jobs {
		if want(j.State) {
			out = append(out, j)
		}
	}
	return out, nil
}

func (s *Store) Events(ctx context.Context, ns string, after int64, limit int) ([]runnel.Event, error) {
	raw, err := s.rdb.ZRangeByScore(ctx, s.p(ns)+"events", &redis.ZRangeBy{Min: "(" + strconv.FormatInt(after, 10), Max: "+inf", Count: int64(limit)}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]runnel.Event, 0, len(raw))
	for _, r := range raw {
		var e struct {
			Seq   int64  `json:"seq"`
			ID    string `json:"id"`
			Queue string `json:"queue"`
			Kind  string `json:"kind"`
			State string `json:"state"`
			At    int64  `json:"at"`
		}
		if err := json.Unmarshal([]byte(r), &e); err != nil {
			return nil, err
		}
		out = append(out, runnel.Event{Seq: e.Seq, Namespace: ns, JobID: e.ID, Queue: e.Queue, Kind: e.Kind, State: runnel.State(e.State), At: runnel.FromMillis(e.At)})
	}
	return out, nil
}

func (s *Store) Stats(ctx context.Context, ns string) ([]runnel.QueueStats, error) {
	m, err := s.rdb.HGetAll(ctx, s.p(ns)+"cnt").Result()
	if err != nil {
		return nil, err
	}
	byQueue := map[string]map[runnel.State]int64{}
	var order []string
	for k, v := range m {
		q, st, ok := strings.Cut(k, "|")
		n, _ := strconv.ParseInt(v, 10, 64)
		if !ok || n <= 0 {
			continue
		}
		if byQueue[q] == nil {
			byQueue[q] = map[runnel.State]int64{}
			order = append(order, q)
		}
		byQueue[q][runnel.State(st)] = n
	}
	slices.Sort(order)
	out := make([]runnel.QueueStats, 0, len(order))
	for _, q := range order {
		out = append(out, runnel.QueueStats{Queue: q, Counts: byQueue[q]})
	}
	return out, nil
}
