package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kinoforge/runnel"
)

var fields = []string{"id", "queue", "kind", "state", "priority", "tenant", "pool", "attempt", "max_attempts", "run_at",
	"waiting_since", "start_by", "start_within", "worker", "lease_until", "version", "pending", "unique_key", "created_at",
	"updated_at", "started_at", "finished_at", "data", "deps"}

var mutable = []string{"queue", "kind", "state", "priority", "tenant", "pool", "attempt", "max_attempts", "run_at",
	"waiting_since", "start_by", "start_within", "worker", "lease_until", "updated_at", "started_at", "finished_at", "data"}

// Store keeps jobs in Redis, Valkey or Dragonfly. Every operation is one Lua
// script, so it is atomic; a namespace's keys share a hash tag, so a namespace
// lives on one cluster slot.
type Store struct {
	rdb    redis.UniversalClient
	prefix string

	insert, claim, apply, extend, prune, pruneEvents, swap *redis.Script

	clockOnce sync.Once
	clock     runnel.Clock
}

// Open connects to the server a redis:// or rediss:// URL names.
func Open(rawURL string) (*Store, error) {
	opt, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	return New(redis.NewClient(opt), "rn"), nil
}

// New returns a store over rdb with keys under prefix (default "rn").
func New(rdb redis.UniversalClient, prefix string) *Store {
	if prefix == "" {
		prefix = "rn"
	}
	return &Store{rdb: rdb, prefix: prefix,
		insert: redis.NewScript(insertScript), claim: redis.NewScript(claimScript), apply: redis.NewScript(applyScript),
		extend: redis.NewScript(extendScript), prune: redis.NewScript(pruneScript), pruneEvents: redis.NewScript(pruneEventsScript), swap: redis.NewScript(swapScript)}
}

// Client returns the Redis client.
func (s *Store) Client() redis.UniversalClient { return s.rdb }

func (s *Store) p(ns string) string {
	if ns == "" {
		ns = "_"
	}
	return s.prefix + ":{" + ns + "}:"
}

func (s *Store) Migrate(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// Time returns the server's clock.
// Clock returns the store's shared clock, which follows Time.
func (s *Store) Clock() runnel.Clock {
	s.clockOnce.Do(func() { s.clock = runnel.NewStoreClock(s) })
	return s.clock
}

func (s *Store) Time(ctx context.Context) (time.Time, error) {
	t, err := s.rdb.Time(ctx).Result()
	return t.UTC(), err
}

func (s *Store) Close() error { return s.rdb.Close() }

func ms(t time.Time) string { return strconv.FormatInt(runnel.Millis(t), 10) }

func encode(j *runnel.Job) (map[string]string, error) {
	data, err := j.EncodeData()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"id": j.ID, "queue": j.Queue, "kind": j.Kind, "state": string(j.State), "priority": strconv.Itoa(j.Priority),
		"tenant": j.Tenant, "pool": j.Pool, "attempt": strconv.Itoa(j.Attempt), "max_attempts": strconv.Itoa(j.MaxAttempts),
		"run_at": ms(j.RunAt), "waiting_since": ms(j.WaitingSince), "start_by": ms(j.StartBy),
		"start_within": strconv.FormatInt(j.StartWindow().Milliseconds(), 10), "worker": j.Worker, "lease_until": ms(j.LeaseUntil),
		"version": strconv.FormatInt(j.Version, 10), "pending": strconv.Itoa(j.Pending), "unique_key": j.UniqueKey,
		"created_at": ms(j.CreatedAt), "updated_at": ms(j.UpdatedAt), "started_at": ms(j.StartedAt), "finished_at": ms(j.FinishedAt),
		"data": string(data), "deps": strings.Join(j.DependsOn, ","),
	}, nil
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	case int64:
		return strconv.FormatInt(x, 10)
	}
	return fmt.Sprint(v)
}

func decode(ns string, vals []any) (*runnel.Job, error) {
	if len(vals) < len(fields) || vals[0] == nil {
		return nil, nil
	}
	m := make(map[string]string, len(fields))
	for i, f := range fields {
		m[f] = str(vals[i])
	}
	i := func(f string) int { n, _ := strconv.Atoi(m[f]); return n }
	t := func(f string) time.Time { n, _ := strconv.ParseInt(m[f], 10, 64); return runnel.FromMillis(n) }
	j := &runnel.Job{ID: m["id"], Namespace: ns, Queue: m["queue"], Kind: m["kind"], State: runnel.State(m["state"]),
		Priority: i("priority"), Tenant: m["tenant"], Pool: m["pool"], Attempt: i("attempt"), MaxAttempts: i("max_attempts"),
		RunAt: t("run_at"), WaitingSince: t("waiting_since"), StartBy: t("start_by"), Worker: m["worker"],
		LeaseUntil: t("lease_until"), Pending: i("pending"), UniqueKey: m["unique_key"], CreatedAt: t("created_at"),
		UpdatedAt: t("updated_at"), StartedAt: t("started_at"), FinishedAt: t("finished_at")}
	j.Version, _ = strconv.ParseInt(m["version"], 10, 64)
	if err := j.DecodeData([]byte(m["data"])); err != nil {
		return nil, err
	}
	j.DependsOn = nil
	if m["deps"] != "" {
		j.DependsOn = strings.Split(m["deps"], ",")
	}
	return j, nil
}

func decodeList(ns string, raw any) ([]*runnel.Job, error) {
	rows, _ := raw.([]any)
	out := make([]*runnel.Job, 0, len(rows))
	for _, r := range rows {
		vals, _ := r.([]any)
		j, err := decode(ns, vals)
		if err != nil {
			return nil, err
		}
		if j != nil {
			out = append(out, j)
		}
	}
	return out, nil
}

func jsonArg(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

func (s *Store) Insert(ctx context.Context, ns string, jobs []*runnel.Job, now time.Time) ([]runnel.Inserted, error) {
	payload := make([]map[string]any, len(jobs))
	for i, j := range jobs {
		m, err := encode(j)
		if err != nil {
			return nil, err
		}
		obj := make(map[string]any, len(m)+2)
		for k, v := range m {
			obj[k] = v
		}
		deps := j.DependsOn
		if deps == nil {
			deps = []string{}
		}
		obj["dep_ids"] = deps
		until := int64(0)
		if j.UniqueFor > 0 {
			until = runnel.Millis(now) + j.UniqueFor.D().Milliseconds()
		}
		obj["unique_until"] = strconv.FormatInt(until, 10)
		payload[i] = obj
	}
	arg, err := jsonArg(payload)
	if err != nil {
		return nil, err
	}
	raw, err := s.insert.Run(ctx, s.rdb, []string{s.p(ns)}, ms(now), arg).Result()
	if err != nil {
		return nil, err
	}
	rows, _ := raw.([]any)
	if len(rows) != len(jobs) {
		return nil, fmt.Errorf("runnel redis: insert returned %d results for %d jobs", len(rows), len(jobs))
	}
	out := make([]runnel.Inserted, len(jobs))
	for i, r := range rows {
		cols, _ := r.([]any)
		if len(cols) < 3 {
			return nil, errors.New("runnel redis: bad insert result")
		}
		res := runnel.Inserted{ID: str(cols[1])}
		switch str(cols[0]) {
		case "dup":
			res.Duplicate = true
		case "dep":
			res.Err = runnel.ErrDependency
		case "full":
			res.Err = runnel.ErrTenantFull
		case "qfull":
			res.Err = runnel.ErrQueueFull
		default:
			p, _ := strconv.Atoi(str(cols[2]))
			jobs[i].Pending = p
			jobs[i].State = runnel.StateWaiting
			if p > 0 {
				jobs[i].State, jobs[i].StartBy = runnel.StateBlocked, time.Time{}
			}
		}
		out[i] = res
	}
	return out, nil
}

func (s *Store) Claim(ctx context.Context, ns string, req runnel.ClaimRequest) ([]*runnel.Job, error) {
	if req.Limit <= 0 || len(req.Queues) == 0 {
		return nil, nil
	}
	qs, err := jsonArg(req.Queues)
	if err != nil {
		return nil, err
	}
	only := req.IDs
	if only == nil {
		only = []string{}
	}
	ids, err := jsonArg(only)
	if err != nil {
		return nil, err
	}
	now := runnel.Millis(req.Now)
	scan := min(max(req.Limit*4, 32), 500)
	raw, err := s.claim.Run(ctx, s.rdb, []string{s.p(ns)}, now, now+req.Lease.Milliseconds(), req.Worker, req.Limit, scan, qs, ids).Result()
	if err != nil {
		return nil, err
	}
	return decodeList(ns, raw)
}

func (s *Store) Extend(ctx context.Context, ns, worker string, ids []string, until time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	arg, err := jsonArg(ids)
	if err != nil {
		return nil, err
	}
	raw, err := s.extend.Run(ctx, s.rdb, []string{s.p(ns)}, ms(until), worker, arg).Result()
	if err != nil {
		return nil, err
	}
	rows, _ := raw.([]any)
	lost := make([]string, 0, len(rows))
	for _, r := range rows {
		lost = append(lost, str(r))
	}
	return lost, nil
}

func (s *Store) Apply(ctx context.Context, ns string, changes []runnel.Change, now time.Time) ([]bool, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	payload := make([]map[string]any, len(changes))
	for i, c := range changes {
		m, err := encode(c.Job)
		if err != nil {
			return nil, err
		}
		f := make(map[string]string, len(mutable))
		for _, k := range mutable {
			f[k] = m[k]
		}
		payload[i] = map[string]any{"id": c.Job.ID, "expect": m["version"], "lease_before": ms(c.LeaseBefore), "f": f}
	}
	arg, err := jsonArg(payload)
	if err != nil {
		return nil, err
	}
	raw, err := s.apply.Run(ctx, s.rdb, []string{s.p(ns)}, ms(now), arg).Result()
	if err != nil {
		return nil, err
	}
	rows, _ := raw.([]any)
	out := make([]bool, len(changes))
	for i := range out {
		if i < len(rows) {
			n, _ := rows[i].(int64)
			out[i] = n == 1
		}
		if out[i] {
			changes[i].Job.Version++
		}
	}
	return out, nil
}

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
	chunk := int64(max(q.Limit*2, 50))
	for range 50 {
		ids, err := s.rdb.ZRevRangeByLex(ctx, key, &redis.ZRangeBy{Max: maxLex, Min: "-", Count: chunk}).Result()
		if err != nil {
			return nil, "", err
		}
		if len(ids) == 0 {
			break
		}
		jobs, err := s.Get(ctx, ns, ids)
		if err != nil {
			return nil, "", err
		}
		for _, j := range jobs {
			if (q.Queue == "" || j.Queue == q.Queue) && (q.Kind == "" || j.Kind == q.Kind) && (q.Tenant == "" || j.Tenant == q.Tenant) &&
				(q.State == "" || j.State == q.State) {
				out = append(out, j)
				if len(out) == q.Limit {
					return out, j.ID, nil
				}
			}
		}
		maxLex = "(" + ids[len(ids)-1]
		if int64(len(ids)) < chunk {
			break
		}
	}
	return out, "", nil
}

func (s *Store) Due(ctx context.Context, ns string, kind runnel.DueKind, now time.Time, limit int) ([]*runnel.Job, error) {
	var key, maxScore string
	var want runnel.State
	switch kind {
	case runnel.DueLease:
		key, maxScore, want = "leases", "("+ms(now), runnel.StateRunning
	case runnel.DueStart:
		key, maxScore, want = "startby", ms(now), runnel.StateWaiting
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
		if j.State == want {
			out = append(out, j)
		}
	}
	return out, nil
}

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
	n, err := s.prune.Run(ctx, s.rdb, []string{s.p(ns)}, ms(req.Before), req.Limit, states, req.Queue, archive).Int()
	return n, err
}

func (s *Store) PruneEvents(ctx context.Context, ns string, req runnel.EventPruneRequest) (int, error) {
	if req.Limit <= 0 || req.Before.IsZero() {
		return 0, nil
	}
	return s.pruneEvents.Run(ctx, s.rdb, []string{s.p(ns)}, ms(req.Before), strconv.FormatInt(req.UpTo, 10), req.Limit).Int()
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

// Subscribe signals when jobs in ns may be ready to claim.
func (s *Store) Subscribe(ctx context.Context, ns string) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	return ch, s.listen(ctx, s.p(ns)+"wake", func(string) {
		select {
		case ch <- struct{}{}:
		default:
		}
	})
}

// Ended sends the id of each job in ns that reaches a final state.
func (s *Store) Ended(ctx context.Context, ns string) (<-chan string, error) {
	ch := make(chan string, 256)
	return ch, s.listen(ctx, s.p(ns)+"end", func(id string) {
		select {
		case ch <- id:
		default:
		}
	})
}

func (s *Store) listen(ctx context.Context, channel string, fn func(string)) error {
	sub := s.rdb.Subscribe(ctx, channel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}
	go func() {
		defer sub.Close()
		msgs := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-msgs:
				if !ok {
					return
				}
				fn(m.Payload)
			}
		}
	}()
	return nil
}

var (
	_ runnel.Store      = (*Store)(nil)
	_ runnel.Waker      = (*Store)(nil)
	_ runnel.EndWatcher = (*Store)(nil)
	_ runnel.TimeSource = (*Store)(nil)
)
