package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kinoforge/runnel"
)

const (
	scanFactor = 4
	minScan    = 32
	maxScan    = 500
)

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
			res.Err = runnel.ErrGroupFull
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
	scan := min(max(req.Limit*scanFactor, minScan), maxScan)
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
