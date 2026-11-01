package redisstore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kinoforge/runnel"
)

var fields = []string{"id", "queue", "kind", "state", "priority", "tenant", "pool", "attempt", "max_attempts", "run_at",
	"waiting_since", "start_by", "start_within", "worker", "lease_until", "version", "pending", "unique_key", "created_at",
	"updated_at", "started_at", "finished_at", "data", "deps"}

var mutable = []string{"queue", "kind", "state", "priority", "tenant", "pool", "attempt", "max_attempts", "run_at",
	"waiting_since", "start_by", "start_within", "worker", "lease_until", "updated_at", "started_at", "finished_at", "data"}

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
