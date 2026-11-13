package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
)

func TestRemoteWorkerFlow(t *testing.T) {
	spec.Rule(t, "S02", "S08", "L04", "O04", "O07")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/media", nil)
	prodKey := key(t, srv, "media", "producer", "reader")
	workKey := key(t, srv, "media", "worker")
	prod := remote.New(srv.URL, prodKey, "media")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	w := remote.NewWorker(remote.New(srv.URL, workKey, "media"), runnel.WorkerConfig{Queues: []string{"thumbs"}, Concurrency: 2, Lease: 5 * time.Second, ShutdownTimeout: time.Second})
	w.HandleFunc("resize", func(ctx context.Context, j *runnel.Job) error {
		var in struct{ W int }
		if err := j.Bind(&in); err != nil {
			return runnel.Fail(err)
		}
		if in.W <= 0 {
			return runnel.Fail(errors.New("width must be positive"))
		}
		return j.SetResult(map[string]int{"w": in.W / 2})
	})
	w.Handle("render", runnel.Async(
		func(ctx context.Context, j *runnel.Job) error { return runnel.Pending("r-1", 20*time.Millisecond) },
		func(ctx context.Context, j *runnel.Job) error { return j.SetResult(j.Handle) }))
	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(wctx)
	}()
	defer func() {
		wcancel()
		<-done
	}()

	ok, err := prod.Enqueue(ctx, "thumbs", "resize", map[string]int{"W": 640})
	if err != nil {
		t.Fatal(err)
	}
	bad, _ := prod.Enqueue(ctx, "thumbs", "resize", map[string]int{"W": 0}, runnel.MaxAttempts(3))
	asyncJob, _ := prod.Enqueue(ctx, "thumbs", "render", nil)
	waitState := func(id string, want runnel.State) *runnel.Job {
		for ctx.Err() == nil {
			j, err := prod.Get(ctx, id)
			if err == nil && j.State == want {
				return j
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatalf("job %s never reached %s", id, want)
		return nil
	}
	if j := waitState(ok.ID, runnel.StateCompleted); string(j.Result) != `{"w":320}` {
		t.Fatalf("result %s", j.Result)
	}
	if j := waitState(bad.ID, runnel.StateDead); j.Attempt != 1 {
		t.Fatalf("permanent failure retried: attempt %d", j.Attempt)
	}
	if j := waitState(asyncJob.ID, runnel.StateCompleted); string(j.Result) != `"r-1"` {
		t.Fatalf("async result %s", j.Result)
	}
	if _, err := prod.Get(ctx, runnel.NewID()); !errors.Is(err, runnel.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	other := remote.New(srv.URL, workKey, "media").Source(0)
	jobs, err := other.Claim(ctx, "thief", []string{"thumbs"}, 1, 5*time.Second)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("claim on empty queue: %v %v", jobs, err)
	}
	code, _ := call(t, srv, workKey, "POST", "/v1/ns/media/jobs/"+ok.ID+"/finish", map[string]any{"worker": "thief", "version": 1, "outcome": map[string]any{"kind": "done"}})
	if code != 409 {
		t.Fatalf("finishing someone else's job: %d", code)
	}
}

type reply struct {
	StatusCode int
	Header     http.Header
}

func send(t *testing.T, srv *httptest.Server, token, method, path, prefer string, body any) (reply, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, srv.URL+path, r)
	req.Header.Set("Authorization", "Bearer "+token)
	if prefer != "" {
		req.Header.Set("Prefer", prefer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return reply{StatusCode: resp.StatusCode, Header: resp.Header}, out
}

func TestWaitModes(t *testing.T) {
	spec.Rule(t, "S05", "S06", "S07", "S02", "W03", "T06")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/door", nil)
	prodKey := key(t, srv, "door", "producer", "reader", "admin")
	workKey := key(t, srv, "door", "worker")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, out := send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "wait=1", map[string]any{"queue": "slow", "kind": "k"})
	if resp.StatusCode != 202 || resp.Header.Get("Retry-After") == "" || resp.Header.Get("Location") != "/v1/ns/door/jobs/"+out["id"].(string) {
		t.Fatalf("no worker, want 202 with a link: %d %v %v", resp.StatusCode, resp.Header, out)
	}
	pending := out["id"].(string)
	resp, out = send(t, srv, prodKey, "GET", "/v1/ns/door/jobs/"+pending, "wait=1", nil)
	if resp.StatusCode != 200 || out["state"] != "waiting" || resp.Header.Get("Preference-Applied") != "wait=1" {
		t.Fatalf("long poll on a waiting job: %d %v", resp.StatusCode, out)
	}

	w := remote.NewWorker(remote.New(srv.URL, workKey, "door"), runnel.WorkerConfig{Queues: []string{"fast"}, Concurrency: 2, Lease: 5 * time.Second, ShutdownTimeout: time.Second})
	w.HandleFunc("echo", func(ctx context.Context, j *runnel.Job) error { return j.SetResult(j.Payload) })
	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(wctx)
	}()
	defer func() {
		wcancel()
		<-done
	}()

	resp, out = send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "respond-async, wait=10", map[string]any{"queue": "fast", "kind": "echo", "payload": map[string]int{"n": 7}})
	if resp.StatusCode != 200 || out["state"] != "completed" || resp.Header.Get("Preference-Applied") != "wait=10" {
		t.Fatalf("wait for a fast job: %d %v", resp.StatusCode, out)
	}
	if res, _ := json.Marshal(out["result"]); string(res) != `{"n":7}` {
		t.Fatalf("result %s", res)
	}
	j, err := remote.New(srv.URL, prodKey, "door").EnqueueAndWait(ctx, "fast", "echo", "hi")
	if err != nil || j.State != runnel.StateCompleted || string(j.Result) != `"hi"` {
		t.Fatalf("remote enqueue and wait: %+v %v", j, err)
	}
	if code, body := call(t, srv, prodKey, "PUT", "/v1/ns/door/queues/fast", map[string]any{"name": "fast", "wait": "5s"}); code >= 300 {
		t.Fatalf("set queue %d %v", code, body)
	}
	resp, out = send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "", map[string]any{"queue": "fast", "kind": "echo", "ttl": "1m"})
	if resp.StatusCode != 200 || out["state"] != "completed" || resp.Header.Get("Preference-Applied") != "wait=5" || out["deadline"] == nil {
		t.Fatalf("queue default wait: %d %v", resp.StatusCode, out)
	}
	in, err := remote.New(srv.URL, prodKey, "door").Enqueue(ctx, "slow", "k", nil, runnel.TTL(2*time.Hour), runnel.In(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := remote.New(srv.URL, prodKey, "door").Get(ctx, in.ID); err != nil || got.Deadline.Sub(got.RunAt).Round(time.Minute) != time.Hour {
		t.Fatalf("remote ttl and delay: %+v %v", got, err)
	}

	call(t, srv, prodKey, "PUT", "/v1/ns/door/queues/capped", map[string]any{"name": "capped", "max_queued": 1})
	send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "", map[string]any{"queue": "capped", "kind": "k"})
	resp, out = send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "", map[string]any{"queue": "capped", "kind": "k"})
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "1" || out["title"] != "queue full" {
		t.Fatalf("full queue: %d %v %v", resp.StatusCode, resp.Header, out)
	}
	if _, err := remote.New(srv.URL, prodKey, "door").Enqueue(ctx, "capped", "k", nil); !errors.Is(err, runnel.ErrQueueFull) {
		t.Fatalf("remote full queue: %v", err)
	}
}

func TestRemoteSettings(t *testing.T) {
	spec.Rule(t, "S12")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/gate", nil)
	ctx := context.Background()
	owner := remote.New(srv.URL, key(t, srv, "gate", "admin"), "gate")
	if err := owner.SetQueue(ctx, runnel.QueueConfig{Name: "image", MaxQueued: 5}); err != nil {
		t.Fatal(err)
	}
	if err := owner.SetPool(ctx, runnel.PoolConfig{Name: "m.flux", Slots: 2, GroupSlots: 1}); err != nil {
		t.Fatal(err)
	}
	if code, body := call(t, srv, admin, "GET", "/v1/ns/gate/pools/m.flux", nil); code != 200 || body["slots"] != float64(2) || body["group_slots"] != float64(1) {
		t.Fatalf("pool read back: %d %v", code, body)
	}
	producer := remote.New(srv.URL, key(t, srv, "gate", "producer"), "gate")
	var apiErr *remote.APIError
	if err := producer.SetPool(ctx, runnel.PoolConfig{Name: "m.flux", Slots: 9}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden {
		t.Fatalf("producer set a pool: %v", err)
	}
}
