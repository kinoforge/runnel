package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/sqlite"
)

const admin = "test-admin-token-0123456789"

func setup(t *testing.T) (*httptest.Server, runnel.Store) {
	s, err := sqlite.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(s, server.Config{AdminToken: admin}))
	t.Cleanup(srv.Close)
	return srv, s
}

func call(t *testing.T, srv *httptest.Server, token, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		if s, ok := body.(string); ok {
			r = strings.NewReader(s)
		} else {
			b, _ := json.Marshal(body)
			r = bytes.NewReader(b)
		}
	}
	req, _ := http.NewRequest(method, srv.URL+path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func key(t *testing.T, srv *httptest.Server, ns string, roles ...string) string {
	t.Helper()
	code, out := call(t, srv, admin, "POST", "/v1/namespaces/"+ns+"/keys", map[string]any{"name": "k", "roles": roles})
	if code != 201 {
		t.Fatalf("create key: %d %v", code, out)
	}
	return out["key"].(string)
}

func TestAuthAndRoles(t *testing.T) {
	spec.Rule(t, "A01", "A02", "A03", "A05", "A06")
	srv, _ := setup(t)
	if code, _ := call(t, srv, "", "GET", "/v1/namespaces", nil); code != 401 {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := call(t, srv, "wrong", "GET", "/v1/namespaces", nil); code != 401 {
		t.Fatalf("bad token: %d", code)
	}
	if code, _ := call(t, srv, admin, "PUT", "/v1/namespaces/billing", map[string]any{"webhook_secret": "s3cret"}); code != 201 {
		t.Fatalf("create ns: %d", code)
	}
	if code, _ := call(t, srv, admin, "PUT", "/v1/namespaces/Bad_NS!", nil); code != 400 {
		t.Fatalf("bad ns name: %d", code)
	}
	call(t, srv, admin, "PUT", "/v1/namespaces/search", nil)
	reader := key(t, srv, "billing", "reader")
	producer := key(t, srv, "billing", "producer")
	if code, _ := call(t, srv, reader, "POST", "/v1/ns/billing/jobs", map[string]any{"queue": "q", "kind": "k"}); code != 403 {
		t.Fatalf("reader enqueued: %d", code)
	}
	if code, _ := call(t, srv, producer, "POST", "/v1/ns/search/jobs", map[string]any{"queue": "q", "kind": "k"}); code != 403 {
		t.Fatalf("cross-namespace enqueue: %d", code)
	}
	if code, _ := call(t, srv, producer, "GET", "/v1/namespaces", nil); code != 403 {
		t.Fatalf("key reached admin route: %d", code)
	}
	_, view := call(t, srv, admin, "GET", "/v1/namespaces/billing", nil)
	raw, _ := json.Marshal(view)
	if strings.Contains(string(raw), "s3cret") || strings.Contains(string(raw), `"hash"`) {
		t.Fatalf("namespace view leaks secrets: %s", raw)
	}
	code, out := call(t, srv, admin, "GET", "/v1/namespaces/billing", nil)
	keys := out["keys"].([]any)
	id := keys[0].(map[string]any)["id"].(string)
	if code, _ = call(t, srv, admin, "DELETE", "/v1/namespaces/billing/keys/"+id, nil); code != 204 {
		t.Fatalf("delete key: %d", code)
	}
	if code, _ := call(t, srv, reader, "GET", "/v1/ns/billing/stats", nil); code != 401 {
		t.Fatalf("deleted key still works: %d", code)
	}
}

func TestValidation(t *testing.T) {
	spec.Rule(t, "S01", "S02", "S03", "S04", "S09")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/app", nil)
	p := key(t, srv, "app", "producer", "admin")
	cases := []struct {
		name string
		body any
		want int
	}{
		{"unknown field", map[string]any{"queue": "q", "kind": "k", "evil": 1}, 400},
		{"bad queue", map[string]any{"queue": "../etc", "kind": "k"}, 400},
		{"bad json", `{"queue":`, 400},
		{"http webhook", map[string]any{"queue": "q", "kind": "k", "webhook": "http://example.com"}, 400},
		{"reserved queue", map[string]any{"queue": runnel.WebhookQueue, "kind": "k"}, 400},
		{"priority range", map[string]any{"queue": "q", "kind": "k", "priority": 5000}, 400},
		{"trailing data", `{"queue":"q","kind":"k"} {}`, 400},
		{"ok", map[string]any{"queue": "q", "kind": "k", "payload": map[string]int{"a": 1}}, 201},
	}
	for _, c := range cases {
		if code, out := call(t, srv, p, "POST", "/v1/ns/app/jobs", c.body); code != c.want {
			t.Errorf("%s: %d %v", c.name, code, out)
		}
	}
	big := map[string]any{"queue": "q", "kind": "k", "payload": strings.Repeat("x", 5<<20)}
	if code, _ := call(t, srv, p, "POST", "/v1/ns/app/jobs", big); code != 413 {
		t.Errorf("huge body: %d", code)
	}
	if code, _ := call(t, srv, p, "PUT", "/v1/ns/app/queues/other", map[string]any{"name": "q"}); code != 400 {
		t.Errorf("path/body mismatch: %d", code)
	}
	if code, _ := call(t, srv, p, "GET", "/v1/ns/app/jobs/not-an-id", nil); code != 404 {
		t.Errorf("bad id: %d", code)
	}
	once := map[string]any{"queue": "q", "kind": "k", "unique_key": "once"}
	first, _ := send(t, srv, p, "POST", "/v1/ns/app/jobs", "", once)
	again, out := send(t, srv, p, "POST", "/v1/ns/app/jobs", "", once)
	if first.StatusCode != 201 || again.StatusCode != 200 || out["duplicate"] != true || again.Header.Get("Location") == "" {
		t.Errorf("duplicate: %d then %d %v", first.StatusCode, again.StatusCode, out)
	}
	code, out := call(t, srv, p, "POST", "/v1/ns/app/jobs", map[string]any{"jobs": []any{map[string]any{"queue": "q", "kind": "k"}, map[string]any{"queue": "q", "kind": "k"}}})
	if results, _ := out["results"].([]any); code != 200 || len(results) != 2 {
		t.Errorf("batch: %d %v", code, out)
	}
}

func TestNoAdminToken(t *testing.T) {
	spec.Rule(t, "A02")
	s, err := sqlite.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(s, server.Config{}))
	defer srv.Close()
	for _, token := range []string{"", "anything"} {
		if code, _ := call(t, srv, token, "GET", "/v1/namespaces", nil); code != 401 {
			t.Fatalf("admin route without an admin token, bearer %q: %d", token, code)
		}
	}
}

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

func TestMetricsAndHealth(t *testing.T) {
	spec.Rule(t, "S11", "A01")
	srv, st := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/m", nil)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz %v %v", resp, err)
	}
	resp, err = http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "runnel_http_requests_total") {
		t.Fatalf("metrics missing")
	}
	resp, err = http.Get(srv.URL + "/openapi.yaml")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("openapi %v %v", resp, err)
	}
	_ = st.Close()
	resp, err = http.Get(srv.URL + "/healthz")
	if err != nil || resp.StatusCode != 503 {
		t.Fatalf("healthz with the store gone %v %v", resp, err)
	}
}

func TestConcurrentKeysAndNamespaceDelete(t *testing.T) {
	spec.Rule(t, "A04", "A05")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/race", nil)
	const n = 20
	tokens := make(chan string, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tokens <- key(t, srv, "race", "reader")
		}()
	}
	wg.Wait()
	close(tokens)
	_, out := call(t, srv, admin, "GET", "/v1/namespaces/race", nil)
	if got := len(out["keys"].([]any)); got != n {
		t.Fatalf("created %d keys at once, %d kept", n, got)
	}
	var last string
	for tok := range tokens {
		if code, _ := call(t, srv, tok, "GET", "/v1/ns/race/stats", nil); code != 200 {
			t.Fatalf("new key rejected: %d", code)
		}
		last = tok
	}
	for range 100 - n {
		key(t, srv, "race", "reader")
	}
	if code, _ := call(t, srv, admin, "POST", "/v1/namespaces/race/keys", map[string]any{"roles": []string{"reader"}}); code != 400 {
		t.Fatalf("key 101: %d", code)
	}
	if code, _ := call(t, srv, admin, "DELETE", "/v1/namespaces/race", nil); code != 204 {
		t.Fatalf("delete namespace: %d", code)
	}
	if code, _ := call(t, srv, last, "GET", "/v1/ns/race/stats", nil); code != 401 {
		t.Fatalf("key of a deleted namespace still works: %d", code)
	}
	call(t, srv, admin, "PUT", "/v1/namespaces/race", nil)
	if code, _ := call(t, srv, last, "GET", "/v1/ns/race/stats", nil); code != 401 {
		t.Fatalf("old key came back with a recreated namespace: %d", code)
	}
	if code, _ := call(t, srv, admin, "DELETE", "/v1/namespaces/other/keys/0123456789ab", nil); code != 404 {
		t.Fatalf("deleting a missing key: %d", code)
	}
}

func send(t *testing.T, srv *httptest.Server, token, method, path, prefer string, body any) (*http.Response, map[string]any) {
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
	return resp, out
}

func TestWaitModes(t *testing.T) {
	spec.Rule(t, "S05", "S06", "S07", "S02", "W03")
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
	w.HandleFunc("echo", func(ctx context.Context, j *runnel.Job) error { return j.SetResult(json.RawMessage(j.Payload)) })
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

	call(t, srv, prodKey, "PUT", "/v1/ns/door/queues/capped", map[string]any{"name": "capped", "max_waiting": 1})
	send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "", map[string]any{"queue": "capped", "kind": "k"})
	resp, out = send(t, srv, prodKey, "POST", "/v1/ns/door/jobs", "", map[string]any{"queue": "capped", "kind": "k"})
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "1" || out["title"] != "queue full" {
		t.Fatalf("full queue: %d %v %v", resp.StatusCode, resp.Header, out)
	}
	if _, err := remote.New(srv.URL, prodKey, "door").Enqueue(ctx, "capped", "k", nil); !errors.Is(err, runnel.ErrQueueFull) {
		t.Fatalf("remote full queue: %v", err)
	}
}
