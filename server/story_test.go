package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
)

func askAs(t *testing.T, srv *httptest.Server, token, method, path, body string, header ...string) answer {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return answer{code: resp.StatusCode, tag: resp.Header.Get("ETag"), body: out}
}

func background(t *testing.T, run func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := run(ctx); err != nil {
			t.Errorf("background: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type hooks struct {
	reader runnel.CallbackReader
	mu     sync.Mutex
	got    []runnel.CallbackBody
	bad    int
}

func (h *hooks) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cb, err := h.reader.Read(r)
	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		h.bad++
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	h.got = append(h.got, cb)
}

func (h *hooks) seen() ([]runnel.CallbackBody, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]runnel.CallbackBody(nil), h.got...), h.bad
}

func TestStoryServiceOverHTTP(t *testing.T) {
	spec.Rule(t, "U20", "A03", "S03", "S05", "H01", "H02", "J03")
	srv, store := setup(t)
	const secret = "service-callback-secret"
	if code, out := call(t, srv, admin, "PUT", "/v1/namespaces/shop", map[string]any{"callback_secret": secret}); code >= 300 {
		t.Fatalf("make namespace: %d %v", code, out)
	}
	producer := key(t, srv, "shop", "producer", "reader")
	worker := key(t, srv, "shop", "worker")
	inbox := &hooks{reader: runnel.CallbackReader{Secrets: []string{secret}}}
	rcv := httptest.NewTLSServer(inbox)
	defer rcv.Close()

	if code, _ := call(t, srv, admin, "PUT", "/v1/ns/shop/queues/emails", map[string]any{"name": "emails", "callback_url": "http://example.com/hooks"}); code != 400 {
		t.Fatalf("plain http queue callback: %d", code)
	}
	if code, out := call(t, srv, admin, "PUT", "/v1/ns/shop/queues/emails", map[string]any{"name": "emails", "callback_url": rcv.URL + "/hooks"}); code != 200 {
		t.Fatalf("set queue: %d %v", code, out)
	}
	m := runnel.NewMaintainer(store, runnel.MaintainerConfig{Namespaces: []string{"shop"}, Interval: 50 * time.Millisecond,
		Callbacks: runnel.CallbackConfig{Enabled: true, Client: rcv.Client()}})
	background(t, m.Run)
	w := remote.NewWorker(remote.New(srv.URL, worker, "shop"), runnel.WorkerConfig{Queues: []string{"emails"}, Lease: 5 * time.Second, ShutdownTimeout: time.Second})
	w.HandleFunc("welcome", func(ctx context.Context, j *runnel.Job) error {
		var in struct{ To string }
		if err := j.Bind(&in); err != nil {
			return runnel.Fail(err)
		}
		return j.SetResult(map[string]string{"sent": in.To})
	})
	background(t, w.Run)

	if resp, _ := send(t, srv, worker, "POST", "/v1/ns/shop/jobs", "", map[string]any{"queue": "emails", "kind": "welcome"}); resp.StatusCode != 403 {
		t.Fatalf("worker key enqueued: %d", resp.StatusCode)
	}
	if resp, _ := send(t, srv, producer, "POST", "/v1/ns/shop/claim", "", map[string]any{"queues": []string{"emails"}, "worker": "w", "limit": 1}); resp.StatusCode != 403 {
		t.Fatalf("producer key claimed: %d", resp.StatusCode)
	}
	resp, out := send(t, srv, producer, "POST", "/v1/ns/shop/jobs", "wait=15", map[string]any{"queue": "emails", "kind": "welcome", "payload": map[string]string{"to": "ana@example.com"}})
	if resp.StatusCode != 200 || out["state"] != "completed" || out["callback_url"] != rcv.URL+"/hooks" {
		t.Fatalf("enqueue and wait: %d %v", resp.StatusCode, out)
	}
	if res, _ := json.Marshal(out["result"]); string(res) != `{"sent":"ana@example.com"}` {
		t.Fatalf("result %s", res)
	}
	id, _ := out["id"].(string)
	eventually(t, "the callback", func() bool { got, _ := inbox.seen(); return len(got) == 1 })
	got, bad := inbox.seen()
	if bad != 0 || got[0].Event != "job.completed" || got[0].Job.ID != id || got[0].Job.State != runnel.StateCompleted || got[0].Job.Namespace != "shop" {
		t.Fatalf("callback %+v, %d refused", got, bad)
	}
}

func TestStorySharedPool(t *testing.T) {
	spec.Rule(t, "U21", "S19", "S20", "S21", "A03", "C12")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/media", nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	app := remote.New(srv.URL, key(t, srv, "media", "admin", "producer", "reader"), "media")
	operator := key(t, srv, "media", "admin")
	reader := key(t, srv, "media", "reader")
	const pool = "/v1/ns/media/pools/gpu"

	if err := app.UpdatePool(ctx, "gpu", func(p *runnel.PoolConfig) { p.Slots = 2 }); err != nil {
		t.Fatal(err)
	}
	read := askAs(t, srv, operator, "GET", pool, "")
	if read.code != 200 || read.tag == "" || read.body["slots"] != float64(2) {
		t.Fatalf("operator reads the pool: %d %q %v", read.code, read.tag, read.body)
	}
	if got := askAs(t, srv, reader, "PATCH", pool, `{"paused":true}`, "Content-Type", mergePatch); got.code != 403 {
		t.Fatalf("reader paused a pool: %d", got.code)
	}
	paused := askAs(t, srv, operator, "PATCH", pool, `{"paused":true}`, "Content-Type", mergePatch, "If-Match", read.tag)
	if paused.code != 200 || paused.body["paused"] != true || paused.tag == read.tag {
		t.Fatalf("operator pause: %d %q %v", paused.code, paused.tag, paused.body)
	}
	if got := askAs(t, srv, operator, "PATCH", pool, `{"paused":false}`, "Content-Type", mergePatch, "If-Match", read.tag); got.code != 412 {
		t.Fatalf("patch with the tag from before the pause: %d", got.code)
	}
	if err := app.UpdatePool(ctx, "gpu", func(p *runnel.PoolConfig) { p.Slots = 4 }); err != nil {
		t.Fatal(err)
	}
	now := askAs(t, srv, operator, "GET", pool, "")
	if now.body["paused"] != true || now.body["slots"] != float64(4) {
		t.Fatalf("the app's slots undid the pause: %v", now.body)
	}

	var gpu []string
	for range 3 {
		res, err := app.Enqueue(ctx, "renders", "k", nil, runnel.Pool("gpu"))
		if err != nil {
			t.Fatal(err)
		}
		gpu = append(gpu, res.ID)
	}
	probe, _ := app.Enqueue(ctx, "renders", "k", nil, runnel.Pool("cpu"))
	w := remote.NewWorker(remote.New(srv.URL, key(t, srv, "media", "worker"), "media"),
		runnel.WorkerConfig{Queues: []string{"renders"}, Lease: 5 * time.Second, ShutdownTimeout: time.Second, PollInterval: 50 * time.Millisecond})
	w.HandleFunc("k", func(ctx context.Context, j *runnel.Job) error { return nil })
	background(t, w.Run)
	state := func(id string) runnel.State {
		j, err := app.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j.State
	}
	eventually(t, "a job of another pool", func() bool { return state(probe.ID) == runnel.StateCompleted })
	for _, id := range gpu {
		if st := state(id); st != runnel.StateWaiting {
			t.Fatalf("job of a paused pool is %s", st)
		}
	}
	if got := askAs(t, srv, operator, "PATCH", pool, `{"paused":false}`, "Content-Type", mergePatch, "If-Match", now.tag); got.code != 200 {
		t.Fatalf("resume: %d %v", got.code, got.body)
	}
	eventually(t, "the paused pool's jobs", func() bool {
		for _, id := range gpu {
			if state(id) != runnel.StateCompleted {
				return false
			}
		}
		return true
	})
}

func TestStoryKeyLifecycle(t *testing.T) {
	spec.Rule(t, "U22", "A01", "A04", "A05")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/team", nil)
	stats := "/v1/ns/team/stats"
	first := key(t, srv, "team", "reader")
	if code, _ := call(t, srv, first, "GET", stats, nil); code != 200 {
		t.Fatalf("new key: %d", code)
	}
	id, _, ok := runnel.ParseAPIKey(first)
	if !ok {
		t.Fatalf("key %q does not parse", first)
	}
	if code, _ := call(t, srv, admin, "DELETE", "/v1/namespaces/team/keys/"+id, nil); code != 204 {
		t.Fatalf("delete key: %d", code)
	}
	resp, _ := send(t, srv, first, "GET", stats, "", nil)
	if resp.StatusCode != 401 || !strings.Contains(resp.Header.Get("WWW-Authenticate"), "Bearer") {
		t.Fatalf("deleted key: %d %v", resp.StatusCode, resp.Header)
	}
	second := key(t, srv, "team", "reader")
	if code, _ := call(t, srv, second, "GET", stats, nil); code != 200 {
		t.Fatalf("second key: %d", code)
	}
	if code, _ := call(t, srv, admin, "DELETE", "/v1/namespaces/team", nil); code != 204 {
		t.Fatalf("delete namespace: %d", code)
	}
	call(t, srv, admin, "PUT", "/v1/namespaces/team", nil)
	if code, _ := call(t, srv, second, "GET", stats, nil); code != 401 {
		t.Fatalf("key of a deleted namespace came back: %d", code)
	}
	if code, _ := call(t, srv, "rnk_not_a_key", "GET", stats, nil); code != 401 {
		t.Fatalf("garbage key: %d", code)
	}
}

func TestStoryBackpressure(t *testing.T) {
	spec.Rule(t, "U23", "J08", "S07", "S04")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/flood", nil)
	producer := key(t, srv, "flood", "producer")
	worker := key(t, srv, "flood", "worker")
	if code, _ := call(t, srv, admin, "PUT", "/v1/ns/flood/queues/capped", map[string]any{"name": "capped", "max_queued": 2}); code != 200 {
		t.Fatalf("set queue: %d", code)
	}
	enqueue := func() (reply, map[string]any) {
		return send(t, srv, producer, "POST", "/v1/ns/flood/jobs", "", map[string]any{"queue": "capped", "kind": "k"})
	}
	for range 2 {
		if resp, out := enqueue(); resp.StatusCode != 201 || resp.Header.Get("Location") == "" {
			t.Fatalf("enqueue under the cap: %d %v", resp.StatusCode, out)
		}
	}
	full := func(when string) {
		t.Helper()
		resp, out := enqueue()
		if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" || out["title"] != "queue full" {
			t.Fatalf("%s: %d %v %v", when, resp.StatusCode, resp.Header, out)
		}
	}
	full("third waiting job")
	jobs, err := remote.New(srv.URL, worker, "flood").Source(0).Claim(context.Background(), "w", []string{"capped"}, 1, 5*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	if resp, out := enqueue(); resp.StatusCode != 201 {
		t.Fatalf("enqueue once a job runs: %d %v", resp.StatusCode, out)
	}
	full("full again")
}
