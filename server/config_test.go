package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
)

const mergePatch = "application/merge-patch+json"

type answer struct {
	code int
	tag  string
	body map[string]any
}

func ask(t *testing.T, srv *httptest.Server, method, path, body string, header ...string) answer {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+admin)
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

func TestConfigETag(t *testing.T) {
	spec.Rule(t, "S19")
	srv, _ := setup(t)
	const p = "/v1/ns/default/pools/p"
	first := ask(t, srv, "PUT", p, `{"name":"p","slots":2}`)
	if first.code != 200 || !strings.HasPrefix(first.tag, `"`) {
		t.Fatalf("put %d tag %q", first.code, first.tag)
	}
	if got := ask(t, srv, "GET", p, ""); got.tag != first.tag {
		t.Fatalf("get tag %q, put tag %q", got.tag, first.tag)
	}
	second := ask(t, srv, "PUT", p, `{"name":"p","slots":3}`, "If-Match", first.tag)
	if second.code != 200 || second.tag == first.tag {
		t.Fatalf("matching put %d tag %q", second.code, second.tag)
	}
	if got := ask(t, srv, "PUT", p, `{"name":"p","slots":4}`, "If-Match", first.tag); got.code != 412 || got.body["title"] != "precondition failed" {
		t.Fatalf("stale put %d %v", got.code, got.body)
	}
	if got := ask(t, srv, "PUT", p, `{"name":"p","slots":4}`, "If-Match", "W/"+second.tag); got.code != 412 {
		t.Fatalf("weak tag matched If-Match: %d", got.code)
	}
	if got := ask(t, srv, "PUT", p, `{"name":"p","slots":4}`, "If-Match", `"x", `+second.tag); got.code != 200 {
		t.Fatalf("tag list put %d", got.code)
	}
	if got := ask(t, srv, "PUT", p, `{"name":"p","slots":5}`, "If-Match", "*"); got.code != 200 {
		t.Fatalf("If-Match * on an existing pool: %d", got.code)
	}
	if got := ask(t, srv, "GET", p, ""); got.body["slots"] != float64(5) {
		t.Fatalf("pool %v", got.body)
	}
	const q = "/v1/ns/default/pools/q"
	if got := ask(t, srv, "PUT", q, `{"name":"q","slots":1}`, "If-Match", "*"); got.code != 412 {
		t.Fatalf("If-Match on a missing pool: %d", got.code)
	}
	if got := ask(t, srv, "PUT", q, `{"name":"q","slots":1}`, "If-None-Match", "*"); got.code != 200 {
		t.Fatalf("create only: %d", got.code)
	}
	if got := ask(t, srv, "PUT", q, `{"name":"q","slots":2}`, "If-None-Match", "*"); got.code != 412 {
		t.Fatalf("create only over an existing pool: %d", got.code)
	}
}

func TestConfigPatch(t *testing.T) {
	spec.Rule(t, "S20")
	srv, _ := setup(t)
	const p = "/v1/ns/default/pools/p"
	ask(t, srv, "PUT", p, `{"name":"p","slots":2,"rate":5,"burst":3,"paused":true}`)
	got := ask(t, srv, "PATCH", p, `{"slots":7}`, "Content-Type", mergePatch)
	if got.code != 200 || got.body["slots"] != float64(7) || got.body["rate"] != float64(5) || got.body["burst"] != float64(3) || got.body["paused"] != true {
		t.Fatalf("patch %d %v", got.code, got.body)
	}
	if read := ask(t, srv, "GET", p, ""); read.tag != got.tag {
		t.Fatalf("patch tag %q, get tag %q", got.tag, read.tag)
	}
	got = ask(t, srv, "PATCH", p, `{"rate":null,"paused":false}`, "Content-Type", "application/json")
	if _, has := got.body["rate"]; got.code != 200 || has || got.body["burst"] != float64(3) || got.body["paused"] != nil {
		t.Fatalf("null clears: %d %v", got.code, got.body)
	}
	if got := ask(t, srv, "PATCH", p, `{"rate":0.1}`, "Content-Type", mergePatch); got.body["rate"] != 0.1 {
		t.Fatalf("rate %v", got.body)
	}
	created := ask(t, srv, "PATCH", "/v1/ns/default/pools/new", `{"slots":1}`, "Content-Type", mergePatch)
	if created.code != 200 || created.body["name"] != "new" || created.body["slots"] != float64(1) {
		t.Fatalf("patch creates: %d %v", created.code, created.body)
	}
	if got := ask(t, srv, "PATCH", "/v1/ns/default/queues/fresh", `{"paused":true}`, "Content-Type", mergePatch); got.code != 200 || got.body["paused"] != true {
		t.Fatalf("queue patch creates: %d %v", got.code, got.body)
	}
	for body, want := range map[string]int{
		`{"name":"other"}`: 400,
		`{"slotz":1}`:      400,
		`{"slots":-1}`:     400,
		`[1]`:              400,
		`{"slots":1} {}`:   400,
	} {
		if got := ask(t, srv, "PATCH", p, body, "Content-Type", mergePatch); got.code != want {
			t.Fatalf("patch %s: %d, want %d", body, got.code, want)
		}
	}
	if got := ask(t, srv, "PATCH", p, `{"slots":1}`, "Content-Type", "text/plain"); got.code != 415 {
		t.Fatalf("text body: %d", got.code)
	}
	if got := ask(t, srv, "PATCH", "/v1/ns/default/schedules/s", `{"queue":"q"}`, "Content-Type", mergePatch); got.code != 400 {
		t.Fatalf("incomplete schedule: %d", got.code)
	}
	cur := ask(t, srv, "GET", p, "")
	if got := ask(t, srv, "PATCH", p, `{"slots":8}`, "Content-Type", mergePatch, "If-Match", created.tag); got.code != 412 {
		t.Fatalf("stale patch: %d", got.code)
	}
	if got := ask(t, srv, "PATCH", p, `{"slots":8}`, "Content-Type", mergePatch, "If-Match", cur.tag); got.code != 200 || got.body["slots"] != float64(8) {
		t.Fatalf("matching patch: %d %v", got.code, got.body)
	}
	if got := ask(t, srv, "PATCH", "/v1/ns/default/pools/none", `{"slots":1}`, "Content-Type", mergePatch, "If-Match", "*"); got.code != 412 {
		t.Fatalf("If-Match on a missing pool: %d", got.code)
	}
	call(t, srv, admin, "PUT", "/v1/namespaces/gate", nil)
	producer := key(t, srv, "gate", "producer")
	req, _ := http.NewRequest("PATCH", srv.URL+"/v1/ns/gate/pools/p", strings.NewReader(`{"slots":1}`))
	req.Header.Set("Authorization", "Bearer "+producer)
	req.Header.Set("Content-Type", mergePatch)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("producer patched a pool: %d", resp.StatusCode)
	}
}

func TestRemoteUpdateKeepsOtherWriters(t *testing.T) {
	spec.Rule(t, "S21")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/gate", nil)
	ctx := context.Background()
	c := remote.New(srv.URL, key(t, srv, "gate", "admin"), "gate")
	calls := 0
	err := c.UpdatePool(ctx, "m", func(p *runnel.PoolConfig) {
		calls++
		if calls == 1 {
			if err := c.SetPool(ctx, runnel.PoolConfig{Name: "m", Paused: true, Rate: 2}); err != nil {
				t.Fatal(err)
			}
		}
		p.Slots = 5
	})
	if err != nil || calls != 2 {
		t.Fatalf("create race: %v after %d calls", err, calls)
	}
	calls = 0
	err = c.UpdatePool(ctx, "m", func(p *runnel.PoolConfig) {
		calls++
		if calls == 1 {
			if err := c.SetPool(ctx, runnel.PoolConfig{Name: "m", Slots: 5, Paused: true, Rate: 2, Burst: 4}); err != nil {
				t.Fatal(err)
			}
		}
		p.GroupSlots = 1
	})
	if err != nil || calls != 2 {
		t.Fatalf("update race: %v after %d calls", err, calls)
	}
	got := ask(t, srv, "GET", "/v1/ns/gate/pools/m", "")
	if b := got.body; b["slots"] != float64(5) || b["group_slots"] != float64(1) || b["paused"] != true || b["rate"] != float64(2) || b["burst"] != float64(4) {
		t.Fatalf("pool %v", b)
	}
	if err := c.SetQueue(ctx, runnel.QueueConfig{Name: "image", MaxQueued: 5}); err != nil {
		t.Fatal(err)
	}
	if err := c.UpdateQueue(ctx, "image", func(q *runnel.QueueConfig) { q.Paused = true }); err != nil {
		t.Fatal(err)
	}
	if got := ask(t, srv, "GET", "/v1/ns/gate/queues/image", ""); got.body["max_queued"] != float64(5) || got.body["paused"] != true {
		t.Fatalf("queue %v", got.body)
	}
	if err := c.UpdateQueue(ctx, "image", func(q *runnel.QueueConfig) { q.MaxQueued = -1 }); !errors.Is(err, runnel.ErrInvalid) {
		t.Fatalf("bad update: %v", err)
	}
}

func TestQueueCallbackOverAPI(t *testing.T) {
	spec.Rule(t, "S03")
	srv, _ := setup(t)
	const q, hook = "/v1/ns/default/queues/q", "https://example.com/h"
	if got := ask(t, srv, "PUT", q, `{"name":"q","callback_url":"http://example.com/h"}`); got.code != 400 {
		t.Fatalf("http queue callback put: %d", got.code)
	}
	if got := ask(t, srv, "PATCH", q, `{"callback_url":"http://example.com/h"}`, "Content-Type", mergePatch); got.code != 400 {
		t.Fatalf("http queue callback patch: %d", got.code)
	}
	if got := ask(t, srv, "PATCH", q, `{"callback_url":"`+hook+`"}`, "Content-Type", mergePatch); got.code != 200 || got.body["callback_url"] != hook {
		t.Fatalf("queue callback patch: %d %v", got.code, got.body)
	}
	added := ask(t, srv, "POST", "/v1/ns/default/jobs", `{"queue":"q","kind":"k"}`)
	id, _ := added.body["id"].(string)
	if got := ask(t, srv, "GET", "/v1/ns/default/jobs/"+id, ""); got.body["callback_url"] != hook {
		t.Fatalf("job %d %v", got.code, got.body)
	}
	const s = `{"name":"s","queue":"q","kind":"k","spec":"@every 1h","callback_url":"%s"}`
	if got := ask(t, srv, "PUT", "/v1/ns/default/schedules/s", fmt.Sprintf(s, "http://example.com/h")); got.code != 400 {
		t.Fatalf("http schedule callback: %d", got.code)
	}
	if got := ask(t, srv, "PUT", "/v1/ns/default/schedules/s", fmt.Sprintf(s, hook)); got.code != 200 {
		t.Fatalf("https schedule callback: %d %v", got.code, got.body)
	}
	for _, body := range []string{
		`{"name":"r","queue":"runnel.callbacks","kind":"k","spec":"@every 1h"}`,
		`{"name":"r","queue":"q","kind":"runnel.callback","spec":"@every 1h"}`,
	} {
		if got := ask(t, srv, "PUT", "/v1/ns/default/schedules/r", body); got.code != 400 {
			t.Fatalf("reserved schedule %s: %d", body, got.code)
		}
	}
}
