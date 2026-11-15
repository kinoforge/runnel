package server_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
)

func TestErrorStatuses(t *testing.T) {
	spec.Rule(t, "S01", "S02")
	srv, store := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/codes", nil)
	k := key(t, srv, "codes", "producer", "reader", "admin")
	ctx := context.Background()
	c := runnel.NewClient(store, "codes")
	done, _ := c.Enqueue(ctx, "q", "k", nil)
	claimed, err := store.Claim(ctx, "codes", runnel.ClaimRequest{Queues: []string{"q"}, Worker: "w", Limit: 1, Lease: time.Minute, Now: time.Now()})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim %v %v", claimed, err)
	}
	if ok, err := store.Apply(ctx, "codes", []runnel.Change{{Job: runnel.Decide(claimed[0], runnel.Outcome{Kind: runnel.OutcomeDone}, time.Now())}}, time.Now()); err != nil || !ok[0] {
		t.Fatalf("finish %v %v", ok, err)
	}
	waiting, _ := c.Enqueue(ctx, "q", "k", nil)
	call(t, srv, k, "PUT", "/v1/ns/codes/queues/full", map[string]any{"name": "full", "max_queued": 1})
	call(t, srv, k, "POST", "/v1/ns/codes/jobs", map[string]any{"queue": "full", "kind": "k"})
	tag := askAs(t, srv, k, "PUT", "/v1/ns/codes/pools/p", `{"name":"p","slots":1}`).tag
	askAs(t, srv, k, "PUT", "/v1/ns/codes/pools/p", `{"name":"p","slots":2}`)

	for _, c := range []struct {
		name, method, path, body string
		header                   []string
		want                     int
	}{
		{"invalid input", "POST", "/v1/ns/codes/jobs", `{"queue":"q","kind":"a b"}`, nil, 400},
		{"unknown job", "GET", "/v1/ns/codes/jobs/" + runnel.NewID(), "", nil, 404},
		{"unknown setting", "GET", "/v1/ns/codes/queues/missing", "", nil, 404},
		{"delete an unknown queue", "DELETE", "/v1/ns/codes/queues/missing", "", nil, 404},
		{"delete an unknown pool", "DELETE", "/v1/ns/codes/pools/missing", "", nil, 404},
		{"delete an unknown schedule", "DELETE", "/v1/ns/codes/schedules/missing", "", nil, 404},
		{"cancel a finished job", "POST", "/v1/ns/codes/jobs/" + done.ID + "/cancel", "", nil, 409},
		{"retry a job that is not finished", "POST", "/v1/ns/codes/jobs/" + waiting.ID + "/retry", "", nil, 409},
		{"stale tag", "PUT", "/v1/ns/codes/pools/p", `{"name":"p","slots":3}`, []string{"If-Match", tag}, 412},
		{"patch of another type", "PATCH", "/v1/ns/codes/pools/p", `{"slots":3}`, []string{"Content-Type", "text/plain"}, 415},
		{"missing dependency", "POST", "/v1/ns/codes/jobs", `{"queue":"q","kind":"k","depends_on":["` + runnel.NewID() + `"]}`, nil, 422},
		{"full queue", "POST", "/v1/ns/codes/jobs", `{"queue":"full","kind":"k"}`, nil, 429},
	} {
		req, _ := http.NewRequest(c.method, srv.URL+c.path, strings.NewReader(c.body))
		req.Header.Set("Authorization", "Bearer "+k)
		for i := 0; i+1 < len(c.header); i += 2 {
			req.Header.Set(c.header[i], c.header[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/problem+json") {
			t.Errorf("%s: %d %q, want %d as problem+json", c.name, resp.StatusCode, resp.Header.Get("Content-Type"), c.want)
		}
	}
	if code, _ := call(t, srv, admin, "GET", "/v1/namespaces/missing", nil); code != 404 {
		t.Errorf("unknown namespace: %d", code)
	}
	if code, _ := call(t, srv, admin, "DELETE", "/v1/namespaces/missing", nil); code != 404 {
		t.Errorf("delete an unknown namespace: %d", code)
	}
	if code, _ := call(t, srv, admin, "DELETE", "/v1/namespaces/codes/keys/000000000000", nil); code != 404 {
		t.Errorf("unknown key: %d", code)
	}
}

func TestSettingNamesMatchPaths(t *testing.T) {
	spec.Rule(t, "S09", "S20")
	srv, _ := setup(t)
	bodies := map[string]string{
		"queues":    `{"name":"%s","max_running":1}`,
		"pools":     `{"name":"%s","slots":1}`,
		"schedules": `{"name":"%s","spec":"@daily","queue":"q","kind":"k"}`,
	}
	for kind, body := range bodies {
		path := "/v1/ns/default/" + kind + "/a"
		if got := ask(t, srv, "PUT", path, strings.ReplaceAll(body, "%s", "b")); got.code != 400 {
			t.Errorf("%s: put with another name: %d", kind, got.code)
		}
		if got := ask(t, srv, "PUT", path, strings.ReplaceAll(body, "%s", "a")); got.code != 200 {
			t.Errorf("%s: put with the path's name: %d %v", kind, got.code, got.body)
		}
		if got := ask(t, srv, "PATCH", path, `{"name":"b"}`, "Content-Type", mergePatch); got.code != 400 {
			t.Errorf("%s: patch to another name: %d", kind, got.code)
		}
		if got := ask(t, srv, "PATCH", path, `{"name":"a"}`, "Content-Type", mergePatch); got.code != 200 || got.body["name"] != "a" {
			t.Errorf("%s: patch with the path's name: %d %v", kind, got.code, got.body)
		}
		if got := ask(t, srv, "GET", "/v1/ns/default/"+kind+"/b", ""); got.code != 404 {
			t.Errorf("%s: a refused write made b: %d", kind, got.code)
		}
	}
}

func TestPreconditionLists(t *testing.T) {
	spec.Rule(t, "S19")
	srv, _ := setup(t)
	const p = "/v1/ns/default/pools/p"
	put := func(slots string, header ...string) answer {
		return ask(t, srv, "PUT", p, `{"name":"p","slots":`+slots+`}`, header...)
	}
	put("1")
	if got := put("2", "If-None-Match", `"other"`); got.code != 200 {
		t.Fatalf("If-None-Match with another tag: %d", got.code)
	}
	cur := ask(t, srv, "GET", p, "").tag
	if got := put("3", "If-None-Match", "W/"+cur); got.code != 412 {
		t.Fatalf("If-None-Match compares weakly, so W/ of the tag matches: %d", got.code)
	}
	if got := put("3", "If-None-Match", `"other", `+cur); got.code != 412 {
		t.Fatalf("If-None-Match list holding the tag: %d", got.code)
	}
	req, _ := http.NewRequest("PUT", srv.URL+p, strings.NewReader(`{"name":"p","slots":4}`))
	req.Header.Set("Authorization", "Bearer "+admin)
	req.Header.Add("If-Match", `"other"`)
	req.Header.Add("If-Match", cur)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("If-Match over two header lines: %d", resp.StatusCode)
	}
	if got := ask(t, srv, "GET", p, ""); got.body["slots"] != float64(4) {
		t.Fatalf("refused writes changed the pool: %v", got.body)
	}
	if got := ask(t, srv, "PATCH", "/v1/ns/default/pools/fresh", `{"slots":1}`, "Content-Type", mergePatch, "If-None-Match", "*"); got.code != 200 {
		t.Fatalf("create-only patch: %d", got.code)
	}
	if got := ask(t, srv, "PATCH", "/v1/ns/default/pools/fresh", `{"slots":2}`, "Content-Type", mergePatch, "If-None-Match", "*"); got.code != 412 {
		t.Fatalf("create-only patch of an existing pool: %d", got.code)
	}
}

func TestMergePatchNested(t *testing.T) {
	spec.Rule(t, "S20", "J10")
	srv, _ := setup(t)
	const s = "/v1/ns/default/schedules/report"
	ask(t, srv, "PUT", s, `{"name":"report","spec":"@daily","queue":"q","kind":"k","meta":{"a":"1","b":"2"}}`)
	got := ask(t, srv, "PATCH", s, `{"meta":{"a":null,"c":"3"}}`, "Content-Type", mergePatch)
	meta, _ := got.body["meta"].(map[string]any)
	if got.code != 200 || len(meta) != 2 || meta["b"] != "2" || meta["c"] != "3" || got.body["spec"] != "@daily" {
		t.Fatalf("nested merge: %d %v", got.code, got.body)
	}
	const q = "/v1/ns/default/queues/q"
	ask(t, srv, "PUT", q, `{"name":"q","backoff":{"base":"2s","max":"1m"},"timeout":30}`)
	got = ask(t, srv, "PATCH", q, `{"backoff":{"max":"5m"}}`, "Content-Type", mergePatch)
	backoff, _ := got.body["backoff"].(map[string]any)
	if got.code != 200 || backoff["base"] != "2s" || backoff["max"] != "5m0s" || got.body["timeout"] != "30s" {
		t.Fatalf("nested backoff merge: %d %v", got.code, got.body)
	}
	got = ask(t, srv, "PATCH", q, `{"backoff":null,"timeout":"1m"}`, "Content-Type", mergePatch)
	if _, has := got.body["backoff"]; got.code != 200 || has || got.body["timeout"] != "1m0s" {
		t.Fatalf("clear backoff: %d %v", got.code, got.body)
	}
	if got := ask(t, srv, "PATCH", q, `{"timeout":"soon"}`, "Content-Type", mergePatch); got.code != 400 {
		t.Fatalf("bad duration in a patch: %d", got.code)
	}
	if got := ask(t, srv, "PATCH", q, `{"max_running":1.5}`, "Content-Type", mergePatch); got.code != 400 {
		t.Fatalf("fraction for a count: %d", got.code)
	}
}

func TestBatchAnswers(t *testing.T) {
	spec.Rule(t, "S04", "J04", "J02")
	srv, _ := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/b", nil)
	k := key(t, srv, "b", "producer", "reader")
	code, out := call(t, srv, k, "POST", "/v1/ns/b/jobs", map[string]any{"jobs": []any{
		map[string]any{"queue": "q", "kind": "k", "unique_key": "u"},
		map[string]any{"queue": "q", "kind": "k", "unique_key": "u"},
		map[string]any{"queue": "q", "kind": "k", "depends_on": []string{runnel.NewID()}},
		map[string]any{"queue": "q", "kind": "k"},
	}})
	results, _ := out["results"].([]any)
	if code != 200 || len(results) != 4 {
		t.Fatalf("batch: %d %v", code, out)
	}
	r := func(i int) map[string]any { m, _ := results[i].(map[string]any); return m }
	if r(0)["id"] == nil || r(0)["duplicate"] != nil || r(1)["duplicate"] != true || r(1)["id"] != r(0)["id"] {
		t.Errorf("unique key twice in one batch: %v %v", r(0), r(1))
	}
	if r(2)["id"] != nil || r(2)["error"] == nil || r(3)["id"] == nil || r(3)["error"] != nil {
		t.Errorf("missing dependency in a batch: %v %v", r(2), r(3))
	}
	jobs := make([]any, 1001)
	for i := range jobs {
		jobs[i] = map[string]any{"queue": "big", "kind": "k"}
	}
	if code, _ := call(t, srv, k, "POST", "/v1/ns/b/jobs", map[string]any{"jobs": jobs}); code != 400 {
		t.Errorf("1001 jobs: %d", code)
	}
	if code, _ := call(t, srv, k, "POST", "/v1/ns/b/jobs", map[string]any{"jobs": []any{map[string]any{"queue": "big", "kind": "k"}, map[string]any{"queue": "big", "kind": ""}}}); code != 400 {
		t.Errorf("batch with a bad job: %d", code)
	}
	if code, out := call(t, srv, k, "GET", "/v1/ns/b/jobs?queue=big", nil); code != 200 || len(out["items"].([]any)) != 0 {
		t.Errorf("refused batches wrote jobs: %d %v", code, out)
	}
}
