package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
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
		wg.Go(func() {
			tokens <- key(t, srv, "race", "reader")
		})
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
