package server_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/hybrid"
)

func TestReadyz(t *testing.T) {
	spec.Rule(t, "S13", "A01")
	s, err := hybrid.Open(hybrid.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	api := server.New(s, server.Config{AdminToken: admin})
	srv := httptest.NewServer(api)
	defer srv.Close()
	if code, _ := get(t, srv.URL+"/readyz"); code != 200 {
		t.Fatalf("readyz %d", code)
	}
	api.Drain()
	if code, _ := get(t, srv.URL+"/readyz"); code != 503 {
		t.Fatalf("readyz while draining %d", code)
	}
	if code, _ := get(t, srv.URL+"/healthz"); code != 200 {
		t.Fatalf("healthz while draining %d", code)
	}
}

func TestBackupRoute(t *testing.T) {
	spec.Rule(t, "S14", "A02")
	s, err := hybrid.Open(hybrid.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	srv := httptest.NewServer(server.New(s, server.Config{AdminToken: admin}))
	defer srv.Close()
	call(t, srv, admin, "PUT", "/v1/namespaces/b", nil)
	if _, err := runnel.NewClient(s, "b").Enqueue(context.Background(), "q", "k", nil); err != nil {
		t.Fatal(err)
	}
	if code, _ := call(t, srv, key(t, srv, "b", "admin"), "GET", "/v1/backup", nil); code != 403 {
		t.Fatalf("backup with a namespace key %d", code)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/v1/backup", nil)
	req.Header.Set("Authorization", "Bearer "+admin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/x-tar" {
		t.Fatalf("backup %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if _, err := tar.NewReader(bytes.NewReader(body)).Next(); err != nil {
		t.Fatalf("backup is not a tar: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "r")
	if err := hybrid.Restore(bytes.NewReader(body), dir); err != nil {
		t.Fatal(err)
	}
	r, err := hybrid.Open(hybrid.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if st, _ := runnel.NewClient(r, "b").Stats(context.Background()); len(st) != 1 || st[0].Counts[runnel.StateWaiting] != 1 {
		t.Fatalf("restored stats %+v", st)
	}
	sq, _ := setup(t)
	if code, _ := call(t, sq, admin, "GET", "/v1/backup", nil); code != 501 {
		t.Fatalf("backup of sqlite %d", code)
	}
}

func TestEnqueueCarriesTrace(t *testing.T) {
	spec.Rule(t, "S15")
	srv, st := setup(t)
	call(t, srv, admin, "PUT", "/v1/namespaces/tr", nil)
	tok := key(t, srv, "tr", "producer")
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	send := func(meta map[string]string) string {
		b, _ := json.Marshal(map[string]any{"queue": "q", "kind": "k", "meta": meta})
		req, _ := http.NewRequest("POST", srv.URL+"/v1/ns/tr/jobs", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("traceparent", tp)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct{ ID string }
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out.ID
	}
	own := "00-11111111111111111111111111111111-2222222222222222-01"
	for id, want := range map[string]string{send(nil): tp, send(map[string]string{"traceparent": own}): own} {
		jobs, err := st.Get(context.Background(), "tr", []string{id})
		if err != nil || len(jobs) != 1 || jobs[0].Meta["traceparent"] != want {
			t.Fatalf("job %s meta %v: %v", id, jobs, err)
		}
	}
}

func TestServerCapsAreSettings(t *testing.T) {
	spec.Rule(t, "S16")
	s, err := hybrid.Open(hybrid.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	srv := httptest.NewServer(server.New(s, server.Config{AdminToken: admin, MaxBatch: 2, MaxClaim: 1, MaxExtend: 1, MaxDelay: time.Hour}))
	defer srv.Close()
	call(t, srv, admin, "PUT", "/v1/namespaces/c", nil)
	tok := key(t, srv, "c", "producer", "worker")
	job := map[string]any{"queue": "q", "kind": "k"}
	if code, _ := call(t, srv, tok, "POST", "/v1/ns/c/jobs", map[string]any{"jobs": []any{job, job, job}}); code != 400 {
		t.Fatalf("3 jobs over max_batch 2: %d", code)
	}
	if code, _ := call(t, srv, tok, "POST", "/v1/ns/c/jobs", map[string]any{"queue": "q", "kind": "k", "delay": "2h"}); code != 400 {
		t.Fatalf("delay over max_delay: %d", code)
	}
	if code, _ := call(t, srv, tok, "POST", "/v1/ns/c/jobs", map[string]any{"jobs": []any{job, job}}); code != 200 {
		t.Fatalf("batch of 2: %d", code)
	}
	code, out := call(t, srv, tok, "POST", "/v1/ns/c/claim", map[string]any{"worker": "w", "queues": []string{"q"}, "limit": 10})
	if jobs, _ := out["jobs"].([]any); code != 200 || len(jobs) != 1 {
		t.Fatalf("claim over max_claim 1: %d %v", code, out)
	}
	if code, _ := call(t, srv, tok, "POST", "/v1/ns/c/extend", map[string]any{"worker": "w", "ids": []string{"a", "b"}, "lease": "30s"}); code != 400 {
		t.Fatalf("extend over max_extend 1: %d", code)
	}
}
