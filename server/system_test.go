package server_test

import (
	"net/http/httptest"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/hybrid"
)

type plainStore struct{ runnel.Store }

func TestSystemRoute(t *testing.T) {
	spec.Rule(t, "S17", "A02")
	s, err := hybrid.Open(hybrid.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	srv := httptest.NewServer(server.New(s, server.Config{AdminToken: admin, Version: "v9.9.9"}))
	defer srv.Close()
	call(t, srv, admin, "PUT", "/v1/namespaces/sys", nil)
	if code, _ := call(t, srv, key(t, srv, "sys", "admin"), "GET", "/v1/system", nil); code != 403 {
		t.Fatalf("system with a namespace key %d", code)
	}
	code, body := call(t, srv, admin, "GET", "/v1/system", nil)
	if code != 200 || body["version"] != "v9.9.9" || body["started_at"] == "" || body["go_version"] == "" {
		t.Fatalf("system %d %v", code, body)
	}
	if mem, _ := body["memory"].(map[string]any); mem == nil || mem["total_bytes"].(float64) <= 0 || mem["heap_bytes"].(float64) <= 0 {
		t.Fatalf("memory %v", body["memory"])
	}
	st, _ := body["storage"].(map[string]any)
	if st == nil || st["kind"] != "hybrid" || st["bytes"].(float64) <= 0 {
		t.Fatalf("storage %v", body["storage"])
	}

	sq, _ := setup(t)
	if _, body := call(t, sq, admin, "GET", "/v1/system", nil); body["storage"].(map[string]any)["kind"] != "sqlite" {
		t.Fatalf("sqlite storage %v", body["storage"])
	}

	plain := httptest.NewServer(server.New(plainStore{s}, server.Config{AdminToken: admin}))
	defer plain.Close()
	code, body = call(t, plain, admin, "GET", "/v1/system", nil)
	if v, ok := body["storage"]; code != 200 || !ok || v != nil {
		t.Fatalf("store without a report %d %v", code, body)
	}
}
