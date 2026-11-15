package server_test

import (
	"context"
	"errors"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/remote"
	"github.com/kinoforge/runnel/server"
	"github.com/kinoforge/runnel/store/hybrid"
)

func TestLowDiskStatus(t *testing.T) {
	spec.Rule(t, "S02", "D03")
	s, err := hybrid.Open(hybrid.Options{Dir: t.TempDir(), MinFreeBytes: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	srv := httptest.NewServer(server.New(s, server.Config{AdminToken: admin}))
	defer srv.Close()
	call(t, srv, admin, "PUT", "/v1/namespaces/disk", nil)
	k := key(t, srv, "disk", "producer")
	r, body := send(t, srv, k, "POST", "/v1/ns/disk/jobs", "", map[string]any{"queue": "q", "kind": "k"})
	if r.StatusCode != 507 || body["title"] != "storage full" || r.Header.Get("Retry-After") == "" {
		t.Fatalf("enqueue on a low disk: %d %v %q", r.StatusCode, body, r.Header.Get("Retry-After"))
	}
	if _, err := remote.New(srv.URL, k, "disk").Enqueue(context.Background(), "q", "k", nil); !errors.Is(err, runnel.ErrNoSpace) {
		t.Fatalf("remote enqueue on a low disk: %v, want ErrNoSpace", err)
	}
}
