package runnel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestVerifySignature(t *testing.T) {
	spec.Rule(t, "H05")
	body := []byte(`{"event":"job.completed"}`)
	now := time.Unix(1_800_000_000, 0)
	sig := Sign("new", now.Unix(), body)
	cases := []struct {
		name    string
		header  string
		body    []byte
		at      time.Time
		secrets []string
		ok      bool
	}{
		{"matches", sig, body, now, []string{"new"}, true},
		{"rotated", sig, body, now, []string{"old", "new"}, true},
		{"wrong secret", sig, body, now, []string{"old"}, false},
		{"changed body", sig, []byte(`{}`), now, []string{"new"}, false},
		{"too old", sig, body, now.Add(6 * time.Minute), []string{"new"}, false},
		{"from the future", sig, body, now.Add(-6 * time.Minute), []string{"new"}, false},
		{"no v1", "t=1800000000", body, now, []string{"new"}, false},
		{"bad time", "t=x,v1=00", body, now, []string{"new"}, false},
		{"empty", "", body, now, []string{"new"}, false},
		{"no secret", sig, body, now, nil, false},
	}
	for _, c := range cases {
		err := VerifySignature(c.header, c.body, 0, c.at, c.secrets...)
		if (err == nil) != c.ok || (err != nil && !errors.Is(err, ErrSignature)) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

func TestReadCallbackAndTraceHeaders(t *testing.T) {
	spec.Rule(t, "H05", "H06")
	got := make(chan CallbackBody, 1)
	var trace string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace = r.Header.Get("traceparent")
		c, err := ReadCallback(r, time.Minute, "s3cret")
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		got <- c
	}))
	defer srv.Close()
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	done := &Job{ID: NewID(), Namespace: "app", Queue: "q", Kind: "k", State: StateCompleted, CallbackURL: srv.URL,
		Meta: map[string]string{"traceparent": tp}}
	hook, err := NewCallbackJob(done, "job.completed", 7, "")
	if err != nil {
		t.Fatal(err)
	}
	sender := NewCallbackSender(func(context.Context, string) (string, error) { return "s3cret", nil }, CallbackConfig{AllowPrivate: true})
	if err := sender.Work(context.Background(), hook); err != nil {
		t.Fatal(err)
	}
	c := <-got
	if c.Event != "job.completed" || c.Delivery != 7 || c.Job.ID != done.ID || trace != tp {
		t.Fatalf("callback %+v trace %q", c, trace)
	}
	wrong := NewCallbackSender(func(context.Context, string) (string, error) { return "other", nil }, CallbackConfig{AllowPrivate: true})
	if err := wrong.Work(context.Background(), hook); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("wrong secret answered %v", err)
	}
}
