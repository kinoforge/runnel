package runnel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

type hookReceiver struct {
	mu         sync.Mutex
	status     int
	retryAfter string
	method     string
	header     http.Header
	body       []byte
}

func (h *hookReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.method, h.header, h.body = r.Method, r.Header.Clone(), b
	if h.retryAfter != "" {
		w.Header().Set("Retry-After", h.retryAfter)
	}
	if h.status == http.StatusFound {
		w.Header().Set("Location", "/elsewhere")
	}
	w.WriteHeader(h.status)
}

func (h *hookReceiver) answer(status int, retryAfter string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status, h.retryAfter = status, retryAfter
}

func TestCallbackDelivery(t *testing.T) {
	spec.Rule(t, "H02", "H03")
	rcv := &hookReceiver{status: http.StatusOK}
	srv := httptest.NewServer(rcv)
	defer srv.Close()
	done := &Job{ID: NewID(), Namespace: "app", Queue: "q", Kind: "k", State: StateCompleted, CallbackURL: srv.URL + "/hook", Result: json.RawMessage(`{"ok":true}`)}
	hook, err := NewCallbackJob(done, "job.completed", 42, CallbackConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if hook.Queue != CallbackQueue || hook.Kind != CallbackKind || hook.MaxAttempts != 10 {
		t.Fatalf("delivery job %+v", hook)
	}
	sender := NewCallbackSender(func(context.Context, string) (string, error) { return "s3cret", nil }, CallbackConfig{AllowPrivate: true})
	deliver := func() Outcome { return OutcomeOf(sender.Work(context.Background(), hook), nil) }
	if o := deliver(); o.Kind != OutcomeDone {
		t.Fatalf("2xx became %+v", o)
	}
	rcv.mu.Lock()
	sig := rcv.header.Get("Runnel-Signature")
	ts, _ := strconv.ParseInt(strings.TrimPrefix(strings.Split(sig, ",")[0], "t="), 10, 64)
	var sent struct {
		Event    string
		Delivery int64
		Job      JobSummary
	}
	err = json.Unmarshal(rcv.body, &sent)
	if err != nil || rcv.method != http.MethodPost || rcv.header.Get("Runnel-Event") != "job.completed" || rcv.header.Get("Runnel-Delivery") != "42" ||
		sig != Sign("s3cret", ts, rcv.body) || sent.Event != "job.completed" || sent.Delivery != 42 || sent.Job.ID != done.ID || string(sent.Job.Result) != `{"ok":true}` {
		t.Errorf("delivery %s %v %s: %v", rcv.method, rcv.header, rcv.body, err)
	}
	rcv.mu.Unlock()
	cases := []struct {
		status     int
		retryAfter string
		want       OutcomeKind
		delay      time.Duration
	}{
		{http.StatusInternalServerError, "", OutcomeRetry, 0},
		{http.StatusTooManyRequests, "7", OutcomeRetry, 7 * time.Second},
		{http.StatusTooManyRequests, "9223372037", OutcomeRetry, 0},
		{http.StatusServiceUnavailable, "3601", OutcomeRetry, 0},
		{http.StatusRequestTimeout, "", OutcomeRetry, 0},
		{http.StatusNotFound, "", OutcomeFail, 0},
		{http.StatusFound, "", OutcomeFail, 0},
	}
	for _, c := range cases {
		rcv.answer(c.status, c.retryAfter)
		if o := deliver(); o.Kind != c.want || o.Delay.D() != c.delay {
			t.Errorf("%d became %+v, want %s after %v", c.status, o, c.want, c.delay)
		}
	}
}

func TestCallbackSignedWithOwnNamespace(t *testing.T) {
	spec.Rule(t, "H07")
	rcv := &hookReceiver{status: http.StatusOK}
	srv := httptest.NewServer(rcv)
	defer srv.Close()
	secrets := map[string]string{"a": "secret-a", "b": "secret-b"}
	var asked []string
	sender := NewCallbackSender(func(_ context.Context, ns string) (string, error) {
		asked = append(asked, ns)
		return secrets[ns], nil
	}, CallbackConfig{AllowPrivate: true})
	own, err := NewCallbackJob(&Job{ID: NewID(), Namespace: "a", Queue: "q", Kind: "k", State: StateCompleted, CallbackURL: srv.URL}, "job.completed", 1, CallbackConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Work(context.Background(), own); err != nil {
		t.Fatal(err)
	}
	rcv.mu.Lock()
	sig, body := rcv.header.Get("Runnel-Signature"), rcv.body
	rcv.header, rcv.body = nil, nil
	rcv.mu.Unlock()
	ts, _ := strconv.ParseInt(strings.TrimPrefix(strings.Split(sig, ",")[0], "t="), 10, 64)
	if sig != Sign("secret-a", ts, body) {
		t.Fatalf("own delivery signature %q", sig)
	}
	forged, err := NewCallbackJob(&Job{ID: NewID(), Namespace: "b", Queue: "q", Kind: "k", State: StateCompleted, CallbackURL: srv.URL}, "job.completed", 2, CallbackConfig{})
	if err != nil {
		t.Fatal(err)
	}
	forged.Namespace = "a"
	o := OutcomeOf(sender.Work(context.Background(), forged), nil)
	rcv.mu.Lock()
	sent := rcv.body != nil
	rcv.mu.Unlock()
	if o.Kind != OutcomeFail || sent || slices.Contains(asked, "b") {
		t.Fatalf("forged delivery %+v sent=%v asked=%v", o, sent, asked)
	}
}
