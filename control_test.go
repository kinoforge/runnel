package runnel

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestBackoffEdges(t *testing.T) {
	spec.Rule(t, "O03")
	exact := Backoff{Jitter: -1}
	for attempt, want := range map[int]time.Duration{-3: time.Second, 0: time.Second, 1: time.Second, 2: 2 * time.Second, 10: 512 * time.Second, 11: 10 * time.Minute, 1_000_000: 10 * time.Minute} {
		if got := exact.Delay(attempt); got != want {
			t.Errorf("defaults, attempt %d: %v, want %v", attempt, got, want)
		}
	}
	triple := Backoff{Base: Duration(100 * time.Millisecond), Factor: 3, Max: Duration(time.Hour), Jitter: -1}
	for attempt, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 300 * time.Millisecond, 3: 900 * time.Millisecond} {
		if got := triple.Delay(attempt); got != want {
			t.Errorf("factor 3, attempt %d: %v, want %v", attempt, got, want)
		}
	}
	if got := (Backoff{Base: Duration(time.Second), Factor: 0.5, Jitter: -1}).Delay(3); got != 4*time.Second {
		t.Errorf("a factor under 1 should mean 2: %v", got)
	}
	if got := (Backoff{Base: Duration(time.Minute), Max: Duration(time.Second), Jitter: -1}).Delay(1); got != time.Second {
		t.Errorf("max under base: %v", got)
	}
	spread := Backoff{Base: Duration(time.Second), Jitter: 0.5}
	low, high := false, false
	for range 2000 {
		d := spread.Delay(1)
		if d < 500*time.Millisecond || d > 1500*time.Millisecond {
			t.Fatalf("jitter 0.5 gave %v", d)
		}
		low = low || d < 900*time.Millisecond
		high = high || d > 1100*time.Millisecond
	}
	if !low || !high {
		t.Fatalf("jitter did not spread both ways: low=%v high=%v", low, high)
	}
	capped := Backoff{Base: Duration(time.Second), Max: Duration(2 * time.Second)}
	for range 500 {
		if d := capped.Delay(50); d < 1600*time.Millisecond || d > 2400*time.Millisecond {
			t.Fatalf("default jitter around the cap gave %v", d)
		}
	}
}

func TestControlErrorsWrapped(t *testing.T) {
	spec.Rule(t, "O09", "O06", "O07", "O08", "O04")
	other := errors.New("other")
	for _, c := range []struct {
		err  error
		want Outcome
	}{
		{Busy(3 * time.Second), Outcome{Kind: OutcomeBusy, Delay: Duration(3 * time.Second)}},
		{Pending("h-1", time.Minute), Outcome{Kind: OutcomePending, Handle: "h-1", Delay: Duration(time.Minute)}},
		{RetryIn(7*time.Second, errors.New("slow down")), Outcome{Kind: OutcomeRetry, Delay: Duration(7 * time.Second), Error: "slow down"}},
		{Fail(errors.New("bad input")), Outcome{Kind: OutcomeFail, Error: "bad input"}},
		{errors.New("plain"), Outcome{Kind: OutcomeRetry, Error: "plain"}},
	} {
		for name, err := range map[string]error{
			"bare":     c.err,
			"wrapped":  fmt.Errorf("charge: %w", c.err),
			"twice":    fmt.Errorf("job: %w", fmt.Errorf("charge: %w", c.err)),
			"joined":   errors.Join(other, c.err),
			"multiple": fmt.Errorf("%w and %w", other, c.err),
		} {
			got := OutcomeOf(err, nil)
			if c.want.Kind == OutcomeRetry && c.want.Delay == 0 {
				if got.Kind != OutcomeRetry || got.Delay != 0 || !strings.Contains(got.Error, "plain") {
					t.Errorf("%s %v: %+v", name, c.err, got)
				}
				continue
			}
			if got.Kind != c.want.Kind || got.Delay != c.want.Delay || got.Handle != c.want.Handle || got.Error != c.want.Error {
				t.Errorf("%s %v: %+v, want %+v", name, c.err, got, c.want)
			}
		}
	}
	running := &Job{ID: NewID(), Queue: "q", Kind: "k", State: StateRunning, Attempt: 1, MaxAttempts: 5, Worker: "w"}
	if n := Decide(running, OutcomeOf(fmt.Errorf("poll: %w", Pending("", 0)), nil), time.Now()); n.State != StateDead || len(n.Errors) != 1 {
		t.Fatalf("pending without a handle ended %s %+v", n.State, n.Errors)
	}
}

func TestErrorHistory(t *testing.T) {
	spec.Rule(t, "O10", "O11")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	j := &Job{ID: NewID(), Queue: "q", Kind: "k", State: StateWaiting, MaxAttempts: 100, Backoff: Backoff{Jitter: -1}}
	for i := 1; i <= 25; i++ {
		j.Attempt++
		j.State, j.Worker, j.LeaseUntil = StateRunning, "w", now.Add(time.Minute)
		j = Decide(j, Outcome{Kind: OutcomeRetry, Error: "fail " + strconv.Itoa(i)}, now)
		if j.Worker != "" || !j.LeaseUntil.IsZero() || j.State != StateWaiting {
			t.Fatalf("after attempt %d: state %s worker %q lease %v", i, j.State, j.Worker, j.LeaseUntil)
		}
		now = now.Add(time.Second)
	}
	if len(j.Errors) != 20 || j.Errors[0].Attempt != 6 || j.Errors[19].Attempt != 25 || j.Errors[19].Error != "fail 25" {
		t.Fatalf("history has %d entries from attempt %d to %d", len(j.Errors), j.Errors[0].Attempt, j.Errors[len(j.Errors)-1].Attempt)
	}
	for i, e := range j.Errors {
		if e.Queue != "q" || e.Kind != "k" || e.At.IsZero() || (i > 0 && !e.At.After(j.Errors[i-1].At)) {
			t.Fatalf("entry %d: %+v", i, e)
		}
	}
	j.Attempt++
	j.State = StateRunning
	j = Decide(j, Outcome{Kind: OutcomeRetry, Error: strings.Repeat("x", maxErrorLen+100)}, now)
	if last := j.Errors[len(j.Errors)-1].Error; len(last) != maxErrorLen {
		t.Fatalf("long error kept %d bytes, want %d", len(last), maxErrorLen)
	}
}

func TestSignatureEdges(t *testing.T) {
	spec.Rule(t, "H05")
	body := []byte(`{"event":"job.completed"}`)
	now := time.Unix(1_800_000_000, 0)
	sig := func(secret string, at time.Time) string { return Sign(secret, at.Unix(), body) }
	v1 := func(h string) string { _, v, _ := strings.Cut(h, ",v1="); return v }
	cases := []struct {
		name    string
		header  string
		maxAge  time.Duration
		secrets []string
		ok      bool
	}{
		{"current secret", sig("new", now), 0, []string{"new", "old"}, true},
		{"old secret while rotating", sig("old", now), 0, []string{"new", "old"}, true},
		{"old secret after rotating", sig("old", now), 0, []string{"new"}, false},
		{"four minutes old", sig("new", now.Add(-4*time.Minute)), 0, []string{"new"}, true},
		{"six minutes old", sig("new", now.Add(-6*time.Minute)), 0, []string{"new"}, false},
		{"six minutes ahead", sig("new", now.Add(6*time.Minute)), 0, []string{"new"}, false},
		{"inside a longer age", sig("new", now.Add(-20*time.Minute)), time.Hour, []string{"new"}, true},
		{"outside a shorter age", sig("new", now.Add(-20*time.Second)), 10 * time.Second, []string{"new"}, false},
		{"one of two v1 values", fmt.Sprintf("t=%d,v1=deadbeef,v1=%s", now.Unix(), v1(sig("new", now))), 0, []string{"new"}, true},
		{"spaces after commas", fmt.Sprintf("t=%d, v1=%s", now.Unix(), v1(sig("new", now))), 0, []string{"new"}, true},
		{"missing", "", 0, []string{"new"}, false},
		{"no time", "v1=" + v1(sig("new", now)), 0, []string{"new"}, false},
		{"bad time", "t=soon,v1=" + v1(sig("new", now)), 0, []string{"new"}, false},
		{"no v1", fmt.Sprintf("t=%d", now.Unix()), 0, []string{"new"}, false},
		{"time changed", fmt.Sprintf("t=%d,v1=%s", now.Unix()+1, v1(sig("new", now))), 0, []string{"new"}, false},
		{"empty secret", sig("", now), 0, []string{""}, false},
		{"no secrets", sig("new", now), 0, nil, false},
	}
	for _, c := range cases {
		err := VerifySignature(c.header, body, c.maxAge, now, c.secrets...)
		if c.ok && err != nil {
			t.Errorf("%s refused: %v", c.name, err)
		}
		if !c.ok && !errors.Is(err, ErrSignature) {
			t.Errorf("%s accepted: %v", c.name, err)
		}
	}
	if err := VerifySignature(sig("new", now), []byte(`{"event":"job.dead"}`), 0, now, "new"); !errors.Is(err, ErrSignature) {
		t.Fatalf("changed body accepted: %v", err)
	}
}

func TestAPIKeyFormat(t *testing.T) {
	spec.Rule(t, "A04")
	token, k, err := NewAPIKey("app", "ci", []string{RoleReader}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	id, secret, ok := ParseAPIKey(token)
	if !ok || id != k.ID || len(id) != 12 || len(secret) != 64 || strings.Contains(k.Hash, secret) || k.Hash != HashSecret(secret) {
		t.Fatalf("token %q parsed to %q %q %v, hash %q", token, id, secret, ok, k.Hash)
	}
	for _, c := range []string{id, secret} {
		if strings.Trim(c, "0123456789abcdef") != "" {
			t.Errorf("%q is not lowercase hex", c)
		}
	}
	other, _, _ := NewAPIKey("app", "ci", []string{RoleReader}, time.Time{})
	if other == token {
		t.Fatal("two keys made alike")
	}
	for _, bad := range []string{
		"",
		strings.TrimPrefix(token, "rnk_"),
		"rnx_" + strings.TrimPrefix(token, "rnk_"),
		"rnk_" + id + secret,
		"rnk_" + id[1:] + "_" + secret,
		"rnk_" + id + "_" + secret[1:],
		"rnk_" + id + "_" + secret + "0",
		"rnk_" + id + "_" + secret + "_x",
		token + " ",
	} {
		if _, _, ok := ParseAPIKey(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
	if k.Matches(strings.ToUpper(secret), time.Now()) && strings.ToUpper(secret) != secret {
		t.Error("an upper-case secret matched")
	}
}
