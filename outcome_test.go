package runnel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func running(attempt, maxAttempts int) *Job {
	now := time.Now().UTC()
	return &Job{ID: NewID(), Queue: "q", Kind: "k", State: StateRunning, Attempt: attempt, MaxAttempts: maxAttempts,
		Worker: "w", LeaseUntil: now.Add(time.Minute), WaitingSince: now.Add(-time.Second), Version: 3}
}

func TestDecide(t *testing.T) {
	now := time.Now().UTC()
	t.Run("done keeps result and clears lease", func(t *testing.T) {
		spec.Rule(t, "O01")
		n := Decide(running(1, 3), Outcome{Kind: OutcomeDone, Result: json.RawMessage(`1`)}, now)
		if n.State != StateCompleted || string(n.Result) != "1" || n.Worker != "" || !n.LeaseUntil.IsZero() || n.FinishedAt != now {
			t.Fatalf("%+v", n)
		}
	})
	t.Run("retry backs off then dies", func(t *testing.T) {
		spec.Rule(t, "O02")
		n := Decide(running(1, 2), Outcome{Kind: OutcomeRetry, Error: "x"}, now)
		if n.State != StateWaiting || !n.RunAt.After(now) || len(n.Errors) != 1 {
			t.Fatalf("%+v", n)
		}
		d := Decide(running(2, 2), Outcome{Kind: OutcomeRetry, Error: "x"}, now)
		if d.State != StateDead {
			t.Fatalf("exhausted retry: %s", d.State)
		}
	})
	t.Run("fallback walks targets in order", func(t *testing.T) {
		spec.Rule(t, "O04", "O05")
		j := running(1, 1)
		j.Fallbacks = []Target{{Queue: "b", Pool: "p2"}, {Kind: "k3"}}
		j.StartTimeout = Duration(time.Minute)
		a := Decide(j, Outcome{Kind: OutcomeFail, Error: "x"}, now)
		if a.Queue != "b" || a.Pool != "p2" || a.Fallback != 1 || a.Attempt != 0 || a.State != StateWaiting || a.StartBy != now.Add(time.Minute) {
			t.Fatalf("first fallback %+v", a)
		}
		a.State, a.Attempt = StateRunning, 1
		b := Decide(a, Outcome{Kind: OutcomeFallback}, now)
		if b.Queue != "b" || b.Kind != "k3" || b.Fallback != 2 || !b.StartBy.IsZero() {
			t.Fatalf("second fallback %+v", b)
		}
		b.State, b.Attempt = StateRunning, 1
		if c := Decide(b, Outcome{Kind: OutcomeFail}, now); c.State != StateDead || len(c.Errors) != 3 {
			t.Fatalf("after last fallback %+v", c)
		}
	})
	t.Run("busy gives the attempt back and keeps waiting_since", func(t *testing.T) {
		spec.Rule(t, "O06")
		j := running(1, 1)
		n := Decide(j, Outcome{Kind: OutcomeBusy}, now)
		if n.State != StateWaiting || n.Attempt != 0 || !n.WaitingSince.Equal(j.WaitingSince) || n.RunAt != now.Add(DefaultBusyDelay) {
			t.Fatalf("%+v", n)
		}
	})
	t.Run("pending polls and keeps attempt", func(t *testing.T) {
		spec.Rule(t, "O07")
		n := Decide(running(1, 1), Outcome{Kind: OutcomePending, Handle: "h"}, now)
		if n.State != StatePolling || n.Handle != "h" || n.Attempt != 1 || n.RunAt != now.Add(DefaultPollDelay) {
			t.Fatalf("%+v", n)
		}
		n.State = StateRunning
		if again := Decide(n, Outcome{Kind: OutcomeBusy, Delay: Duration(time.Second)}, now); again.State != StatePolling || again.Attempt != 1 {
			t.Fatalf("busy while polling %+v", again)
		}
		if bad := Decide(running(1, 1), Outcome{Kind: OutcomePending}, now); bad.State != StateDead {
			t.Fatalf("pending without handle %+v", bad)
		}
	})
	t.Run("release returns the job untouched", func(t *testing.T) {
		spec.Rule(t, "R06")
		n := Decide(running(1, 3), Outcome{Kind: OutcomeRelease}, now)
		if n.State != StateWaiting || n.Attempt != 0 || n.RunAt != now || len(n.Errors) != 0 {
			t.Fatalf("%+v", n)
		}
	})
}

func TestOutcomeOf(t *testing.T) {
	spec.Rule(t, "O08", "O09")
	cases := map[OutcomeKind]error{
		OutcomeDone:     nil,
		OutcomeRetry:    errors.New("x"),
		OutcomeFail:     Fail(errors.New("x")),
		OutcomeFallback: Fallback(errors.New("x")),
		OutcomeBusy:     Busy(time.Second),
		OutcomePending:  Pending("h", time.Second),
	}
	for want, err := range cases {
		if got := OutcomeOf(err, nil).Kind; got != want {
			t.Errorf("%v: got %s want %s", err, got, want)
		}
	}
	wrapped := errors.Join(errors.New("ctx"), Busy(2*time.Second))
	if o := OutcomeOf(wrapped, nil); o.Kind != OutcomeBusy || o.Delay.D() != 2*time.Second {
		t.Fatalf("wrapped busy %+v", o)
	}
	if o := OutcomeOf(RetryIn(time.Minute, errors.New("slow")), nil); o.Kind != OutcomeRetry || o.Delay.D() != time.Minute || o.Error != "slow" {
		t.Fatalf("retry in %+v", o)
	}
}

func TestBackoff(t *testing.T) {
	spec.Rule(t, "O03")
	b := Backoff{Base: Duration(time.Second), Max: Duration(10 * time.Second), Jitter: -1}
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 9: 10 * time.Second, 500: 10 * time.Second} {
		if got := b.Delay(attempt); got != want {
			t.Errorf("attempt %d: %v want %v", attempt, got, want)
		}
	}
	j := Backoff{}.Delay(1)
	if j < 800*time.Millisecond || j > 1200*time.Millisecond {
		t.Fatalf("default jitter out of range: %v", j)
	}
}

func TestValidation(t *testing.T) {
	spec.Rule(t, "J02")
	ok := func() *Job { return &Job{Queue: "q", Kind: "k", MaxAttempts: 1} }
	bad := map[string]func(*Job){
		"queue":    func(j *Job) { j.Queue = "a b" },
		"kind":     func(j *Job) { j.Kind = "" },
		"priority": func(j *Job) { j.Priority = 1001 },
		"payload":  func(j *Job) { j.Payload = json.RawMessage("{") },
		"group":    func(j *Job) { j.Group = "bad|group" },
		"unique":   func(j *Job) { j.UniqueKey = "a|b" },
		"dep":      func(j *Job) { j.DependsOn = []string{"nope"} },
		"callback": func(j *Job) { j.CallbackURL = "ftp://x" },
		"webauth":  func(j *Job) { j.CallbackURL = "https://user:pw@x.com" },
		"target":   func(j *Job) { j.Fallbacks = []Target{{}} },
		"attempts": func(j *Job) { j.MaxAttempts = 0 },
		"big":      func(j *Job) { j.Payload = json.RawMessage(`"` + strings.Repeat("x", 1<<20) + `"`) },
		"negative": func(j *Job) { j.Timeout = -1 },
	}
	if err := ok().validate(DefaultLimits); err != nil {
		t.Fatal(err)
	}
	for name, mut := range bad {
		j := ok()
		mut(j)
		if err := j.validate(DefaultLimits); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s accepted: %v", name, err)
		}
	}
}

func TestKeys(t *testing.T) {
	spec.Rule(t, "A03", "A04")
	token, k, err := NewAPIKey("app", "ci", []string{RoleWorker}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	id, secret, ok := ParseAPIKey(token)
	if !ok || id != k.ID || !k.Matches(secret, time.Now()) || k.Matches(secret+"x", time.Now()) {
		t.Fatalf("key round trip failed")
	}
	if !k.Has(RoleWorker) || k.Has(RoleProducer) {
		t.Fatalf("roles wrong")
	}
	k.ExpiresAt = time.Now().Add(-time.Second)
	if k.Matches(secret, time.Now()) {
		t.Fatalf("expired key matched")
	}
	if _, _, err := NewAPIKey("app", "x", []string{"root"}, time.Time{}); err == nil {
		t.Fatalf("unknown role accepted")
	}
}

func TestSSRFGuard(t *testing.T) {
	spec.Rule(t, "H04")
	c := SafeHTTPClient(time.Second, false)
	for _, u := range []string{"http://127.0.0.1:1/", "http://169.254.169.254/latest", "http://[::1]:1/", "http://10.0.0.1/"} {
		resp, err := c.Get(u)
		if err == nil {
			resp.Body.Close()
		}
		if !errors.Is(err, errBlockedAddress) {
			t.Errorf("%s not blocked: %v", u, err)
		}
	}
}

type skewed struct{ ahead time.Duration }

func (s skewed) Time(context.Context) (time.Time, error) { return time.Now().Add(s.ahead), nil }

func TestStoreClockFollowsSource(t *testing.T) {
	spec.Rule(t, "K01", "J05")
	c := NewStoreClock(skewed{ahead: time.Hour})
	got := time.Until(c.Now())
	if got < time.Hour-time.Second || got > time.Hour+time.Second {
		t.Fatalf("clock offset %v, want about 1h", got)
	}
	if off := c.Offset(); off < time.Hour-time.Second || off > time.Hour+time.Second {
		t.Fatalf("offset %v", off)
	}
	j := &Job{Queue: "q", Kind: "k"}
	In(time.Minute)(j)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := j.Prepare(now, DefaultLimits); err != nil {
		t.Fatal(err)
	}
	if !j.RunAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("In measured from the wrong clock: run_at %v, now %v", j.RunAt, now)
	}
}
