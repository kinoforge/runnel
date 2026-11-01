package runnel

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func FuzzDurationJSON(f *testing.F) {
	spec.Rule(f, "J10")
	for _, s := range []string{`"30s"`, `12.5`, `"-1h"`, `1e300`, `"x"`, `null`, `"9223372036s"`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var d Duration
		if json.Unmarshal(b, &d) != nil {
			return
		}
		out, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("marshal %v: %v", d, err)
		}
		var back Duration
		if err := json.Unmarshal(out, &back); err != nil || back != d {
			t.Fatalf("round trip %s -> %s -> %v (%v)", b, out, back, err)
		}
	})
}

func FuzzParseAPIKey(f *testing.F) {
	spec.Rule(f, "A04")
	f.Add("rnk_0123456789ab_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	f.Add("rnk__")
	f.Add("")
	f.Fuzz(func(t *testing.T, token string) {
		id, secret, ok := ParseAPIKey(token)
		if ok && (len(id) != 12 || len(secret) != 64) {
			t.Fatalf("accepted id %q secret %q", id, secret)
		}
	})
}

func FuzzScheduleSpec(f *testing.F) {
	spec.Rule(f, "T02")
	for _, s := range []string{"* * * * *", "0 */5 * * * *", "@every 10m", "@daily", "61 * * * *", ""} {
		f.Add(s, "")
	}
	f.Add("0 9 * * *", "Europe/Rome")
	f.Fuzz(func(t *testing.T, spec, tz string) {
		sch, err := parseSpec(spec, tz)
		if err == nil {
			sch.Next(time.Now())
		}
	})
}

func FuzzWebhookURL(f *testing.F) {
	spec.Rule(f, "J02")
	for _, s := range []string{"https://example.com/h", "http://u:p@x", "javascript:alert(1)", "https://", "//x"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		_ = ValidWebhookURL(raw, DefaultLimits.MaxWebhookURL)
	})
}

func FuzzDecide(f *testing.F) {
	spec.Rule(f, "O10", "O11")
	f.Add("done", "", int64(0), "", 1, 5, 0, 2)
	f.Add("retry", "boom", int64(time.Second), "", 5, 5, 0, 0)
	f.Add("fallback", "", int64(0), "", 1, 3, 1, 1)
	f.Add("busy", "", int64(-5), "", 2, 2, 0, 0)
	f.Add("pending", "", int64(time.Minute), "h-1", 1, 1, 0, 0)
	f.Fuzz(func(t *testing.T, kind, msg string, delay int64, handle string, attempt, maxAttempts, used, fallbacks int) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		attempt, maxAttempts = 1+abs(attempt)%1000, 1+abs(maxAttempts)%1000
		fallbacks = abs(fallbacks) % 17
		used = abs(used) % (fallbacks + 1)
		j := &Job{ID: NewID(), Queue: "q", Kind: "k", State: StateRunning, Attempt: attempt, MaxAttempts: maxAttempts,
			Worker: "w", LeaseUntil: now.Add(time.Minute), WaitingSince: now, Version: 1, Fallback: used}
		for range fallbacks {
			j.Fallbacks = append(j.Fallbacks, Target{Queue: "other"})
		}
		o := Outcome{Kind: OutcomeKind(kind), Error: msg, Delay: Duration(delay), Handle: handle}
		if o.Validate() != nil {
			return
		}
		n := Decide(j, o, now)
		if !n.State.Valid() {
			t.Fatalf("invalid state %q", n.State)
		}
		if n.State != StateRunning && n.State != StatePolling && (n.Worker != "" || !n.LeaseUntil.IsZero()) {
			t.Fatalf("%s keeps a lease: %+v", n.State, n)
		}
		if n.State.Final() && n.FinishedAt.IsZero() {
			t.Fatalf("final without finished_at")
		}
		if n.Fallback > len(n.Fallbacks) {
			t.Fatalf("fallback index %d past %d targets", n.Fallback, len(n.Fallbacks))
		}
		if len(n.Errors) > maxErrors {
			t.Fatalf("%d errors kept", len(n.Errors))
		}
	})
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
