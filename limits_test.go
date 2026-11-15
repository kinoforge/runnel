package runnel

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestNameBoundaries(t *testing.T) {
	spec.Rule(t, "J02")
	cases := []struct {
		check func(string) bool
		in    string
		ok    bool
	}{
		{ValidName, "a", true},
		{ValidName, "9", true},
		{ValidName, "a.b-c_D", true},
		{ValidName, strings.Repeat("a", 128), true},
		{ValidName, strings.Repeat("a", 129), false},
		{ValidName, "", false},
		{ValidName, "_a", false},
		{ValidName, ".a", false},
		{ValidName, "-a", false},
		{ValidName, "a b", false},
		{ValidName, "a/b", false},
		{ValidName, "a:b", false},
		{ValidName, "é", false},
		{ValidGroup, "customer:42@eu", true},
		{ValidGroup, strings.Repeat("g", 128), true},
		{ValidGroup, strings.Repeat("g", 129), false},
		{ValidGroup, ":a", false},
		{ValidGroup, "a|b", false},
		{ValidNamespace, "shop-1_eu", true},
		{ValidNamespace, strings.Repeat("n", 63), true},
		{ValidNamespace, strings.Repeat("n", 64), false},
		{ValidNamespace, "Shop", false},
		{ValidNamespace, "a.b", false},
		{ValidNamespace, "-a", false},
	}
	for _, c := range cases {
		if got := c.check(c.in); got != c.ok {
			t.Errorf("%.20q: %v, want %v", c.in, got, c.ok)
		}
	}
}

func metaOf(n, keyLen, valueLen int) map[string]string {
	m := map[string]string{}
	for i := range n {
		k := fmt.Sprintf("%0*d", keyLen, i)
		m[k] = strings.Repeat("v", valueLen)
	}
	return m
}

func deps(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = NewID()
	}
	return out
}

func TestJobBoundaries(t *testing.T) {
	spec.Rule(t, "J02")
	l := DefaultLimits
	payload := func(n int) json.RawMessage { return json.RawMessage(`"` + strings.Repeat("x", n-2) + `"`) }
	url := func(n int) string { return "https://example.com/" + strings.Repeat("p", n-len("https://example.com/")) }
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		set  func(*Job)
		ok   bool
	}{
		{"priority 1000", func(j *Job) { j.Priority = 1000 }, true},
		{"priority -1000", func(j *Job) { j.Priority = -1000 }, true},
		{"priority 1001", func(j *Job) { j.Priority = 1001 }, false},
		{"priority -1001", func(j *Job) { j.Priority = -1001 }, false},
		{"attempts 1000", func(j *Job) { j.MaxAttempts = 1000 }, true},
		{"attempts 1001", func(j *Job) { j.MaxAttempts = 1001 }, false},
		{"attempts -1", func(j *Job) { j.MaxAttempts = -1 }, false},
		{"payload at the limit", func(j *Job) { j.Payload = payload(l.MaxPayload) }, true},
		{"payload one byte over", func(j *Job) { j.Payload = payload(l.MaxPayload + 1) }, false},
		{"payload not json", func(j *Job) { j.Payload = json.RawMessage(`{"a":`) }, false},
		{"meta at the limit", func(j *Job) { j.Meta = metaOf(l.MaxMeta, l.MaxMetaKey, l.MaxMetaValue) }, true},
		{"meta one entry over", func(j *Job) { j.Meta = metaOf(l.MaxMeta+1, 4, 1) }, false},
		{"meta key one byte over", func(j *Job) { j.Meta = metaOf(1, l.MaxMetaKey+1, 1) }, false},
		{"meta value one byte over", func(j *Job) { j.Meta = metaOf(1, 4, l.MaxMetaValue+1) }, false},
		{"meta empty key", func(j *Job) { j.Meta = map[string]string{"": "v"} }, false},
		{"unique key at the limit", func(j *Job) { j.UniqueKey = strings.Repeat("u", l.MaxUniqueKey) }, true},
		{"unique key one byte over", func(j *Job) { j.UniqueKey = strings.Repeat("u", l.MaxUniqueKey+1) }, false},
		{"unique key with a nul", func(j *Job) { j.UniqueKey = "a\x00b" }, false},
		{"dependencies at the limit", func(j *Job) { j.DependsOn = deps(l.MaxDependsOn) }, true},
		{"dependencies one over", func(j *Job) { j.DependsOn = deps(l.MaxDependsOn + 1) }, false},
		{"callback at the limit", func(j *Job) { j.CallbackURL = url(l.MaxCallbackURL) }, true},
		{"callback one byte over", func(j *Job) { j.CallbackURL = url(l.MaxCallbackURL + 1) }, false},
		{"pool name", func(j *Job) { j.Pool = "m.flux-1" }, true},
		{"bad pool name", func(j *Job) { j.Pool = "m flux" }, false},
		{"negative start timeout", func(j *Job) { j.StartTimeout = -1 }, false},
		{"negative unique window", func(j *Job) { j.UniqueFor = -1 }, false},
		{"deadline at run_at", func(j *Job) { j.RunAt, j.Deadline = at, at }, false},
		{"deadline before run_at", func(j *Job) { j.RunAt, j.Deadline = at, at.Add(-time.Second) }, false},
		{"timeout filling the deadline", func(j *Job) { j.RunAt, j.Deadline, j.Timeout = at, at.Add(time.Minute), Duration(time.Minute) }, true},
		{"timeout past the deadline", func(j *Job) { j.RunAt, j.Deadline, j.Timeout = at, at.Add(time.Minute), Duration(time.Minute + 1) }, false},
		{"start timeout past the deadline", func(j *Job) { j.RunAt, j.Deadline, j.StartTimeout = at, at.Add(time.Minute), Duration(2 * time.Minute) }, false},
	}
	for _, c := range cases {
		j := &Job{Queue: "q", Kind: "k", MaxAttempts: 1}
		c.set(j)
		err := j.validate(l)
		if c.ok && err != nil {
			t.Errorf("%s refused: %v", c.name, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s accepted: %v", c.name, err)
		}
	}
}

func TestCallbackURLs(t *testing.T) {
	spec.Rule(t, "J02", "S03")
	for raw, ok := range map[string]bool{
		"https://app.example.com/hooks/runnel": true,
		"https://app.example.com:8443/h?x=1":   true,
		"http://app.example.com/h":             true,
		"ftp://app.example.com/h":              false,
		"/hooks/runnel":                        false,
		"https://":                             false,
		"https://user@app.example.com/h":       false,
		"https://user:pw@app.example.com/h":    false,
		"app.example.com/h":                    false,
		"https://app.example.com/%zz":          false,
	} {
		if err := ValidCallbackURL(raw, DefaultLimits.MaxCallbackURL); (err == nil) != ok {
			t.Errorf("%s: %v, want ok=%v", raw, err, ok)
		}
	}
}

func TestDurationJSON(t *testing.T) {
	spec.Rule(t, "J10")
	for in, want := range map[string]time.Duration{
		`"30s"`:    30 * time.Second,
		`"1h2m3s"`: time.Hour + 2*time.Minute + 3*time.Second,
		`"250ms"`:  250 * time.Millisecond,
		`30`:       30 * time.Second,
		`1.5`:      1500 * time.Millisecond,
		`0`:        0,
		`"-1s"`:    -time.Second,
	} {
		var d Duration
		if err := json.Unmarshal([]byte(in), &d); err != nil || d.D() != want {
			t.Errorf("%s: %v %v, want %v", in, d, err, want)
		}
	}
	for _, in := range []string{`"abc"`, `"30"`, `true`, `[]`, `{}`, `1e300`, `-1e300`} {
		var d Duration
		if err := json.Unmarshal([]byte(in), &d); err == nil {
			t.Errorf("%s accepted as %v", in, d)
		}
	}
	for d, want := range map[time.Duration]string{30 * time.Second: `"30s"`, 0: `"0s"`, 90 * time.Minute: `"1h30m0s"`} {
		if b, err := json.Marshal(Duration(d)); err != nil || string(b) != want {
			t.Errorf("%v written as %s %v, want %s", d, b, err, want)
		}
	}
	var q QueueConfig
	if err := json.Unmarshal([]byte(`{"name":"q","timeout":"2m","start_timeout":45}`), &q); err != nil || q.Timeout.D() != 2*time.Minute || q.StartTimeout.D() != 45*time.Second {
		t.Fatalf("queue durations %+v %v", q, err)
	}
}

func TestSettingsBoundaries(t *testing.T) {
	spec.Rule(t, "X03")
	queues := map[string]struct {
		q  QueueConfig
		ok bool
	}{
		"rate 1e6":            {QueueConfig{Name: "q", Rate: 1e6}, true},
		"rate over 1e6":       {QueueConfig{Name: "q", Rate: 1e6 + 1}, false},
		"group rate 1e6":      {QueueConfig{Name: "q", GroupRate: 1e6}, true},
		"group rate over":     {QueueConfig{Name: "q", GroupRate: 1e6 + 1}, false},
		"negative rate":       {QueueConfig{Name: "q", Rate: -0.1}, false},
		"attempts 1000":       {QueueConfig{Name: "q", MaxAttempts: 1000}, true},
		"attempts 1001":       {QueueConfig{Name: "q", MaxAttempts: 1001}, false},
		"attempts unset":      {QueueConfig{Name: "q"}, true},
		"negative max_queued": {QueueConfig{Name: "q", MaxQueued: -1}, false},
		"negative group cap":  {QueueConfig{Name: "q", GroupMaxRunning: -1}, false},
		"negative burst":      {QueueConfig{Name: "q", Burst: -1}, false},
		"negative ttl":        {QueueConfig{Name: "q", TTL: -1}, false},
		"negative retention":  {QueueConfig{Name: "q", DeadRetention: -1}, false},
		"bad name":            {QueueConfig{Name: "a b"}, false},
		"no name":             {QueueConfig{}, false},
		"callback":            {QueueConfig{Name: "q", CallbackURL: "https://example.com/h"}, true},
		"bad callback":        {QueueConfig{Name: "q", CallbackURL: "mailto:a@example.com"}, false},
	}
	for name, c := range queues {
		if err := c.q.Validate(); (err == nil) != c.ok || (err != nil && !errors.Is(err, ErrInvalid)) {
			t.Errorf("queue %s: %v, want ok=%v", name, err, c.ok)
		}
	}
	pools := map[string]struct {
		p  PoolConfig
		ok bool
	}{
		"rate 1e6":       {PoolConfig{Name: "p", Rate: 1e6}, true},
		"rate over 1e6":  {PoolConfig{Name: "p", Rate: 1e6 + 1}, false},
		"negative slots": {PoolConfig{Name: "p", Slots: -1}, false},
		"negative group": {PoolConfig{Name: "p", GroupSlots: -1}, false},
		"negative burst": {PoolConfig{Name: "p", Burst: -1}, false},
		"paused until":   {PoolConfig{Name: "p", PausedUntil: time.Now().Add(time.Hour)}, true},
		"bad name":       {PoolConfig{Name: "p/q"}, false},
	}
	for name, c := range pools {
		if err := c.p.Validate(); (err == nil) != c.ok || (err != nil && !errors.Is(err, ErrInvalid)) {
			t.Errorf("pool %s: %v, want ok=%v", name, err, c.ok)
		}
	}
}

func TestScheduleSpecs(t *testing.T) {
	spec.Rule(t, "T02")
	base := Schedule{Name: "s", Queue: "q", Kind: "k"}
	for _, c := range []struct {
		spec, tz string
		ok       bool
	}{
		{"*/5 * * * *", "", true},
		{"0 9 * * 1-5", "Europe/Berlin", true},
		{"@every 10m", "", true},
		{"@daily", "", true},
		{"@hourly", "UTC", true},
		{"", "", false},
		{"* * *", "", false},
		{"0 */5 * * * *", "", false},
		{"61 * * * *", "", false},
		{"@every", "", false},
		{"@sometimes", "", false},
		{"0 9 * * *", "Mars/Olympus", false},
	} {
		s := base
		s.Spec, s.Timezone = c.spec, c.tz
		if err := s.Validate(); (err == nil) != c.ok {
			t.Errorf("%q in %q: %v, want ok=%v", c.spec, c.tz, err, c.ok)
		}
	}
	for _, bad := range []Schedule{{Spec: "@daily", Queue: "q", Kind: "k"}, {Name: "s", Spec: "@daily", Kind: "k"}, {Name: "s", Spec: "@daily", Queue: "q", Kind: "k", Priority: 2000}} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("schedule %+v accepted: %v", bad, err)
		}
	}
	from := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	sch, err := parseSpec("0 9 * * *", "Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	if next := sch.Next(from).UTC(); !next.Equal(time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("9:00 in Tokyo after %v is %v", from, next)
	}
}
