package hybrid

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/kinoforge/runnel"
)

func TestBodyRoundTrip(t *testing.T) {
	at := time.Date(2026, 11, 5, 7, 0, 0, 123456789, time.UTC)
	in := &runnel.Job{
		ID: "0193a000-0000-7000-8000-000000000001", UniqueKey: "u", DependsOn: []string{"a", "b"},
		Payload: json.RawMessage(`{"to":"x"}`), Result: json.RawMessage(`{"ok":true}`), Handle: "h", CallbackURL: "https://example.com/hook",
		StartTimeout: runnel.Duration(time.Second), Timeout: runnel.Duration(time.Minute), UniqueFor: runnel.Duration(time.Hour),
		Backoff: runnel.Backoff{Base: runnel.Duration(time.Second), Max: runnel.Duration(time.Minute), Factor: 1.5, Jitter: 0.1},
		Meta:    map[string]string{"trace": "abc"},
		Errors:  []runnel.AttemptError{{Attempt: 1, Queue: "q", Kind: "k", At: at, Error: "boom"}},
	}
	out := &runnel.Job{ID: in.ID}
	if err := decodeBody(encodeBody(in), out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
	key, deps, err := keyOf(encodeBody(in))
	if err != nil || key != "u" || !reflect.DeepEqual(deps, in.DependsOn) {
		t.Fatalf("keyOf %q %v %v", key, deps, err)
	}
	if err := decodeBody(encodeBody(in)[:10], &runnel.Job{}); err == nil {
		t.Fatal("cut body decoded")
	}
}

func TestParseIDCanonicalOnly(t *testing.T) {
	const id = "0193a000-0000-7000-8000-00000000000a"
	if u, ok := parseID(id); !ok || u.String() != id {
		t.Fatalf("canonical id refused")
	}
	for _, bad := range []string{"0193A000-0000-7000-8000-00000000000A", "nope", ""} {
		if _, ok := parseID(bad); ok {
			t.Fatalf("%q accepted", bad)
		}
	}
}
